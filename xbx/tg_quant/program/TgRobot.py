import logging
import os
from threading import active_count
import time
from functools import wraps

import delegator
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (CallbackQueryHandler, CommandHandler, Filters,
                          MessageHandler, Updater)

from config.Config import *
from manager.Utility import *

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)

logger = logging.getLogger(__name__)

__tasks = set()


def restricted(func):
    @wraps(func)
    def wrapped(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in enabled_users:
            print(f"Unauthorized access denied for {user_id}.")
            return
        return func(update, context, *args, **kwargs)

    return wrapped


@restricted
def start(update, context):
    def to_buttons(cmd_row):
        return [InlineKeyboardButton(e[0], callback_data=e[1]) for e in cmd_row]

    keyboard = [
        to_buttons(row) for row in SC_MENU_ITEM_ROWS
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    msg = ("支持的命令:\r\n"
           "/tasks 查看任务\r\n"
           # "/sudo_login to call sudo\r\n"
           "/kill kill任务\r\n"
           "/list_strategy  查询策略列表 \r\n"
           "/list_account   查询账户列表 \r\n"
           "/start_strategy  id 启动策略 参数id/name\r\n"
           "/stop_strategy id  停止策略 参数id/name\r\n"
           "/restart_strategy id 重启策略 参数id/name\r\n"
           "/clearpos 清仓 参数:账户id|清仓比例\r\n"
           "快捷键:")
    update.message.reply_text(msg, reply_markup=reply_markup)


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


def __is_out_all(cmd):
    param = 'oa;'
    if cmd.startswith(param):
        return cmd[len(param):], True
    return cmd, False


# @run_async
def __do_exec(cmd, update, context, is_script=False, need_filter_cmd=True):
    message = update.message or update.callback_query.message
    reply_text = message.reply_text  # to hold func reply_text
    logger.debug('exec command "%s", is_script "%s"', cmd, is_script)
    max_idx = 5
    cmd, is_out_all = __is_out_all(cmd)
    if is_out_all:
        max_idx = 999999

    if need_filter_cmd and not __check_cmd_chars(cmd):
        reply_text(f'This cmd is illegal.')
        return

    if is_script:
        cmd = 'ao;pm2'

    try:
        c = delegator.run(cmd, block=False, timeout=1e6)
    except FileNotFoundError as e:
        reply_text(f"{e}")
        return
    out = ''
    task = (f'{c.pid}', cmd, c)
    __tasks.add(task)
    start_time = time.time()
    idx = 0

    for line in c.subprocess:
        print(line)
        out += line
        cost_time = time.time() - start_time
        if cost_time > 1:
            reply_text(out[:MAX_TASK_OUTPUT])
            idx += 1
            out = ''
            start_time = time.time()
        if idx > max_idx:
            reply_text(f'Command not finished. You can kill it by sending /kill {c.pid}')
            break
    c.block()

    __tasks.remove(task)
    if out:
        reply_text(out[:MAX_TASK_OUTPUT])
    if idx > 3:
        reply_text(f'Task finished: {cmd}')


def __do_cd(update, context):
    cmd: str = update.message.text
    if not cmd.startswith('cd '):
        return False
    try:
        os.chdir(cmd[3:])
        update.message.reply_text(f'pwd: {os.getcwd()}')
    except FileNotFoundError as e:
        update.message.reply_text(f'{e}')
    return True


def __check_cmd(cmd: str):
    cmd = cmd.lower()
    if cmd.startswith('sudo'):
        cmd = cmd[4:].strip()
    cmd = cmd.split(' ')[0]
    if CMD_WHITE_LIST and cmd not in CMD_WHITE_LIST:
        return False
    if cmd in CMD_BLACK_LIST:
        return False
    return True


def __check_cmd_chars(cmd: str):
    for char in CMD_BLACK_CHARS:
        if char in cmd:
            return False
    return True


@restricted
def do_exec(update, context):
    if not update.message:
        return
    if __do_cd(update, context):
        return
    cmd: str = update.message.text
    if not __check_cmd(cmd):
        return
    __do_exec(cmd, update, context)


@restricted
def do_tasks(update, context):
    msg = '\r\n'.join([', '.join(e[:2]) for e in __tasks])
    if not msg:
        msg = "Task list is empty"
    update.message.reply_text(msg)


@restricted
def do_script(update, context):
    args = context.args.copy()
    if args:
        cmd = ' '.join(args)
        __do_exec(cmd, update, context, is_script=True)
        return
    scripts = '\r\n'.join(
        os.path.join(r[len(SCRIPTS_ROOT_PATH):], file)
        for r, d, f in os.walk(SCRIPTS_ROOT_PATH) for file in f
    )
    msg = "Usage: /script script_name args\r\n"
    msg += scripts
    update.message.reply_text(msg)


@restricted
def do_kill(update, context):
    if not context.args:
        update.message.reply_text('Usage: /kill pid')
        return

    pid = context.args[0]
    for task in __tasks:
        if task[0] == pid:
            task[2].kill()
            update.message.reply_text(f'killed: {task[1]}')
            return
    update.message.reply_text(f'pid "{pid}" not find')

@restricted
def do_list_strategy(update, context):
    """
    查看策略列表
    """
    if CTRL_MODE == 'PM2':
        cmd = 'pm2 list'
        __do_exec(cmd, update, context, is_script=False)
        return
    else :
        cmd = 'docker ps -a'
        __do_exec(cmd, update, context, is_script=False)
        return

@restricted
def do_list_account(update, context):
    """
    查询账户列表
    """
    account_list = [value for value in json.loads(BINANCE_API_CONFIGS).keys()]
    update.message.reply_text(str(account_list))
                

@restricted
def do_start_strategy(update, context):
    """
    启动策略
    """
    if context.args:
        id_or_name = context.args[0]
        cmd = 'pm2 start ' + id_or_name
        __do_exec(cmd, update, context, is_script=False)
        return
    else:
        update.message.reply_text('Usage: /start_strategy  id_or_name')


@restricted
def do_stop_strategy(update, context):
    """
    停止策略
    """
    if context.args:
        id_or_name = context.args[0]
        cmd = 'pm2 stop ' + id_or_name
        __do_exec(cmd, update, context, is_script=False)
        return
    else:
        update.message.reply_text('Usage: /stop_strategy id_or_name')


@restricted
def do_restart_strategy(update, context):
    """
    重启策略
    """
    if context.args:
        id_or_name = context.args[0]
        cmd = 'pm2 restart ' + id_or_name
        __do_exec(cmd, update, context, is_script=False)
        return
    else:
        update.message.reply_text('Usage: /restart_strategy id_or_name')


@restricted
def do_clearPos(update, context):
    '''
    清仓
    /clearpos   所有账户全部清仓
    /clearpos account   指定账户清仓
    /clearpos account percent  指定账户清仓特定比例
    '''
    if len(context.args) == 2:
        account_name = context.args[0]
        percent = int(context.args[1])
        exchange = exchange_config[account_name] 
        if exchange:
           clear_pos(exchange= exchange,percent=percent)
           update.message.reply_text('对指定账户清仓完毕!')
        else:
           update.message.reply_text('请输入正确的账户信息!')
    elif len(context.args) == 1: 
        account_name = context.args[0]
        exchange = exchange_config[account_name] 
        if exchange:
           clear_pos(exchange= exchange)
           update.message.reply_text('对指定账户清仓完毕!')
        else:
           update.message.reply_text('请输入正确的账户信息!')
    else:
        for exchange in exchange_config.values():
           clear_pos(exchange= exchange)
        update.message.reply_text('所有账户清仓完毕!')


@restricted
def do_sudo_login(update, context):
    if not context.args:
        update.message.reply_text('Usage: /sudo_login password')
        return

    password = context.args[0]
    c = delegator.chain(f'echo "{password}" | sudo -S xxxvvv')
    out = c.out
    if 'xxxvvv: command not found' in out:
        update.message.reply_text(f'sudo succeeded.')
    update.message.reply_text(f'sudo failed.')


@restricted
def shortcut_cb(update, context):
    query = update.callback_query
    cmd = query.data
    if cmd not in SC_MENU_ITEM_CMDS.keys():
        update.callback_query.message.reply_text(f'This cmd is illegal.')
    cmd_info = SC_MENU_ITEM_CMDS[cmd]
    is_script = cmd_info[2] if len(cmd_info) >= 3 else False
    if not is_script:
        __do_exec(cmd, update, context, is_script=is_script, need_filter_cmd=False)
    else:
        do_script(update, context)


def start_tg_robot():
    updater = Updater(TELEGRAM_TOKEN, use_context=True, request_kwargs=REQUEST_KWARGS)

    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", start))
    dp.add_handler(CallbackQueryHandler(shortcut_cb))

    dp.add_handler(CommandHandler("list_strategy", do_list_strategy, pass_args=True))
    dp.add_handler(CommandHandler("list_account", do_list_account, pass_args=True))
    dp.add_handler(CommandHandler("start_strategy", do_start_strategy, pass_args=True))
    dp.add_handler(CommandHandler("stop_strategy", do_stop_strategy, pass_args=True))
    dp.add_handler(CommandHandler("restart_strategy", do_restart_strategy, pass_args=True))
    dp.add_handler(CommandHandler("clearpos", do_clearPos, pass_args=True))

    if not ONLY_SHORTCUT_CMD:
        dp.add_handler(CommandHandler("script", do_script, pass_args=True))
        dp.add_handler(MessageHandler(Filters.text, do_exec))

    dp.add_error_handler(error)
    updater.start_polling(timeout =600)
    logger.info('Telegram shell bot started.')
    updater.idle()

if __name__ == '__main__':
    start_tg_robot()
