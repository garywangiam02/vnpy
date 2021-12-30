import os
import ccxt
import json


TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", '1872009999:AAGF8YtfYZomovrtPVkFmFAfTPczyM1D-Bc') # tg token
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", -1001265758984) # 用于实盘操作告警
ENABLED_USERS = os.environ.get("ENABLED_USERS", 1874744992) # 合法用户id列表
BINANCE_API_CONFIGS = os.environ.get("BINANCE_API_CONFIGS", '{"qinghaihu999999@163.com": {"apiKey": "ufEnlqdwRn240HCAuOwFGLNyV1QcbmruwiisszYTU3VncPlPElCS3PRffffffff","secret": "KWq8yf5ljrNEXky8T5qm8xwJHDjz5mul8zEe7sJTVRqy7ETUPAxti8ffffffffff"}}') # 账户列表
CTRL_MODE = os.environ.get("CTRL_MODE", 'PM2')
MAX_TASK_OUTPUT = os.environ.get("MAX_TASK_OUTPUT", 3000) 
PROXY_URL = os.environ.get("PROXY_URL", '')
TRY_TIMES = os.environ.get("TRY_TIMES", 10)
SLEEP_TIMES = os.environ.get("SLEEP_TIMES", 20)


enabled_users = set(int(e.strip()) for e in str(ENABLED_USERS).split(','))

CMD_WHITE_LIST = {}
CMD_BLACK_LIST = {'rm'}
CMD_BLACK_CHARS = {';', '\n'}
ONLY_SHORTCUT_CMD = False


# display command, or script
SC_MENU_ITEM_ROWS = (
    (
        ('pwd', 'pwd'), 
        ('ls', 'ls'),
    ),
    (
        ('策略列表', 'pm2 list -m'),
        ('日志', 'pm2 logs --nostream --lines 10'),
    ),
    (
        ('一键清仓!!!', 'python util.py'),  
    ),
)

SC_MENU_ITEM_CMDS = {}
for row in SC_MENU_ITEM_ROWS:
    for cmd in row:
        SC_MENU_ITEM_CMDS[cmd[1]] = cmd

REQUEST_KWARGS = {
    'proxy_url': PROXY_URL
}

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
SCRIPTS_ROOT_PATH = os.path.join(ROOT_PATH, '')
LOGPATH = os.path.join(ROOT_PATH, '')


 # ===创建交易所
exchange_config = {}

for account_name,account_api in json.loads(BINANCE_API_CONFIGS).items():
     binance_coinfig = {
         'apiKey': account_api['apiKey'],
         'secret': account_api['secret'],
         'timeout': 30000,
         'rateLimit': 10,
         'hostname': 'binancezh.com', 
         'enableRateLimit': False,
         'options': {
             'adjustForTimeDifference': True,  # ←---- resolves the timestamp
             'recvWindow': 10000,
            },
        }
     exchange_config[account_name] = ccxt.binance(binance_coinfig) 
 