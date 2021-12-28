# -*- coding: utf-8 -*-
import time
from notify.dingding import *
from notify.telegram import *
from notify.wechat import *
from config.config import *

if DINGDING_ROBOT_ID:
    notify_sender = DingTalkRobot(robot_id = DINGDING_ROBOT_ID,secret = DINGDING_SECRET)
elif TELEGRAM_TOKEN:
    notify_sender = TgRobot(token = TELEGRAM_TOKEN, chat_id = TELEGRAM_CHAT_ID)
elif WECHAT_CORPID:
    notify_sender = WechatRobot(WECHAT_CORPID, WECHAT_SECRET, WECHAT_AGENT_ID)
else:
    raise ValueError("框架没有检测到告警机器人配置,请检查!")



def get_min_interval(df):  # 从配置df获得最小的运行时间间隔
    rule = ['m', 'h', 'd']
    for rule_type in rule:
        _df = df[df.time_interval.str.contains(rule_type)]
        if _df.shape[0] > 0:
            return str(_df['time_interval'].apply(lambda x: int(x.replace(rule_type, ''))).min()) + rule_type


def run_function_till_success(notify_sender,function, tryTimes=5, sleepTimes=60):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    while True:
        if retry > tryTimes:
            return False
        try:
            result = function()
            return [result, retry]
        except (Exception) as reason:
            print(reason)
            notify_sender.send_msg(TRADE_MARKET + ':' + str(reason))
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内



def robust(actual_do,*args, **keyargs):
    tryTimes    = DEFAULT_SLEEP_TIMES
    sleepTimes  = DEFAULT_TRY_TIMES
    result = run_function_till_success(notify_sender,function=lambda: actual_do(*args, **keyargs), tryTimes=tryTimes, sleepTimes=sleepTimes)
    if result:
        return result[0]
    else:
        notify_sender.send_msg(TRADE_MARKET + ':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数')
