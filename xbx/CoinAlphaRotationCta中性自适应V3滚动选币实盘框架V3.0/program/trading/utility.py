# -*- coding: utf-8 -*-
import asyncio
import json
import urllib
import io
from flask import Response
import time
import os
from notify.dingding import *
from notify.telegram import *
from notify.wechat import *

from config.config import settings

if settings.DINGDING_ROBOT_ID:
    notify_sender = DingTalkRobot(robot_id = settings.DINGDING_ROBOT_ID,secret = settings.DINGDING_SECRET)
elif settings.TELEGRAM_TOKEN:
    notify_sender = TgRobot(token = settings.TELEGRAM_TOKEN, chat_id = settings.TELEGRAM_CHAT_ID)
elif settings.WECHAT_CORPID:
    notify_sender = WechatRobot(settings.WECHAT_CORPID, settings.WECHAT_SECRET, settings.WECHAT_AGENT_ID)
else:
    raise ValueError("框架没有检测到告警机器人配置,请检查!")


# 装饰函数，用于异步运行
def asyncrun(func):  
    def wrapfunc(*arg, **kwargs):
        return asyncio.ensure_future(func(*arg, **kwargs))

    return wrapfunc


def get_min_interval(df):  # 从配置df获得最小的运行时间间隔
    rule = ['m', 'h', 'd']
    for rule_type in rule:
        _df = df[df.time_interval.str.contains(rule_type)]
        if _df.shape[0] > 0:
            return str(_df['time_interval'].apply(lambda x: int(x.replace(rule_type, ''))).min()) + rule_type




def json_response(status=200, obj={}, sort=True):
    # wrap a response
    r = {"_system": "geekree api system", "result": obj}
    return Response(
        response=json.dumps(r, ensure_ascii=False, sort_keys=sort, indent=2),
        status=200,
        mimetype="application/json")


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


def url_decode(str):
    return urllib.parse.unquote(str)


def tail(file_name, line_count=10, encoding='utf-8'):
    # print('tail',file_name)
    """
    读取某文本文件最末的行
    :param file_name: 文件名
    :param line_count: 读多少行
    :param encoding: 文件编码
    :return: 数组格式的行列表
    """
    f = open(file_name, mode='rb')
    f.seek(0, io.SEEK_END)
    file_size = f.tell()
    if file_size == 0 or line_count <= 0:
        return []
    lines = []
    prev_char = None
    curr_line = bytearray()
    chars_read = 0
    f.seek(-1, io.SEEK_END)
    while True:
        curr_char = f.read(1)
        chars_read += 1
        # 以下三个步骤：增加字符、增加行、跳出循环，如果文件已经读完，则都要做
        if curr_char not in (b'\n', b'\r') or chars_read == file_size:
            curr_line.extend(curr_char)
        if curr_char == b'\n' or (curr_char == b'\r' and not prev_char == b'\n'
                                  ) or chars_read == file_size:
            curr_line.reverse()
            lines.append(bytes(curr_line).decode(encoding))
            curr_line.clear()
        if len(lines) == line_count or chars_read == file_size:
            break
        # 前退一个字节，此处可以测试一下性能
        f.seek(-2, io.SEEK_CUR)
        prev_char = curr_char
    lines.reverse()
    return lines


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
            notify_sender.send_msg(settings.TRADE_MARKET + ':' + str(reason))
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内



def robust(actual_do,*args, **keyargs):
    tryTimes    = settings.DEFAULT_SLEEP_TIMES
    sleepTimes  = settings.DEFAULT_TRY_TIMES
    result = run_function_till_success(notify_sender,function=lambda: actual_do(*args, **keyargs), tryTimes=tryTimes, sleepTimes=sleepTimes)
    if result:
        return result[0]
    else:
        notify_sender.send_msg(settings.TRADE_MARKET + ':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数')
        os._exit(0)        
        # exit()



if __name__ == "__main__":
    pass

