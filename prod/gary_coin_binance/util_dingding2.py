# -*- coding:utf-8 -*-
'''
Sam@东莞宽客工作室
wechat：hesamfeng

非阻塞的钉钉发动程序
'''

import urllib, requests
import json
import time
from threading import Lock, Thread

import sys
import traceback
from datetime import datetime

#把钉钉的设置id放在这里
#需要用电脑钉钉进行设置。
UIDS = ""

class dingRobot_thread(Thread):

    def __init__(self,uids: list, content: str,backtesting: bool):
        super(dingRobot_thread, self).__init__(name="dingRobot_thread")
        self.url =uids
        self.content = content
        self.backtesting =backtesting

    def run(self):
        if self.backtesting == True:
            return
        program = {
            "msgtype": "text",
            "text": {"content": self.content},
        }
        headers = {'Content-Type': 'application/json'}
        try:
            requests.adapters.DEFAULT_RETRIES = 2
            f = requests.post(self.url, data=json.dumps(program), headers=headers,timeout=5)
        except Exception as ee:
            print("{} 微信发送异常 ex:{},trace:{}".format(datetime.now(), str(ee), traceback.format_exc()),
                  file=sys.stderr)
            print(ee)

        print("dingRobot_thread sent successful!")


def print_dict(d: dict):
    """返回dict的字符串类型"""
    return '\n'.join([f'{key}:{d[key]}' for key in sorted(d.keys())])


def dingding(*args, **kwargs):
    """
    发送钉钉Msg
    :param content:   发送内容
    :return:
    """
    # 参数1
    uids = kwargs.get('uids', None)
    # 没有配置的话，使用缺省UID
    if uids == None:
        uids=UIDS


    # 参数2
    content = kwargs.get('content', None)
    if content is None:
        if len(args) == 0:
            return
        content = args[0]
    if len(content) == 0:
        return

    # dict => str, none str => str
    if not isinstance(content, str):
        if isinstance(content, dict):
            content = '{}'.format(print_dict(content))
        else:
            content = str(content)

    #参数3
    backtesting = kwargs.get('backtesting', None)
    if backtesting is None:
        if len(args) ==0:
            return
        elif len(args) ==1:
            backtesting=False
        else:
            backtesting = args[1]



    t = dingRobot_thread(uids=uids,content=content,backtesting=backtesting)
    t.daemon = False
    # t.run()
    t.start()


if __name__ == '__main__':
    # msgcontent = u'DGquant你好，我是东莞宽客工作室的套利机器人，\n现在的时间是{}\n'.format(time.asctime( time.localtime(time.time()) ))
    msgcontent = u'aliyun.u20: 大家好。 '
    dingding(msgcontent)

