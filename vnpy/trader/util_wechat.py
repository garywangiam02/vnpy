﻿# -*- coding:utf-8 -*-
'''
通过wxpusher发送Weixin的消息
http://wxpusher.zjiecode.com/
开通步骤：
1、关注公众号，注册
2、通过公众号，获取UID
3、通过网站打开管理后台：http://wxpusher.zjiecode.com/admin/ 使用微信扫码登录，=》应用列表=》新建应用，如vnpy2，并获得APP_TOOKEN
4、应用列表=》应用（vnpy2）=》 关注.
'''

from threading import Lock, Thread
import time
import requests
import json
import sys
import traceback
from datetime import datetime
# from functools import wraps
from vnpy.trader.utility import print_dict

global wechat_lock
wechat_lock = Lock()

# 这里可以设置UIDS, 多个人可同时接收
UIDS = ['UID_kZguGPBQPWn41Ni9FK4CgPts2Kjx']

APP_TOKEN = 'AT_aDuiQu41dmAQV2vUMXOaaTDrWyhKJN2x'


class wechat_thread(Thread):
    def __init__(self, content):
        super(wechat_thread, self).__init__(name="wechat_thread")
        self.CORPID = 'ww7006a98ef46c2b44'
        self.CORPSECRET = 'CqvRT9iu3LwRSOqa9vhF8GETLylunZZjeU_5u_0gWNo'
        self.AGENTID = '1000002'
        self.TOUSER = "WangYang"  # 接收者用户名
        self.lock = wechat_lock
        self.message = content

    def _get_access_token(self):
        url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken'
        values = {'corpid': self.CORPID,
                  'corpsecret': self.CORPSECRET,
                  }
        req = requests.post(url, params=values)
        data = json.loads(req.text)
        return data["access_token"]

    def get_access_token(self):
        try:
            with open('./access_token.conf', 'r') as f:
                t, access_token = f.read().split()
        except:
            with open('./access_token.conf', 'w') as f:
                access_token = self._get_access_token()
                cur_time = time.time()
                f.write('\t'.join([str(cur_time), access_token]))
                return access_token
        else:
            cur_time = time.time()
            if 0 < cur_time - float(t) < 7260:
                return access_token
            else:
                with open('./access_token.conf', 'w') as f:
                    access_token = self._get_access_token()
                    f.write('\t'.join([str(cur_time), access_token]))
                    return access_token

    def run(self):
        send_url = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=' + self.get_access_token()
        send_values = {
            "touser": self.TOUSER,
            "msgtype": "text",
            "agentid": self.AGENTID,
            "text": {
                "content": self.message
            },
            "safe": "0"
        }
        send_msges = (bytes(json.dumps(send_values), 'utf-8'))
        respone = requests.post(send_url, send_msges)
        respone = respone.json()  # 当返回的数据是json串的时候直接用.json即可将respone转换成字典
        return


def send_wx_msg(*args, **kwargs):
    """
    发送微信Msg
    :param content:   发送内容
    :return:
    """
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

    t = wechat_thread(content=content)
    t.daemon = False
    t.run()

# class wechat_thread(Thread):
#     """
#     采用线程方式，不阻塞
#     """

#     def __init__(self, uids: list, content: str, topic_ids: list = [], url: str = '', app_token=''):

#         # text：消息标题，最长为256，必填。
#         # desp：消息内容，最长64Kb，可空，支持MarkDown。

#         super(wechat_thread, self).__init__(name="wechat_thread")
#         self.request_url = "http://wxpusher.zjiecode.com/api/send/message"
#         self.uids = uids
#         self.content = content
#         self.topic_ids = topic_ids
#         self.url = url
#         self.lock = wechat_lock
#         self.app_token = app_token if app_token is not None and len(app_token) > 0 else APP_TOKEN

#     def run(self):
#         if self.content is None or len(self.content) == 0:
#             return
#         params = {}
#         params['appToken'] = self.app_token
#         params['content'] = self.content
#         params['contentType'] = 1
#         params['topicIds'] = self.topic_ids
#         params['uids'] = self.uids
#         params['url'] = self.url

#         # 发送请求
#         try:
#             response = requests.post(self.request_url, json=params).json()
#             if not response.get('success', False):
#                 print(response)
#         except Exception as e:
#             print("{} 微信发送异常 ex:{},trace:{}".format(datetime.now(), str(e), traceback.format_exc()),
#                   file=sys.stderr)
#             return

#         print("wechat_thread sent successful!")


def send_wx_msg_bak(*args, **kwargs):
    """
    发送微信Msg
    :param content:   发送内容
    :return:
    """
    content = kwargs.get('content', None)
    if content is None:
        if len(args) == 0:
            return
        content = args[0]
    if len(content) == 0:
        return

    try:
        # 如果存在华富资产的微信模块，则使用
        from vnpy.trader.util_huafu import sendWeChatMsg, WECHAT_URL, WECHAT_GROUP, WECHAT_LEVEL_INFO, \
            WECHAT_MSG_TYPE_ALERT
        target = kwargs.get('target', 'XXX')
        sendWeChatMsg(content=content,
                      target=WECHAT_GROUP.get(target),
                      url=kwargs.get('url', WECHAT_URL),
                      level=kwargs.get('level', WECHAT_LEVEL_INFO),
                      msg_type=kwargs.get('msg_type', WECHAT_MSG_TYPE_ALERT))
        return
    except Exception as ex:
        print(f'发送微信异常:{str(ex)}', file=sys.stderr)
        pass

    # dict => str, none str => str
    if not isinstance(content, str):
        if isinstance(content, dict):
            content = '{}'.format(print_dict(content))
        else:
            content = str(content)

    uids = kwargs.get('uids', [])
    # 没有配置的话，使用缺省UID
    if len(uids) == 0:
        uids.extend(UIDS)

    app_token = kwargs.get('app_token')

    t = wechat_thread(uids=uids, content=content, app_token=app_token)
    t.daemon = False
    # t.run()
    t.start()


if __name__ == '__main__':
    text = u'微信测试标题!!!!\n第二行'

    send_wx_msg(text)
