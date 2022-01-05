# -*- coding: utf-8 -*-
import requests
import json
import datetime
import re


class WechatRobot:
    '''
    企业微信机器人
    '''

    def __init__(self, corpid, secret, agentid) -> None:
        self.corpid = corpid
        self.secret = secret
        self.agentid = agentid
        self.renew_token()
        self.last_send = ''

    def renew_token(self):
        url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        Data = {
            "corpid": self.corpid,
            "corpsecret": self.secret
        }
        try:  # 容错
            r = requests.get(url=url, params=Data)
            self.token = r.json()['access_token']
        except Exception:
            print("send_message()的requests.get()失败，请检查网络连接。")
        self.token_time = datetime.datetime.now()

    def send_msg(self, *mssg):
        content = ''
        for i in mssg:
            content += str(i)
        if self.last_send == content:
            return

        def send_msg(single_text):
            # Token是服务端生成的一串字符串，以作客户端进行请求的一个令牌
            # 当第一次登录后，服务器生成一个Token便将此Token返回给客户端
            # 以后客户端只需带上这个Token前来请求数据即可，无需再次带上用户名和密码
            url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(self.token)
            data = {
                "touser": "WangYang",
                "msgtype": "text",
                "agentid": self.agentid,
                "text": {"content": single_text + '\n' + datetime.datetime.now().strftime("%m-%d %H:%M:%S")},
                "safe": "0"
            }

            try:  # 容错
                result = requests.post(url=url, data=json.dumps(data))
                print('成功发送微信')
            except Exception:
                print("send_message()的requests.post()失败，请检查网络连接。")
        if len(content) >= 1000:
            text_list = re.findall(r'(?:.|\n){600,1000}\n|(?:.|\n)+', content)
            for single_text in text_list:
                send_msg(single_text)
        else:
            send_msg(content)
        return
