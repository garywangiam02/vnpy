# -*- coding: utf-8 -*-
import requests
import json
import datetime


class WechatRobot:
    '''
    企业微信机器人
    '''

    def __init__(self, corpid, secret, agentid) -> None:
        self.corpid = corpid
        self.secret = secret
        self.agentid = agentid

    def send_msg(self, *mssg):
        content = ''
        for i in mssg:
            content += str(i)

        url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        Data = {
            "corpid": self.corpid,
            "corpsecret": self.secret
        }

        try:  # 容错
            r = requests.get(url=url, params=Data)
        except Exception:
            print("send_message()的requests.get()失败，请检查网络连接。")
        # print(r.json())
        # exit()
        token = r.json()['access_token']
        # Token是服务端生成的一串字符串，以作客户端进行请求的一个令牌
        # 当第一次登录后，服务器生成一个Token便将此Token返回给客户端
        # 以后客户端只需带上这个Token前来请求数据即可，无需再次带上用户名和密码
        url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(token)
        data = {
            "touser": "WangYang",
            "msgtype": "text",
            "agentid": self.agentid,
            "text": {"content": content + '\n' + datetime.datetime.now().strftime("%m-%d %H:%M:%S")},
            "safe": "0"
        }

        try:  # 容错
            result = requests.post(url=url, data=json.dumps(data))
            print('成功发送微信')
        except Exception:
            print("send_message()的requests.post()失败，请检查网络连接。")
        return result.text

    # 发送图片
    def send_photo(self, path):
        gurl = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={}&corpsecret={}".format(corpid, secret)
        try:  # 容错
            r = requests.get(gurl)
        except Exception:
            print("send_picture()的requests.get()失败，请检查网络连接。")

        # get token
        dict_result = (r.json())
        Gtoken = dict_result['access_token']
        # print(Gtoken)

        curl = 'https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token={token}&type=image'.format(token=Gtoken)
        files = {'image': open(path, 'rb')}
        try:  # 容错
            r = requests.post(curl, files=files)
        except Exception:
            print("send_picture()的requests.post()失败，请检查网络连接。")

        re = json.loads(r.text)
        media_id = re['media_id']

        Url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        Data = {
            "corpid": self.corpid,
            "corpsecret": self.secret
        }
        try:  # 容错
            r = requests.get(url=Url, params=Data)
        except Exception:
            print("send_picture()的requests.get()失败，请检查网络连接。")

        token = r.json()['access_token']
        url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(token)
        data = {
            "toparty": '1',
            "msgtype": "image",
            "agentid": self.agentid,
            "image": {"media_id": media_id},
            "safe": "0"
        }
        try:  # 容错
            result = requests.post(url=url, data=json.dumps(data))
        except Exception:
            print("send_picture()的requests.post()失败，请检查网络连接。")
        return result

    # 发送文件
    def send_file(self, path):
        gurl = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={}&corpsecret={}".format(corpid, secret)
        try:  # 容错
            r = requests.get(gurl)
        except Exception:
            print("send_file()的requests.get()失败，请检查网络连接。")
        dict_result = (r.json())
        Gtoken = dict_result['access_token']
        curl = 'https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token={token}&type=file'.format(token=Gtoken)
        files = {'file': open(path, 'rb')}
        try:  # 容错
            r = requests.post(curl, files=files)
        except Exception:
            print("send_file()的requests.post()失败，请检查网络连接。")

        re = json.loads(r.text)
        media_id = re['media_id']

        Url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        Data = {
            "corpid": self.corpid,
            "corpsecret": self.secret
        }
        try:  # 容错
            r = requests.get(url=Url, params=Data)
        except Exception:
            print("send_file()的requests.get()失败，请检查网络连接。")
        token = r.json()['access_token']
        url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(token)
        data = {
            "toparty": "1",
            "msgtype": "file",
            "agentid": self.agentid,
            "file": {"media_id": media_id},
            "safe": "0"
        }
        try:  # 容错
            result = requests.post(url=url, data=json.dumps(data))
        except Exception:
            print("send_file()的requests.post()失败，请检查网络连接。")
        return result
