# -*- coding: utf-8 -*-

import time
import hmac
import hashlib
import base64
import urllib
import json
import requests
import logging
import httpx
import datetime
from Utility import *
import telegram



class wechat_robot():
    '''发送微信信息的实例'''

    def __init__(self, corpid,corpsecret,agentid = 1000003,proxies=True):
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.agentid = agentid
        self.proxies = "http://localhost:1080" if proxies else None
        self.header = {'Content-Type': 'application/json'}
        self.api = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=' + self.corpid + '&corpsecret=' + self.corpsecret
        self.api1 = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token='
        self.renew_token()
        self.last_send = ''

    def renew_token(self):
        try:
            self.token = json.loads(httpx.post(self.api, proxies=self.proxies).text)['access_token']
        except Exception as e:
            print('企业微信配置错误或连接失败\n', e)
            self.token = None
        self.token_time = datetime.datetime.now()

    def send_msg(self, *mssg):
        if datetime.datetime.now() > self.token_time + datetime.timedelta(hours=1.9):
            self.renew_token()
        text = ''
        for i in mssg:
            text += str(i)
        if self.last_send == text:  # 过滤掉重复消息
            return
        data = {
            "touser": "@all",
            "msgtype": "text",
            "agentid": self.agentid,
            "text": {
                "content": text,
            },
        }
        if self.token is not None:
            self.ret = httpx.post(self.api1 + self.token, headers=self.header, data=json.dumps(data),
                                  proxies=self.proxies).text
        self.last_send = text

        
class DingTalkRobot(object):
    def __init__(self, robot_id, secret):
        super(DingTalkRobot, self).__init__()
        self.robot_id = robot_id
        self.secret = secret
        self.headers = {'Content-Type': 'application/json; charset=utf-8'}
        self.times = 0
        self.start_time = time.time()

    # 加密签名
    def __spliceUrl(self):
        timestamp = int(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = "https://oapi.dingtalk.com/robot/send?access_token="+f"{self.robot_id}&timestamp={timestamp}&sign={sign}"
        return url

    def send_markdown(self,title, markdown_msg, is_at_all=False, at_mobiles=[]):
        data = {"msgtype": "markdown", "at": {}}
        if self.is_not_null_and_blank_str(markdown_msg):
            titleShow = "### "+title+"\n\n"
            data["markdown"] = {"title": title,"text": titleShow+markdown_msg}
        else:
            logging.error("markdown类型，消息内容不能为空！")
            raise ValueError("markdown类型，消息内容不能为空！")

        if is_at_all:
            data["at"]["isAtAll"] = is_at_all

        if at_mobiles:
            at_mobiles = list(map(str, at_mobiles))
            data["at"]["atMobiles"] = at_mobiles

        logging.debug('markdown类型：%s' % data)
        return self.__post(data)

    def send_msg(self, *mssg):
        text = ''
        for i in mssg:
            text += str(i)
        self.send_text(text)

    
    def send_text(self, msg, is_at_all=False, at_mobiles=[]):
        data = {"msgtype": "text", "at": {}}
        if self.is_not_null_and_blank_str(msg):
            data["text"] = {"content": msg}
        else:
            logging.error("text类型，消息内容不能为空！")
            raise ValueError("text类型，消息内容不能为空！")

        if is_at_all:
            data["at"]["isAtAll"] = is_at_all

        if at_mobiles:
            at_mobiles = list(map(str, at_mobiles))
            data["at"]["atMobiles"] = at_mobiles

        logging.debug('text类型：%s' % data)
        return self.__post(data)


    def send_json(self, msg, is_at_all=False, at_mobiles=[]):
        data = {"msgtype": "text", "at": {}}
        if msg :
            json_msg = json.dumps(msg,ensure_ascii=False)
            data["text"] = {"content": json_msg}
        else:
            logging.error("text类型，消息内容不能为空！")
            raise ValueError("text类型，消息内容不能为空！")

        if is_at_all:
            data["at"]["isAtAll"] = is_at_all

        if at_mobiles:
            at_mobiles = list(map(str, at_mobiles))
            data["at"]["atMobiles"] = at_mobiles

        logging.debug('text类型：%s' % data)
        return self.__post(data)


    def send_image(self, title,image_url, is_at_all=False, at_mobiles=[]):
        markdown_msg = "!["+title+"]("+image_url+")\n"
        return self.send_markdown(title,markdown_msg,is_at_all)   


    def __post(self, data):
        """
        发送消息（内容UTF-8编码）
        :param data: 消息数据（字典）
        :return: 返回发送结果
        """
        self.times += 1
        if self.times > 20:
            if time.time() - self.start_time < 60:
                logging.debug('钉钉官方限制每个机器人每分钟最多发送20条，当前消息发送频率已达到限制条件，休眠一分钟')
                time.sleep(60)
            self.start_time = time.time()

        post_data = json.dumps(data)
        try:
            response = requests.post(self.__spliceUrl(), headers=self.headers, data=post_data)
            logging.debug('成功发送钉钉%'+str(response))
        except Exception as e:
            logging.debug('发送钉钉失败:' +str(e))

    def is_not_null_and_blank_str(self,content):
        if content and content.strip():
            return True
        else:
            return False            


class TgRobot(object):

    def __init__(self, token, chat_id):
        super(TgRobot, self).__init__()
        self.token = token
        self.chat_id = chat_id
        self.bot = telegram.Bot(token)

    
    def send_text(self,content):
      '''
      发送文本消息
      '''
      if content == None:
         return
      self.bot.send_message(self.chat_id,content)


    def send_dataframe(self,content):
      '''
      发送dataframe消息
      '''
      self.bot.send_message(self.chat_id,content.to_markdown(),parse_mode='Markdown')

    def send_photo():
        pass

    def send_msg(self, *mssg):
        text = ''
        for i in mssg:
            text += str(i)
        self.bot.send_message(self.chat_id,text)



if __name__ == '__main__':
    robot_id = "XXXX"
    secret = "XXXX"
    ding = DingTalkRobot(robot_id, secret)
    print(ding.send_text('601308'))
