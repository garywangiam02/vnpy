# -*- coding: utf-8 -*-
import httpx, json, datetime, os, asyncio, time, re  # ,inspect


# ---------------------------------------装饰函数-------------------------------------
def asyncrun(func):  # 装饰函数，用于异步运行
    def wrapfunc(*arg, **kwargs):
        return asyncio.ensure_future(func(*arg, **kwargs))

    return wrapfunc


def get_min_interval(df):  # 从配置df获得最小的运行时间间隔
    rule = ['m', 'h', 'd']
    for rule_type in rule:
        _df = df[df.time_interval.str.contains(rule_type)]
        if _df.shape[0] > 0:
            return str(_df['time_interval'].apply(lambda x: int(x.replace(rule_type, ''))).min()) + rule_type


class send_wx():
    '''发送微信信息的实例'''
    corpid = 'wwd337xxxxxxxxx'
    corpsecret = 'yRIsOxxxxxxxxxx'
    agentid = 1000003

    def __init__(self, proxies=True):
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

    def send(self, *mssg):
        if datetime.datetime.now() > self.token_time + datetime.timedelta(hours=1.9):
            self.renew_token()
        text = ''
        for i in mssg:
            text += str(i)
        if self.last_send == text:  # 过滤掉重复消息
            return
        def send(single_text):
            data={
                "touser": "@all",
                "msgtype": "text",
                "agentid": self.agentid,
                "text": {
                    "content": single_text,
                    },
              }
            if self.token is not None:
                self.ret=httpx.post(self.api1+self.token,headers=self.header,data=json.dumps(data),proxies=self.proxies).text
        if len(text) >= 1000:
            text_list = re.findall(r'(?:.|\n){600,1000}\n|(?:.|\n)+',text)
            for single_text in text_list:
                send(single_text)
        else:
            send(text)
        self.last_send = text
