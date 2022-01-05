# -*- coding: utf-8 -*-
import asyncio
import httpx
import json
import datetime
import time
import hmac
import requests
from trading.utility import *
from hashlib import sha256

requests.packages.urllib3.disable_warnings()  # 禁用关闭ssl证书验证而产生的警告


class Binance():
    methods = {  # private 0-2 分别对应接口鉴权从低到高
        # 现货类
        'get_s_exchangeinfo': {'url': '/api/v3/exchangeInfo', 'method': 'get', 'private': 0},
        'get_s_history': {'url': '/api/v3/klines', 'method': 'get', 'private': 0},
        # u本位合约类
        'f_balance': {'url': '/fapi/v2/balance', 'method': 'get', 'private': 2},
        'f_account': {'url': '/fapi/v2/account', 'method': 'get', 'private': 2},
        'f_order': {'url': '/fapi/v1/order', 'method': 'post', 'private': 2},  # 这个方法别换名字
        'get_f_order': {'url': '/fapi/v1/order', 'method': 'get', 'private': 2},
        'change_f_leverage': {'url': '/fapi/v1/leverage', 'method': 'post', 'private': 2},
        'get_f_history': {'url': '/fapi/v1/klines', 'method': 'get', 'private': 0},
        'get_f_price': {'url': '/fapi/v1/ticker/price', 'method': 'get', 'private': 0},
        'get_f_ticker': {'url': '/fapi/v1/ticker/24hr', 'method': 'get', 'private': 0},
        'get_f_exchangeinfo': {'url': '/fapi/v1/exchangeInfo', 'method': 'get', 'private': 0},
        'f_time': {'url': '/fapi/v1/time', 'method': 'get', 'private': 0},
        # 币本位类
        'get_d_exchangeinfo': {'url': '/dapi/v1/exchangeInfo', 'method': 'get', 'private': 0},
    }
    shost = 'https://api.binance.com'  # 现货/杠杆/币安宝/矿池
    dhost = 'https://dapi.binance.com'  # dapi 币本位合约
    fhost = 'https://fapi.binance.com'  # fapi u本位合约

    def __init__(self, apiKey=None, secret=None, notify_sender=None, proxies=False, timeout=1, order_timeout=10, verify=False, get_server_time=False, verbose=True):
        self.apiKey = apiKey
        self.secret = secret
        self.proxies = "http://localhost:1080" if proxies else None
        self.client = httpx.Client(
            http2=True, proxies=self.proxies, timeout=timeout, verify=verify)
        self.timeout = timeout
        self.order_timeout = order_timeout
        self.verify = verify
        # self.sender = send_wx(proxies=proxies)  # 紧急微信报错发送器
        self.sender = notify_sender  # 紧急tg报错发送器
        self.get_server_time = get_server_time
        self.verbose = verbose

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            kwargs['func'] = name
            return self.act(**kwargs) if name.find('async') < 0 else self.asyncact(**kwargs)
        return wrapper

    def async_repeat(func):
        @asyncrun
        async def wrapfunc(self, *args, **kwargs):
            func_name = kwargs['func']
            try:
                ret = await func(self, *args, **kwargs)
                ret = json.loads(ret)
                if type(ret) is dict:
                    if ret.get('code') is None:
                        return ret
                    else:
                        print(ret, func_name, '\n交易所返回报错')
                        self.sender.send_msg(
                            ret, func_name, '\n交易所返回报错')  # 交易所报错需要及时发现
                        # raise ValueError
                else:
                    return ret
            except Exception as e:
                # 从交易所获取信息出错,网络等问题
                if self.verbose:
                    # traceback.print_exc()
                    print(str(datetime.datetime.now()),
                          func_name, '运行出错，\n错误信息：', e)
                await asyncio.sleep(0.5)
                return await wrapfunc(self, *args, **kwargs)
        return wrapfunc

    def repeat(func):
        # 装饰函数，用于网络不佳时重复尝试
        def wrapfunc(self, *args, **kwargs):
            func_name = kwargs['func']
            try:
                ret = func(self, *args, **kwargs)
                ret = json.loads(ret)
                if type(ret) is dict:
                    if ret.get('code') is None:
                        return ret
                    else:
                        print(ret, func_name, '\n交易所返回报错')
                        print(ret)
                        print(func_name)
                        self.sender.send_msg(ret, func_name, '\n交易所返回报错')
                        # raise ValueError
                else:
                    return ret
            except Exception as e:
                # 从交易所获取信息出错,网络等问题
                if self.verbose:
                    print(str(datetime.datetime.now()),
                          func_name, '运行出错，错误信息：', e)
                time.sleep(0.5)
                return wrapfunc(self, *args, **kwargs)
        return wrapfunc

    def param_to_query(self, x):
        return '&'.join(['='.join((str(i), str(j))) for i, j in x.items()])

    def get_timestamp(self, get_server_time=True):
        if get_server_time:
            try:
                return self.server_time()['serverTime']
            except Exception as e:
                print(e)
                time.sleep(0.5)
                return self.get_timestamp(get_server_time=get_server_time)
        else:
            return int(round(time.time() * 1000))

    def server_time(self):
        return json.loads(self.client.get(self.fhost + '/fapi/v1/time').text)

    def get_func_info(self, func, kwargs):
        if self.methods[func]['url'].find('fapi') >= 0:
            url = self.fhost + self.methods[func]['url']
        elif self.methods[func]['url'].find('dapi') >= 0:
            url = self.dhost + self.methods[func]['url']
        else:
            url = self.shost + self.methods[func]['url']
        headers = {} if self.methods[func]['private'] == 0 else {
            'X-MBX-APIKEY': self.apiKey}
        if self.methods[func]['private'] >= 1:
            kwargs['timestamp'] = self.get_timestamp(
                get_server_time=self.get_server_time)
            if self.methods[func]['private'] == 2:
                kwargs['signature'] = hmac.new(self.secret.encode(
                    'utf-8'), self.param_to_query(kwargs).encode('utf-8'), digestmod=sha256).hexdigest()
        return url, headers, kwargs

    @repeat
    def act(self, **kwargs):
        func = kwargs.pop('func')
        url, headers, kwargs = self.get_func_info(func, kwargs)
        ret = self.client.get(url, headers=headers, params=kwargs) if self.methods[func]['method'] == 'get' else self.client.post(
            url, headers=headers, data=kwargs)
        return ret.text

    @asyncrun
    @async_repeat
    async def asyncact(self, **kwargs):
        func = kwargs.pop('func').replace('async', '')
        url, headers, kwargs = self.get_func_info(func, kwargs)
        timeout = self.order_timeout if func == 'f_order' else self.timeout
        async with httpx.AsyncClient(http2=True, proxies=self.proxies, timeout=timeout, verify=self.verify) as client:
            if self.methods[func]['method'] == 'get':
                ret = await client.get(url, headers=headers, params=kwargs)
            else:
                ret = await client.post(url, headers=headers, data=kwargs)
        return ret.text
