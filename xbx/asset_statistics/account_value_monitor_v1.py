# -*-coding:utf-8-*-
from datetime import datetime
import ccxt
import pandas as pd
from hashlib import sha256
import os
import hmac
import hashlib
import base64
from urllib import parse
import json
import requests
import time
from vnpy.trader.util_wechat import send_wx_msg

pd.set_option("display.max_rows", 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

robot_id = ''  # 填写钉钉id
secret = ''  # 填写钉钉secret
ttl = [
    {
        'account_name': 'garywangiam01',  # 账户1
        'apiKey': 'hUHUVDGd3spe6zsBpcpNM0VaWJ360UKSmdRBvxZSzZYBjaDhhrFnlMqTYlrQ62p5',
        'secret': 'qDIipIkqsTbnXhfeHsLctVSpHFYEQo6bYVMh9Rw47mo8wsOuoc9JXBIXfXRTlRau',
    },
    {
        'account_name': 'garywangiam02',  # 账户2
        'apiKey': '2zkT77vukd7OMogmyOa9czbbF4cZTO7iyWeoXCE3BKWUJ5eDNjyauCGYTGCRGx62',
        'secret': 'uJdJZGGnOXjiYh8OLZ9MEbWQtdLgcaJaN9NQkWNjPaKJjdlJETmO9Z57PKxS8hCQ',
    },
    {
        'account_name': 'garywangiam03',  # 账户3
        'apiKey': '4pokhR5naxqW9DaenMpUdU7qcvDsRuvKvy8OzlABrlPxWo640Y5Q8eKNMdniU7i8',
        'secret': 'g8u9Aj6x7IldERb9MGzDDiVCPSK9GIYVhyVLOj6ZCijRXKElqs8Ri5cRoJhRrMPn',
    },
]

ttl_msg = '账户统计\n\n\n'
ttl_value = 0


for i in ttl:
    BINANCE_CONFIG = {
        'apiKey': i.get('apiKey'),
        'secret': i.get('secret'),
        'timeout': 3000,
        'rateLimit': 10,
        # 'hostname': 'binancezh.com',  # 无法fq的时候启用
        'enableRateLimit': False
    }
    exchange = ccxt.binance(BINANCE_CONFIG)

    def get_u_balance():
        # u本位合约u_balance
        u_balance = exchange.fapiPrivateV2_get_balance()
        u_balance_usdt = 0
        for item in u_balance:
            if float(item.get('balance', 0)) > 0:
                if item['asset'] == 'USDT':
                    u_balance_usdt = float(item['balance']) + float(item['crossUnPnl'])
        return u_balance_usdt

    def get_bi_balance_usdt():
        # 币本位合约
        bi_balance = exchange.dapiPrivate_get_balance()
        bi_one = {}
        for item in bi_balance:
            if float(item.get('balance', 0)) > 0:
                bi_one[item['asset']] = float(item['balance']) + float(item['crossUnPnl'])
        bi_balance_usdt = 0
        for k, v in bi_one.items():
            delivery_symbol = '{}{}'.format(k, 'USD_PERP')
            params = {'symbol': delivery_symbol, 'limit': 5}
            delivery_buy1_price = float(exchange.dapiPublicGetDepth(params=params)['bids'][0][0])
            one_bi_balance_usdt = float(delivery_buy1_price) * float(v)
            bi_balance_usdt += one_bi_balance_usdt
        return bi_balance_usdt

    def get_spot_balance_usdt():
        # 现货
        balance = exchange.privateGetAccount()
        balance_list = balance['balances']
        res_list = []
        spot_balance_usdt = 0
        for item in balance_list:
            if float(item['free']) > 0:
                res_list.append({item['asset']: float(item['free'])})
                if item['asset'] == 'USDT':
                    spot_balance_usdt += float(item['free'])
                else:
                    if item['asset'] == 'LDBNB':
                        item['asset'] = 'BNB'
                    spot_sell1_price = exchange.fetch_ticker('{}/USDT'.format(item['asset']))
                    spot_balance_usdt += float(item['free']) * float(spot_sell1_price['close'])
        return spot_balance_usdt

    u_balance_usdt = get_u_balance()
    bi_balance_usdt = get_bi_balance_usdt()
    spot_balance_usdt = get_spot_balance_usdt()

    value = float(u_balance_usdt) + float(bi_balance_usdt) + float(spot_balance_usdt)
    msg = '账户名:' + i.get('account_name', '') + '\n\n'
    msg += '合计余额:' + str(value) + '\n\n'
    msg += '现货余额USDT:' + str(spot_balance_usdt) + '\n\n'
    msg += '币本位合约余额USDT:' + str(bi_balance_usdt) + '\n\n'
    msg += 'U本位合约余额USDT:' + str(u_balance_usdt) + '\n\n\n'
    one = {
        '账户名': i.get('account_name', ''),
        '合计余额': value,
        '现货余额': spot_balance_usdt,
        '币本位余额': bi_balance_usdt,
        'U本位余额': u_balance_usdt,
        '记录时间': datetime.now(),
    }
    ttl_msg += msg
    ttl_value += value

    df = pd.DataFrame()
    df = df.append(one, ignore_index=True)
    path = './account_value.csv'
    if os.path.exists(path):
        df.to_csv(path, mode='a', index=False, header=False, encoding="utf_8_sig")
    else:
        df.to_csv(path, mode='a', index=False, encoding="utf_8_sig")


ttl_msg += '合计余额USDT' + str(ttl_value) + '\n\n'
ttl_msg += '记录时间' + str(datetime.now()) + '\n'
print(ttl_msg)
send_wx_msg(ttl_msg)
