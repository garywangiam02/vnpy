# -*- coding: utf-8 -*-
from teamon.multi_trade import *

config = {
    'son1': {  # 该策略配置的名称随意，可以自己取名
        'symbol': 'ETHUSDT',  # 交易标的
        'strategy_name': 'real_signal_random',  # 你的策略函数名称
        'para': [500, 2],  # 参数
        'data_num': 1000,  # 策略函数需要多少根k线
        'time_interval': '1m',  # 策略的时间周期
        'leverage': 2,  # 策略基础杠杆
        'weight': 0.5,  # 策略分配的资金权重
    },
    'son2': {
        'symbol': 'DOTUSDT',
        'strategy_name': 'real_signal_random',
        'para': [80, 2],
        'data_num': 1500,
        'time_interval': '1h',
        'leverage': 2,
        'weight': 0.5,
    },
}

testapiKey = 'InWjP0J8xxxxxxxxxxxx'  # 币安apiKey
testsecret = '1xVHalcyxxxxxxxxxxxx'  # 币安的secret  
a = trade(apiKey=testapiKey, secret=testsecret, config=config, proxies=False, timeout=1)
