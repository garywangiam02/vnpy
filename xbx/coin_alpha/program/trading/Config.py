"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import ccxt
import pandas as pd
import math


# ===账户交易资金相关参数
Update_Day = 1  # 每隔多少天更新一次trade_usdt。其中trade_usdt为用于交易选币策略的总资金。
Update_Hour = 11  # 当当天要更新trade_usdt时，在当天的几点进行更新。
Percent = 0.8  # 使用trade_usdt的多少比例进行交易，<=1，>0
File_Name = 'trade_usdt_history.txt'  # 保存历史trade_usdt的文件


# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': '2qJN1Id5BXvTPbPUqzKxEiHSDxhdDd8uEXmqk8YmeaO0U6HdE59A4Ek0do1pYBDQ',
    'secret': 'PgrYNuX7jI83n3d6z4yM1TNsMn19Gr3IiIt1dS0wAfotmr9YbkAjdUYT0AauYF11',
    'timeout': 3000,
    'rateLimit': 10,
    'hostname': 'binancezh.com',  # 无法fq的时候启用
    'enableRateLimit': False}
exchange = ccxt.binance(BINANCE_CONFIG)


# ===交易策略列表
stratagy_list = [
    # # 策略1
    # {
    #     'factor': 'zhenfu',   # 选币时参考的因子
    #     'para': 8,  # 策略的参数
    #     'hold_period': '3H',  # 持仓周期
    #     'selected_coin_num': 1,  # 做空或做多币的数量
    # },

    # # 策略2
    # {
    #     'factor': 'contrarian',
    #     'para': 6,
    #     'hold_period': '3H',
    #     'selected_coin_num': 1,
    # },
    # 策略3
    {
        'factor': 'cmo',
        'para': 6,
        'hold_period': '3H',
        'selected_coin_num': 1,
    },
]


# ===每次获取K线数据的数量
LIMIT = 100


# ===选币交易的币种
symbol_list = ['BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'EOSUSDT', 'LTCUSDT', 'TRXUSDT', 'ETCUSDT', 'LINKUSDT',
               'XLMUSDT', 'ADAUSDT', 'XMRUSDT', 'DASHUSDT', 'ZECUSDT', 'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT',
               'IOTAUSDT', 'BATUSDT', 'VETUSDT', 'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT',
               'KNCUSDT', 'ZRXUSDT', 'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'LENDUSDT', 'KAVAUSDT', 'BANDUSDT',
               'RLCUSDT']


# ===获取交易所相关数据
exchange_info = exchange.fapiPublic_get_exchangeinfo()
# symbol_list = [x['symbol'] for x in exchange_info['symbols']]  # 获取所有可交易币种的list

# 从exchange_info中获取每个币种最小交易量
min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}
# 案例：{'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0}

# 从exchange_info中获取每个币种下单精度
price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['minPrice']), 0.1)) for x in exchange_info['symbols']}
# 案例：{'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3}


# ===钉钉机器人配置信息
dingding_id = ''
dingding_secret = ''


# ===其他
short_sleep_time = 5
long_sleep_time = 20
