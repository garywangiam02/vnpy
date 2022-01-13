"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import ccxt
import pandas as pd
from pprint import pprint
import math
from Utility import robust
import configparser
import warnings
warnings.filterwarnings('ignore')

config = configparser.ConfigParser()
config.read('config.ini')

# 子账号
# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': config['binance']['apiKey'],
    'secret': config['binance']['secret'],
    'timeout': 30000,
    'rateLimit': 10,
    # 'hostname': 'binancezh.com',  # 无法fq的时候启用
    'enableRateLimit': False,
    'options': {
        'adjustForTimeDifference': True,  # ←---- resolves the timestamp
        'recvWindow': 10000,
    },
}
exchange = ccxt.binance(BINANCE_CONFIG)


# ===用于交易的资金
# 获取当前账户净值
balance = robust(exchange.fapiPrivate_get_balance,)  # 获取账户净值
balance = pd.DataFrame(balance)
equity = float(balance[balance['asset'] == 'USDT']['balance'])
print('当前账户净资产：', equity)
trade_usdt = equity # 500

# ===交易策略列表
stratagy_list = [
    # # 策略1
    # {
    #     'hold_period': '6H',  # 持仓周期
    #     'c_factor': 'c001', # 复合因子1号
    #     'factors':[
    #         {
    #         'factor': 'bias',   # 选币时参考的因子
    #         'para': 4,  # 策略的参数
    #         'if_reverse':False,
    #         'weight':1.0,
    #         },
    #         {
    #         'factor': 'cci',   # 选币时参考的因子
    #         'para': 36,  # 策略的参数
    #         'if_reverse':True,
    #         'weight':0.3,
    #         },
    #
    #     ],
    #     'selected_coin_num': 1,  # 做空或做多币的数量
    # },
    # 策略2
    {
        'hold_period': '6H',  # 持仓周期
        'c_factor': 'lasso',  # 复合因子1号
        # 'factor': 'linregr',
        # 'para': 0,
        'if_reverse': True,
        # 'weight': 1.0,
        'selected_coin_num': 1,  # 做空或做多币的数量
    },

]


# ===每次获取K线数据的数量
LIMIT = 1000

black_symbol_list = ['BTCSTUSDT']

# ===获取交易所相关数据
exchange_info = robust(exchange.fapiPublic_get_exchangeinfo)
_symbol_list = [x['symbol'] for x in exchange_info['symbols']]
_symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')] #过滤usdt合约
symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list] # 过滤黑名单


# ===从exchange_info中获取每个币种最小交易量
min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}
# 案例：{'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0}

# ===从exchange_info中获取每个币种下单精度
price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in exchange_info['symbols']}
# 案例：{'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3}


# ===其他
long_sleep_time = 20
cheat_seconds = 0