import ccxt

from Utility import robust
from notify import *
import configparser
import warnings
warnings.filterwarnings('ignore')
import os, sys
os.chdir(sys.path[0])
config = configparser.ConfigParser()
config.read('config.ini')

# 子账号
# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': '',
    'secret': '',
    'timeout': 30000,
    'rateLimit': 10,
    'hostname': 'binancezh.com',  # 无法fq的时候启用
    'enableRateLimit': False,
    'options': {
        'adjustForTimeDifference': True,  # ←---- resolves the timestamp
        'recvWindow': 10000,
    },
}
exchange = ccxt.binance(BINANCE_CONFIG)



trade_usdt = 0 

black_symbol_list = ['BTCSTUSDT']



# ===获取交易所相关数据
exchange_info = robust(exchange.fapiPublic_get_exchangeinfo)
_symbol_list = [x['symbol'] for x in exchange_info['symbols']]
_symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')] #过滤usdt合约
symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list] # 过滤黑名单

# ===交易策略列表
selected_coin_num = int(len(symbol_list) / 2)
selected_strategy_num = 15
stratagy_list = [   
    {
        'hold_period': '1H',  # 持仓周期
        'c_factor': 'c11', # 复合因子1号
        'factors':[
            {
            'factor': 'adapt_boll_v3_sharpe',   # 选币时参考的因子
            'para': 11,  # 策略的参数
            'if_reverse': True,
            'weight':0.4,
            },
            {
            'factor': 'adapt_boll_v3_equity',   # 选币时参考的因子
            'para': 11,  # 策略的参数
            'if_reverse': True,
            'weight':0.6,
            },
        ],
        'selected_coin_num': selected_coin_num,  # 做空或做多币的数量
    },
    {
        'hold_period': '1H',  # 持仓周期
        'c_factor': 'c12', # 复合因子1号
        'factors':[
            {
            'factor': 'adapt_boll_v3_sharpe',   # 选币时参考的因子
            'para': 12,  # 策略的参数
            'if_reverse': True,
            'weight':0.5,
            },
            {
            'factor': 'adapt_boll_v3_equity',   # 选币时参考的因子
            'para': 12,  # 策略的参数
            'if_reverse': True,
            'weight':0.5,
            },
        ],
        'selected_coin_num': selected_coin_num,  # 做空或做多币的数量
    },    
]

cta_config = {
    'UNFIUSDT_adaptboll_v3_with_stoploss_1h': {  # 该策略配置的名称随意，可以自己取名
        'symbol': 'UNFIUSDT',  # 交易标的
        'strategy_name': 'adaptboll_v3_with_stoploss',  # 你的策略函数名称
        'para': [11],  # 参数
        'data_num': 1200,  # 策略函数需要多少根k线
        'time_interval': '1h',  # 策略的时间周期
        'leverage': 2,  # 策略基础杠杆
        'weight': 0.15,  # 策略分配的资金权重
    }
}

# ===每次获取K线数据的数量
LIMIT = 1500


# ===其他
long_sleep_time = 20
cheat_seconds = 0


# 用于生成文件夹路径
def creat_folders(*args):
    abs_path = os.path.abspath(os.path.join(*args))
    if not os.path.exists(abs_path):
        os.makedirs(abs_path)
    return abs_path


_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
path_root_out = creat_folders(_, os.pardir, os.pardir, 'data', 'output')


strategy_config_file_path = f'{path_root_out}/strategy_config.ini'
