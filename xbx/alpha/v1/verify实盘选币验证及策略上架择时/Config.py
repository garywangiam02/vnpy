import ccxt
from Utility import robust
import configparser
import warnings
import pytz

tz_server = pytz.timezone('Asia/Shanghai')  # 此处填入你所在时区

warnings.filterwarnings('ignore')

config = configparser.ConfigParser()
config.read('config.ini')

# 子账号
# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': '',
    'secret': '',
    'timeout': 30000,
    'rateLimit': 10,
    'enableRateLimit': False,
    'options': {
        'adjustForTimeDifference': True,  # ←---- resolves the timestamp
        'recvWindow': 10000,
    },
}
exchange = ccxt.binance(BINANCE_CONFIG)

# ===交易策略列表
stratagy_list = [{'hold_period': '8H', 'c_factor': '2103_8H',
                  'factors': [{'factor': 'Bias', 'para': 0, 'if_reverse': False, 'diff': 0, 'weight': 1.0},
                              {'factor': 'Gap', 'para': 0, 'if_reverse': True, 'diff': 0, 'weight': 1.0},
                              ],
                  'selected_coin_num': 1}]

# ===每次获取K线数据的数量
LIMIT = 1500

# ===其他
long_sleep_time = 20
cheat_seconds = 0
