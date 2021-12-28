# -*- coding: utf-8 -*-
import os

black_symbol_list = ['BTCSTUSDT']  # 不参与交易


# ===每次获取K线数据的数量

MAX_KEEP_LEN = 3000

needed_time_interval_list = ['1h','30m'] # 支持1h,30m,15m,5m配置


# 用于生成文件夹路径
def creat_folders(*args):
    abs_path = os.path.abspath(os.path.join(*args))
    if not os.path.exists(abs_path):
        os.makedirs(abs_path)
    return abs_path


_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
data_path_root = creat_folders(_, os.pardir, os.pardir, 'data','coin')
flag_path_root = creat_folders(_, os.pardir, os.pardir, 'data','flag')


DINGDING_ROBOT_ID =  ''
DINGDING_SECRET = ''

TELEGRAM_TOKEN = '1908201261:AAEND2CexRArAxxxxxxxx'
TELEGRAM_CHAT_ID = -590128xxxx


WECHAT_CORPID = ''
WECHAT_SECRET = ''
WECHAT_AGENT_ID = ''



DEFAULT_SLEEP_TIMES = 20
DEFAULT_TRY_TIMES = 10
TRADE_MARKET = 'DataManagerV1.0'