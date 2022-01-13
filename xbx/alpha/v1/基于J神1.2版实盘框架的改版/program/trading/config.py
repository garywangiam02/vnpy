# -*- coding: utf-8 -*-

"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import json
from threading import Lock
from typing import Dict
import ccxt
import pandas as pd
import math
import time

import warnings

warnings.filterwarnings('ignore')
utc_offset = int(time.localtime().tm_gmtoff / 60 / 60)
time_zone_str = f"{'+' if utc_offset >= 0 else '-'}{utc_offset}:00"

config_filepath = '/Users/gary/workspace/alpha/v1/基于J神1.2版实盘框架的改版/program/trading/config.json'
config_encoding = 'utf-8'

pre_cal_key = '_pre_cal'


class Config(Dict):
    lock = Lock()

    def __init__(self, origin: Dict = None, pre_calculate: bool = True):
        super(Config).__init__()
        self.refresh_config(origin, pre_calculate)

    def refresh_config(self, origin: Dict, pre_calculate: bool):
        with self.lock:
            _config = None
            if origin is None:
                with open(config_filepath, encoding=config_encoding) as config_file:
                    _config = json.load(config_file)
            else:
                _config = origin
            self.clear()
            self.update(_config)
            if pre_cal_key not in self:
                self[pre_cal_key] = {}
            if pre_calculate:
                self.pre_cal_arguments()

    def pre_cal_arguments(self):
        # 子账号
        # ===创建交易所
        BINANCE_CONFIG = {
            'apiKey': self['binance']['apiKey'],
            'secret': self['binance']['secret'],
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
        balance = self.robust(exchange.fapiPrivate_get_balance, )  # 获取账户净值
        balance = pd.DataFrame(balance)
        equity = float(balance[balance['asset'] == 'USDT']['balance'])
        print('当前账户净资产：', equity)
        trade_usdt = equity  # 500

        # ===获取交易所相关数据
        exchange_info = self.robust(exchange.fapiPublic_get_exchangeinfo)
        _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
        # print(_symbol_list)

        symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]
        # print(symbol_list)

        # ===从exchange_info中获取每个币种最小交易量
        min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}
        # 案例：{'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0}

        self[pre_cal_key]['min_qty'] = min_qty

        # ===从exchange_info中获取每个币种下单精度
        price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in
                           exchange_info['symbols']}
        # 案例：{'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3}

        self[pre_cal_key]['price_precision'] = price_precision
        self[pre_cal_key]['exchange'] = exchange

    def run_function_till_success(self, function, tryTimes=5, sleepTimes=60):
        '''
        将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
        '''
        retry = 0
        while True:
            if retry > tryTimes:
                return False
            try:
                result = function()
                return [result, retry]
            except (Exception) as reason:
                print(reason)
                retry += 1
                if sleepTimes != 0:
                    time.sleep(sleepTimes)  # 一分钟请求20次以内

    def robust(self, actual_do, *args, **keyargs):
        tryTimes = int(self['robust']['try_times'])
        sleepTimes = int(self['robust']['sleep_seconds'])
        result = self.run_function_till_success(function=lambda: actual_do(*args, **keyargs), tryTimes=tryTimes,
                                                sleepTimes=sleepTimes)
        if result:
            return result[0]
        else:
            print("trade_market + ':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数'")
            exit()
