'''
0_获取币安数据.py
通过币安接口获取k线数据，保存在本地
区分现货数据与u本位永续数据
'''

import pandas as pd
import ccxt
import time
import os
import glob
from datetime import timedelta, date
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
from Function import *

import requests

def get_klines(symbol='BTCUSDT', spot_or_swap='spot', interval='5m',
               start_time=None, end_time=None, limit=500):

    # api对应现货      https://binance-docs.github.io/apidocs/spot/cn/#k
    # fapi对应u本位合约 https://binance-docs.github.io/apidocs/futures/cn/#k
    # dapi对应币本位合约 https://binance-docs.github.io/apidocs/futures/cn/#k
    if spot_or_swap == 'spot':
        base_url = 'https://api.binance.com'
        path = '/api/v3/klines'
    elif spot_or_swap == 'swap':
        base_url = 'https://fapi.binance.com'
        path = '/fapi/v1/klines'
    else:
        raise ValueError('spot_or_swap参数必须传入spot or swap')

    params = {"symbol": symbol,
              "interval": interval,
              "limit": limit
              }
    if start_time:
        params['startTime'] = start_time

    if end_time:
        params['endTime'] = end_time

    url = base_url + path
    # 此处暂时没有加容错
    response_data = requests.get(url, params=params, timeout=5).json()

    return response_data


def get_history_k_lines(symbol='BTC-USDT', spot_or_swap='spot', time_interval='1h',
                        start_time=pd.to_datetime('2021-01-01 00:00:00'),
                        end_time=pd.to_datetime(date.today())):

    # =====设定参数
    exchange = ccxt.binance()  # huobipro, binance, okex3，使用huobi需要增加limit=2000，XRP-USDT-200327
    # symbol = 'BTC/USDT'
    # time_interval = '1h'
    # 其他可以尝试的值：'1m', '5m', '15m', '30m', '1h', '2h', '1d', '1w', '1M', '1y'


    # =====抓取数据开始结束时间
    # start_time = pd.to_datetime('2021-02-01 00:00:00')
    while start_time <= end_time:

        end_time_loop = start_time + timedelta(days=1)


        # =====开始循环抓取数据
        df_list = []
        start_time_parse8601 = exchange.parse8601(str(start_time))

        while True:

            # 获取数据

            # BTC-USDT -> BTCUSDT
            _symbol = symbol.split('-')[0] + symbol.split('-')[1]
            df = get_klines(_symbol, spot_or_swap, time_interval, start_time_parse8601)

            # 整理数据
            df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
            # df['candle_begin_time'] = pd.to_datetime(df[0], unit='ms')  # 整理时间
            # print(df)

            # 合并数据
            df_list.append(df)

            # 新的since
            t = pd.to_datetime(df.iloc[-1][0], unit='ms')
            # print(t)
            start_time_parse8601 = exchange.parse8601(str(t))

            # 判断是否挑出循环
            if t >= end_time_loop or df.shape[0] <= 1:
                # print('抓取完所需数据，或抓取至最新数据，完成抓取任务，退出循环')
                break

            # 抓取间隔需要暂停2s，防止抓取过于频繁
            time.sleep(2)


        # =====合并整理数据
        df = pd.concat(df_list, ignore_index=True)

        df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                           3: 'low', 4: 'close', 5: 'volume',
                           6: 'quote_volume', 7: 'trade_num',
                           8: 'taker_buy_base_asset_volume',
                           9: 'taker_buy_quote_asset_volume'}, inplace=True)  # 重命名
        df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
        df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume',
                 'quote_volume', 'trade_num',
                 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']]

        # 选取数据时间段
        df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]

        # 去重、排序
        df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
        df.sort_values('candle_begin_time', inplace=True)
        df.reset_index(drop=True, inplace=True)


        # =====保存数据到文件
        if df.shape[0] > 0:
            # 根目录，若不存在则创建
            path = root_path + '/data/raw_history_k_data'
            if not os.path.exists(path):
                os.makedirs(path)

            # 创建交易所文件夹
            path = os.path.join(path, exchange.id)
            if os.path.exists(path) is False:
                os.mkdir(path)
            # 创建spot文件夹
            path = os.path.join(path, spot_or_swap)
            if os.path.exists(path) is False:
                os.mkdir(path)
            # 创建日期文件夹
            path = os.path.join(path, str(pd.to_datetime(start_time).date()))
            if os.path.exists(path) is False:
                os.mkdir(path)

            # 拼接文件目录
            file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
            path = os.path.join(path, file_name)
            # print(path)

            df.to_csv(path, index=False)
            print(f'成功输出{symbol} {str(pd.to_datetime(start_time).date())} {time_interval} 数据至 {path}')

        start_time = start_time + timedelta(days=1)
        time.sleep(2)






#####################

# 需要获取的币对
# 'BTC-USDT', 'ETH-USDT'
symbol_list = ['BTC-USDT', 'ETH-USDT', 'DOGE-USDT']

# 获取数据开始日期 默认end_time为当前
start_time = '2021-01-01 00:00:00'

# 获取数据 且保存到本地
for symbol in symbol_list:
    for spot_or_swap in ['spot', 'swap']:
        get_history_k_lines(symbol=symbol, spot_or_swap=spot_or_swap,
                            time_interval='5m',
                            start_time=pd.to_datetime(start_time))
