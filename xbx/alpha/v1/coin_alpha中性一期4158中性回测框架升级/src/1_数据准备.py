#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from glob   import glob
from joblib import Parallel, delayed

pd_display_rows  = 10
pd_display_cols  = 100
pd_display_width = 1000
pd.set_option('display.max_rows', pd_display_rows)
pd.set_option('display.min_rows', pd_display_rows)
pd.set_option('display.max_columns', pd_display_cols)
pd.set_option('display.width', pd_display_width)
pd.set_option('display.max_colwidth', pd_display_width)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('expand_frame_repr', False)
os.environ['NUMEXPR_MAX_THREADS'] = "256"
start_time = time.time()

from config import root_path, pickle_path
from config import njobs, trade_type_list


# 增量数据的日期范围
step = False # True: 计算增量   False: 计算全量
start_datetime = pd.to_datetime('2020-05-01')

def transfer_bn_raw_data_2_pkl_data(_trade_type='spot', _time_interval='5m', _njobs=16):
    """
    :param _trade_type: 回测数据类型 spot现货 swap期货
    :param _time_interval: 需要读取的K线周期 '5m' 或者 '1m'
    :param _njobs: 并行读取 线程数
    :return: """
    if _trade_type not in ['spot', 'swap']:
        raise ValueError('输入的交易类型只能是 spot 或 swap')
    if _time_interval not in ['1m', '5m']:
        raise ValueError('转换的K线周期只能是 1m 或 5m')

    # 币安现货或期货K线数据存储的路径
    if _time_interval not in ('5m', '1m'):
        raise Error('获取K线时间只能为1m, 5m!!!')
    if '1m' == _time_interval:
        k_folder = os.path.join(root_path, 'data', 'binance', f'{_trade_type}_1m')
    else:
        k_folder = os.path.join(root_path, 'data', 'binance', f'{_trade_type}')

    csv_paths = glob(os.path.join(k_folder, '*', '*.csv')) # 获取子目录下的所有csv全局路径
    csv_paths = list(filter(lambda x: f'_{_time_interval}.csv' in x, csv_paths))  # 选择属于该周期的路径

    symbols = list(set(map(lambda x: x.split(os.sep)[-1].replace('.csv', '').strip(), csv_paths)))  # 获取所有币种名称
    symbols.sort()

    _results = dict()
    for symbol in symbols:
        print(_trade_type, symbol)

        symbol_all_date_paths = list(set(filter(lambda x: symbol in x, csv_paths)))  # 选择属于该币种的路径
        if step:
            # ===日期过滤
            symbol_paths = []
            for _path in symbol_all_date_paths:
                curr_date = pd.to_datetime(_path.split(os.sep)[-2])
                if curr_date >= start_datetime:
                    symbol_paths.append(_path)

            if len(symbol_paths)==0:
                continue
        else:
            symbol_paths = symbol_all_date_paths

        symbol_paths.sort()

        # 并行读取 该币种所有对应周期的K线数据 并合并为一个 pd.DataFrame
        data = pd.concat(Parallel(n_jobs=_njobs)(
            delayed(pd.read_csv)(symbol_path, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
            for symbol_path in symbol_paths), ignore_index=True)

        data['symbol']    = symbol.split('_')[0].lower()  # 增加 symbol
        data['avg_price'] = data['quote_volume'] / data['volume']  # 增加 均价

        data.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')  # 去除重复值
        data.sort_values(by='candle_begin_time', inplace=False)  # 排序
        data.reset_index(drop=True, inplace=True)  # 重置index

        # 按数据类型存储数据到对应文件夹
        data.set_index('candle_begin_time', inplace=True)
        for _id, _df in data.groupby(pd.Grouper(freq='M')):
            # 目录不存在则创建 目录结构 data/pickle_data/spot/2020-01-01/*.pkl
            year  = str(_id.year)
            month = str(('0%d' % _id.month) if _id.month < 10 else _id.month)
            _path = os.path.join(pickle_path, _trade_type, f'{year}-{month}')
            if not os.path.exists(_path):
                os.makedirs(_path)

            _df = _df.copy()
            _df['candle_begin_time'] = _df.index
            _df.reset_index(drop=True, inplace=True)
            _df.to_feather(os.path.join(_path, f'{symbol}.pkl'))


def deal_one_pkl(_pkl_path):
    print('deal', _pkl_path)

    df: pd.DataFrame = pd.read_feather(_pkl_path)
    # =将数据转换为1小时周期
    df.set_index('candle_begin_time', inplace=True)

    df['avg_price_5m'] = df['avg_price']
    agg_dict = {
        'symbol': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',                  # 成交额
        'trade_num': 'sum',                     # 成交笔数
        'taker_buy_base_asset_volume': 'sum',   # 主动买入成交量
        'taker_buy_quote_asset_volume': 'sum',  # 主动买入成交额
        'avg_price_5m': 'first'
    }
    df = df.resample(rule='1H').agg(agg_dict)

    # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
    df['symbol'].fillna(method='ffill', inplace=True)
    # 对开、高、收、低、价格进行补全处理
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(value=df['close'], inplace=True)
    df['high'].fillna(value=df['close'], inplace=True)
    df['low'].fillna(value=df['close'], inplace=True)
    # 将停盘时间的某些列，数据填补为0
    fill_0_list = ['volume', 'quote_volume', 'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

    # =补全1分钟数据中的均价
    path_1m = _pkl_path.replace('_5m.pkl', '_1m.pkl')
    if os.path.isfile(path_1m):  # 如果文件存在
        df_1m = pd.read_feather(path_1m)
        df_1m.set_index('candle_begin_time', inplace=True)
        df['avg_price_1m'] = df_1m['avg_price']

    # =计算最终的均价
    df['avg_price'] = df['avg_price_1m']  # 默认使用1分钟均价
    df['avg_price'].fillna(value=df['avg_price_5m'], inplace=True)  # 没有1分钟均价就使用5分钟均价
    df['avg_price'].fillna(value=df['open'], inplace=True)  # 没有5分钟均价就使用开盘价
    del df['avg_price_5m']
    del df['avg_price_1m']

    # =输出数据
    df.reset_index(inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
    df.reset_index(inplace=True, drop=True)

    return df


for _ in trade_type_list:  # 现货，期货都进行数据转换
    # ===创建目录
    _pickle_path = os.path.join(pickle_path, _)
    if not os.path.exists(_pickle_path):
        os.mkdir(_pickle_path) 

    # ===整理出所有币种数据
    transfer_bn_raw_data_2_pkl_data(_trade_type=_, _time_interval='5m', _njobs=njobs)  # 5分钟
    transfer_bn_raw_data_2_pkl_data(_trade_type=_, _time_interval='1m', _njobs=njobs)  # 1分钟

    # ===遍历所有币种5分钟数据，将数据转换为1小时周期
    # 将每个币种的时间序列数据，合并成面板数据
    _path_list = []
    for _path in glob(os.path.join(_pickle_path, '*', '*_5m.pkl')):
        if step:
            curr_date = pd.to_datetime(_path.split(os.sep)[-2])
            if curr_date >= start_datetime:
                _path_list.append(_path)
        else:
            _path_list.append(_path)

    df_list = Parallel(n_jobs=njobs)(
        delayed(deal_one_pkl)(_path) 
        for _path in _path_list
    )
    panel_df = pd.concat(df_list, ignore_index=True)

    # 按照时间和成交量排序。为什么要按照成交量排序？
    panel_df.sort_values(by=['candle_begin_time', 'volume'], ascending=[True, False], inplace=True)
    panel_df.reset_index(drop=True, inplace=True)

    # ===将面板数据，拆分成每个币种的时间序列数据
    for g_id, g_df in panel_df.groupby('symbol'):
        g_df.reset_index(drop=True, inplace=True)
        g_df.set_index('candle_begin_time', inplace=True)
        for _id, _df in g_df.groupby(pd.Grouper(freq='M')):
            # 目录
            year  = str(_id.year)
            month = str(('0%d' % _id.month) if _id.month < 10 else _id.month)
            _path = os.path.join(_pickle_path, f'{year}-{month}')
            # 保存文件
            _df = _df.copy()
            _df['candle_begin_time'] = _df.index
            _df.reset_index(drop=True, inplace=True)
            _df.to_feather(os.path.join(_path, f'{g_id.upper()}.pkl'))

print('读取并转换数据完成，用时', time.time() - start_time)





