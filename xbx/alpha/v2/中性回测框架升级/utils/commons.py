#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy  as np
from glob import glob
from joblib import Parallel, delayed

from config import data_path
from config import head_columns



def read_factors_async(trade_type, factor_list, hold_hour, filter_factor=None, shift=1, symbol_filters=[]):
    factor_set = set()
    for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
        if d_num==0:
            factor_set.add(f'{factor_name}_bh_{back_hour}')
        else:
            factor_set.add(f'{factor_name}_bh_{back_hour}_diff_{d_num}')

    symbol_factors_dict = dict()
    for _path in glob(os.path.join(data_path, trade_type, '*', 'coin_alpah_factor_*.pkl')):
        symbol = _path.split(os.sep)[-2]
        # 过滤
        if symbol in symbol_filters:
            continue

        #if symbol not in ('ZRX-USDT', ):
        #    continue

        _factor_name = _path.split(os.sep)[-1].replace('.pkl', '').replace('coin_alpah_factor_', '')
        for factor_name in factor_set:
            if _factor_name in factor_name:
                if symbol not in symbol_factors_dict.keys():
                    symbol_factors_dict[symbol] = list()
                symbol_factors_dict[symbol].append(_path)

    all_list = Parallel(n_jobs=16)(
        delayed(read_factors0)(
            trade_type, symbol, path_list, factor_set, hold_hour, filter_factor, shift=shift
        )
        for symbol, path_list in symbol_factors_dict.items()
    )

    all_df = pd.concat(all_list, ignore_index=True)
    if all_df.empty:
        return

    all_df.sort_values(by=['offset', 'candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)

    return all_df


def read_factors0(trade_type, symbol, path_list, factor_set, hold_hour, filter_factor, shift=1):
    # 读头文件
    _head_path = os.path.join(data_path, trade_type, symbol, 'coin_alpah_head.pkl')
    df = pd.read_feather(_head_path)
    # 读过滤因子文件
    if filter_factor:
        _filter = filter_factor.split('_filter_')[0]
        _filter_path = os.path.join(data_path, trade_type, symbol, f'coin_alpah_filter_{_filter}.pkl')
        _df = pd.read_feather(_filter_path)
        if shift > 0:
            df['filter'] = _df[filter_factor].shift(1)
        else:
            df['filter'] = _df[filter_factor]
    else:
        df['filter'] = 0

    # 读因子文件
    for _path in path_list:
        _df = pd.read_feather(_path)
        factor_columns = list(set(_df.columns) - set(head_columns))
        for f in factor_columns:
            if shift > 0:
                df[f] = _df[f].shift(1)
            else:
                df[f] = _df[f]

    df.sort_values(by=['candle_begin_time', ], inplace=True)
    df.reset_index(drop=True, inplace=True)

    _factors = list(factor_set)
    # ===将数据转化为需要的周期
    # 在数据最前面，增加一行数据，这是为了在对>24h的周期进行resample时，保持数据的一致性。
    df = df.loc[0:0, :].append(df, ignore_index=True)
    df.loc[0, 'candle_begin_time'] = pd.to_datetime('2010-01-01')

    # 转换周期
    df['周期开始时间'] = df['candle_begin_time']
    df = df[head_columns + ['周期开始时间', 'filter', ] + _factors]
    df.set_index('candle_begin_time', inplace=True)
    # 必备字段
    agg_dict = {
        'symbol': 'first',
        '周期开始时间': 'first',
        'avg_price': 'first',
        '下个周期_avg_price': 'last',
        'volume': 'sum',
        'filter': 'first',
    }
    for f in _factors:
        agg_dict[f] = 'first' 

    period_df_list = []
    for offset in range(int(hold_hour[:-1])):
        period_df = df.resample(hold_hour, base=offset).agg(agg_dict)
        del period_df['周期开始时间']

        period_df['offset'] = offset
        period_df.reset_index(inplace=True)

        # 合并数据
        period_df_list.append(period_df)

    # 将不同offset的数据，合并到一张表
    period_df = pd.concat(period_df_list, ignore_index=True)

    # 删除一些数据   
    period_df = period_df.iloc[24:]  # 刚开始交易前24个周期删除
    period_df = period_df[period_df['candle_begin_time'] >= pd.to_datetime('2018-01-01')]  # 删除2018年之前的数据
    period_df.reset_index(drop=True, inplace=True)

    return period_df
    







