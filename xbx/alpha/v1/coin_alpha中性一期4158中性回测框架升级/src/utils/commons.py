#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy  as np
from glob import glob
from joblib import Parallel, delayed

from config import data_path
from config import head_columns



def read_factors(trade_type, factor_list, hold_hour):
    factor_set = set()
    for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
        if d_num==0:
            factor_set.add(f'{factor_name}_bh_{back_hour}')
        else:
            factor_set.add(f'{factor_name}_bh_{back_hour}_diff_{d_num}')

    symbol_factors_dict = dict()
    for _path in glob(os.path.join(data_path, trade_type, 'coin_alpah_factor_*.pkl')):
        symbol = _path.split(os.sep)[-1].replace('.pkl', '').replace('coin_alpah_factor_', '')
        symbol_factors_dict[symbol] = _path

    # resample
    all_list = []
    for symbol, _path in symbol_factors_dict.items():
        # 读头文件
        df = pd.read_feather(_path)
        diff_factors = list(factor_set & (set(df.columns) - set(head_columns)))
        if len(diff_factors) != len(factor_set):
            print('有因子不存在文件中', symbol, factor_set - set(diff_factors))
            continue

        df = df[head_columns + diff_factors]

        df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
        df.reset_index(drop=True, inplace=True)

        _factors = list(factor_set)
        # ===将数据转化为需要的周期
        # 在数据最前面，增加一行数据，这是为了在对>24h的周期进行resample时，保持数据的一致性。
        df = df.loc[0:0, :].append(df, ignore_index=True)
        df.loc[0, 'candle_begin_time'] = pd.to_datetime('2010-01-01')

        # 转换周期
        df['周期开始时间'] = df['candle_begin_time']
        df = df[head_columns + ['周期开始时间', ] + _factors]
        df.set_index('candle_begin_time', inplace=True)
        # 必备字段
        agg_dict = {
            'symbol': 'first',
            '周期开始时间': 'first',
            'avg_price': 'first',
            '下个周期_avg_price': 'last',
            'volume': 'sum',
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

        all_list.append(period_df)    

    if len(all_list)==0:
        return

    all_df = pd.concat(all_list, ignore_index=True)
    if all_df.empty:
        return
        
    all_df.sort_values(by=['offset', 'candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)

    return all_df



def read_factors_async(trade_type, factor_list, hold_hour):
    factor_set = set()
    for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
        if d_num==0:
            factor_set.add(f'{factor_name}_bh_{back_hour}')
        else:
            factor_set.add(f'{factor_name}_bh_{back_hour}_diff_{d_num}')

    symbol_factors_dict = dict()
    for _path in glob(os.path.join(data_path, trade_type, 'coin_alpah_factor_*.pkl')):
        symbol = _path.split(os.sep)[-1].replace('.pkl', '').replace('coin_alpah_factor_', '')
        symbol_factors_dict[symbol] = _path

    all_list = Parallel(n_jobs=16)(
        delayed(read_factors_parallel)(
            symbol, _path, factor_set, hold_hour
        )
        for symbol, _path in symbol_factors_dict.items()
    )

    all_df = pd.concat(all_list, ignore_index=True)
    if all_df.empty:
        return

    all_df.sort_values(by=['offset', 'candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)

    return all_df


def read_factors_parallel(symbol, _path, factor_set, hold_hour):
    # 读文件
    df = pd.read_feather(_path)
    diff_factors = list(factor_set & (set(df.columns) - set(head_columns)))
    if len(diff_factors) != len(factor_set):
        print('有因子不存在文件中', symbol, factor_set - set(diff_factors))
        return pd.DataFrame()

    df = df[head_columns + diff_factors]

    df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    _factors = list(factor_set)
    # ===将数据转化为需要的周期
    # 在数据最前面，增加一行数据，这是为了在对>24h的周期进行resample时，保持数据的一致性。
    df = df.loc[0:0, :].append(df, ignore_index=True)
    df.loc[0, 'candle_begin_time'] = pd.to_datetime('2010-01-01')

    # 转换周期
    df['周期开始时间'] = df['candle_begin_time']
    df = df[head_columns + ['周期开始时间', ] + _factors]
    df.set_index('candle_begin_time', inplace=True)
    # 必备字段
    agg_dict = {
        'symbol': 'first',
        '周期开始时间': 'first',
        'avg_price': 'first',
        '下个周期_avg_price': 'last',
        'volume': 'sum',
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



def reduce_mem_usage(df, verbose=False):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64', 'object']

    start_mem = df.memory_usage().sum() / 1024**2    
    if verbose:
        print("Memory usage of the dataframe before converted is :", start_mem, "MB")

    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].dropna().min()
            c_max = df[col].dropna().max()

            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64) 

            elif str(col_type)[:5] == 'float':
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
            else:
                num_unique_values = len(df[col].unique())
                num_total_values = len(df[col])
                rate = num_unique_values/num_total_values
                #rate = df[col].value_counts(normalize=True, dropna=False).values[0]
                if rate <0.5:
                    df[col] = df[col].astype('category')

    end_mem = df.memory_usage().sum() / 1024**2
    if verbose:
        print("Memory usage of the dataframe after converted is :", end_mem, "MB")
    if verbose: 
        print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
    return df


    







