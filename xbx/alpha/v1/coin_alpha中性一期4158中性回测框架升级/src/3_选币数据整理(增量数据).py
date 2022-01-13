#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
from glob import glob
from datetime import datetime, timedelta
from joblib import Parallel, delayed
from utils  import diff

pd_display_rows = 20
pd_display_cols = 8
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

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

from config import njobs
from config import trade_type_list, back_hour_list, diff_d
from config import pickle_path, data_path, output_path
from config import head_columns, delete_columns

from factors import class_list as class_list1


def read_factors_by_symbol(trade_type, symbol, factor_list):
    _factor_dict = dict()
    for _path in glob(os.path.join(data_path, trade_type, f'coin_alpah_factor_{symbol}|*.pkl')):
        arr = _path.split(os.sep)[-1].replace('.pkl', '').replace('coin_alpah_factor_', '').split('|')
        _symbol = arr[0]
        _factor = arr[1]   
        
        if _symbol!=symbol:
            continue

        if _factor not in factor_list:
            continue

        _factor_dict[_factor] = _path

    # 读头文件
    df = pd.read_feather(
        os.path.join(data_path, trade_type, f'coin_alpah_head_{symbol}.pkl')
    )
    for factor, _path in _factor_dict.items():
        df[factor] = pd.read_feather(_path)[factor]
    df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def prepare_one_hold(trade_type, symbol, pkl_file_list):
    print('    ', symbol)
    if pkl_file_list is None or len(pkl_file_list)==0:
        return
    _root_path = os.path.join(data_path, trade_type)

    # 读取数据
    df_list = []
    for _pkl_file in pkl_file_list:
        df_list.append(pd.read_feather(_pkl_file))  
    df = pd.concat(df_list, ignore_index=True)
    df.sort_values(by=['candle_begin_time', ], inplace=True)
 
    df['下个周期_avg_price'] = df['avg_price'].shift(-1)  # 计算下根K线开盘买入涨跌幅
    #df.loc[df['volume'] == 0, '是否交易'] = 0  # 找出不交易的周期
    #df['是否交易'].fillna(value=1, inplace=True)
    """ ******************** 以下是需要修改的代码 ******************** """
    factor_list = []
    # =====技术指标
    if class_list1 is not None and len(class_list1) > 0:
        for n in back_hour_list:
            for cls_name in class_list1:
                _cls = __import__('factors.%s' % cls_name,  fromlist=('', ))
                factors = getattr(_cls, 'signal')(df, n)
                factor_list.extend(factors)
                if diff_d is not None or len(diff_d) > 0:
                    for _name in factors:
                        factor_list.extend(diff.add_diff(df, diff_d, _name))

    """ ************************************************************ """

    factor_list.sort()
    df = df[head_columns + factor_list]
    df.sort_values(by=['candle_begin_time', ], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df, symbol, factor_list



def run(trade_type_list, start_datetime):
    for trade_type in trade_type_list:
        print('\n')
        print(f'trade_type --- {trade_type}')
        # ===创建因子存储目录
        _root_path = os.path.join(data_path, trade_type)
        if not os.path.exists(_root_path):
            os.makedirs(_root_path)

        # ===将不同的币种数据合并到一张表，并且存储
        _path_merge = dict() 
        for _path in glob(os.path.join(pickle_path, trade_type, '*', '*-USDT.pkl')):
            curr_date = pd.to_datetime(_path.split(os.sep)[-2])
            if curr_date < start_datetime:
                continue

            symbol = os.path.split(_path)[1].replace('.pkl', '')
            if symbol not in _path_merge.keys():
                _path_merge[symbol] = list()
            _path_merge[symbol].append(_path)

        results = Parallel(n_jobs=njobs)(
            delayed(prepare_one_hold)(
                trade_type, symbol, pkl_file_list
            )
            for symbol, pkl_file_list in _path_merge.items()
        )

        #===整合
        # 创建临时目录
        _temp_factor_path = os.path.join(output_path, 'temp', trade_type)
        if not os.path.exists(_temp_factor_path):
            os.makedirs(_temp_factor_path)

        # =处理数据
        # 先保存增量数据到临时目录
        for df, symbol, factor_list in results:
            _f_path = os.path.join(_temp_factor_path, f'coin_alpah_factor_{symbol}.pkl')
            if os.path.exists(_f_path):
                old_df = pd.read_feather(_f_path)
                for factor in factor_list:
                    old_df[factor] = df[factor]
                old_df.to_feather(_f_path) 
            else:
                df.to_feather(_f_path) 

        print(f'trade_type --- {trade_type}完成!')

# 合并
def merge(trade_type_list):
    for trade_type in trade_type_list:
        _temp_factor_path = os.path.join(output_path, 'temp', trade_type)
        for _path in glob(os.path.join(_temp_factor_path, 'coin_alpah_factor_*.pkl')):
            symbol = _path.split(os.sep)[-1].replace('.pkl', '').replace('coin_alpah_factor_', '')
            # 增量数据
            new_df = pd.read_feather(_path)
            # 原始数据
            old_df = pd.read_feather(
                os.path.join(data_path, trade_type, f'coin_alpah_factor_{symbol}.pkl')
            )

            # 为了保证不出现增量空因子, 这里多了一步检查
            new_factor_columns   = list(set(new_df.columns) - set(head_columns))
            old_factor_columns   = list(set(old_df.columns) - set(head_columns))
            diff_factor_clolumns = list(set(old_factor_columns) ^ set(new_factor_columns))
            
            if len(diff_factor_clolumns)>0:
                print(symbol, diff_factor_clolumns)
                continue

            # 开始合并
            df = pd.concat([old_df, new_df], ignore_index=True)
            # 去重
            df.drop_duplicates(subset=['candle_begin_time', 'symbol'], keep='first', inplace=True)
            df.dropna(axis=0, subset=["symbol"], inplace=True) # 去掉空行
            df.sort_values(by=['candle_begin_time', ], inplace=True)
            df.reset_index(drop=True, inplace=True)   

            df.to_feather(
                os.path.join(data_path, trade_type, f'coin_alpah_factor_{symbol}.pkl')
            )


if __name__ == '__main__':
    start_datetime = pd.to_datetime('2020-05-01')
    # 向前推进一个月, 预防因子空值
    start_datetime = start_datetime - timedelta(days=30)

    # ===输出日志
    print('\n')
    print(class_list1)
    print('')
    # ===计算因子
    start_time = time.time()
    run(trade_type_list, start_datetime)
    # 合并数据
    merge(trade_type_list)
    print('添加数据列并合并完成，用时', time.time() - start_time)  # 807















