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
from config import pickle_path, data_path
from config import head_columns, delete_columns

from factors import class_list as class_list1


def prepare_one_hold(_root_path, symbol, pkl_file_list):
    symbol = os.path.split(pkl_file_list[0])[1].replace('.pkl', '')
    print('    ', symbol)

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
        for cls_name in class_list1:
            _cls = __import__('factors.%s' % cls_name,  fromlist=('', ))
            for n in back_hour_list:
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


def run(trade_type_list):
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
            symbol = os.path.split(_path)[1].replace('.pkl', '')
            #if symbol not in ('BTC-USDT'):
            #    continue
            if symbol not in _path_merge.keys():
                _path_merge[symbol] = list()
            _path_merge[symbol].append(_path)

        #for symbol, pkl_file_list in _path_merge.items():
        #    prepare_one_hold(_root_path, symbol, pkl_file_list)

        results = Parallel(n_jobs=njobs)(
            delayed(prepare_one_hold)(
                _root_path, symbol, pkl_file_list
            )
            for symbol, pkl_file_list in _path_merge.items()
        )
        
        print('')
        for df, symbol, factor_list in results:
            _f_path = os.path.join(_root_path, f'coin_alpah_factor_{symbol}.pkl')
            if os.path.exists(_f_path):
                old_df = pd.read_feather(_f_path)
                for factor in factor_list:
                    old_df[factor] = df[factor]
                old_df.to_feather(_f_path) 
            else:
                df.to_feather(_f_path) 
            print(f'保存数据    coin_alpah_factor_{symbol}.pkl')

        print(f'trade_type --- {trade_type}完成!')


if __name__ == '__main__':
    # ===输出日志
    print('\n')
    print(class_list1)
    print('')
    # ===计算因子
    start_time = time.time()
    run(trade_type_list)
    print('添加数据列并合并完成，用时', time.time() - start_time)  # 807
    exit()

    df = pd.read_feather(
        os.path.join(data_path, 'spot', 'coin_alpah_factor_BTC-USDT.pkl')
    )
    print(df.columns)












