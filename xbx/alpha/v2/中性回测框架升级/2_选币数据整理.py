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
from config import trade_type_list, back_hour_list, diff_list
from config import pickle_path, data_path
from config import head_columns

from factors import class_list


def cal_one_factor(df, factor_name):
    #print('        ', factor_name)
    df = df.copy()
    """ ******************** 以下是需要修改的代码 ******************** """
    factor_list = []
    # =====技术指标
    _cls = __import__('factors.%s' % factor_name,  fromlist=('', ))
    for n in back_hour_list:
        for d_num in diff_list:
            if d_num > 0:
                _factor_name = f'{factor_name}_bh_{n}_diff_{d_num}'
            else:
                _factor_name = f'{factor_name}_bh_{n}'
            factor_list.append(_factor_name)  
            # 计算因子
            getattr(_cls, 'signal')(df, n, d_num, _factor_name)
            # 为了跟实盘保持一致 
            #df[_factor_name] = df[_factor_name].shift(1)
    """ ************************************************************ """

    factor_list.sort()
    #df = df[head_columns + factor_list].copy()
    df.sort_values(by=['candle_begin_time', ], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df, factor_name


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
            if symbol not in _path_merge.keys():
                _path_merge[symbol] = list()
            _path_merge[symbol].append(_path)

        for symbol, pkl_file_list in _path_merge.items():
            #if symbol not in ('XRP-USDT', ):
            #    continue

            # 读取数据
            df_list = []
            for _pkl_file in pkl_file_list:
                df_list.append(pd.read_feather(_pkl_file))  
            df = pd.concat(df_list, ignore_index=True)
            df.sort_values(by=['candle_begin_time', ], inplace=True)
            df['下个周期_avg_price'] = df['avg_price'].shift(-1)  # 计算下根K线开盘买入涨跌幅

            print('    ', symbol)
            results = Parallel(n_jobs=njobs)(
                delayed(cal_one_factor)(
                    df, cls_name
                )
                for cls_name in class_list
            )

            for df, factor_name in results:
                _symbol_path = os.path.join(_root_path, symbol)
                if not os.path.exists(_symbol_path):
                    os.makedirs(_symbol_path)
                # 保存文件头
                _head_path = os.path.join(_symbol_path, f'coin_alpah_head.pkl')
                if not os.path.exists(_head_path):
                    df_head = df[head_columns]
                    df_head.to_feather(_head_path) 
                # 保存因子
                _factor_path = os.path.join(_symbol_path, f'coin_alpah_factor_{factor_name}.pkl')
                df_factors = df[list(set(df.columns) - set(head_columns))]
                df_factors.to_feather(_factor_path) 

        print(f'trade_type --- {trade_type}完成!')


if __name__ == '__main__':
    # ===输出日志
    print('\n')
    print(class_list)
    print('')
    # ===计算因子
    start_time = time.time()
    run(trade_type_list)
    print('添加数据列并合并完成，用时', time.time() - start_time)  # 807












