#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/1/19 11:21
# @Author  : weixx

# 中性策略框架
import pandas as pd
import glob
import os
import sys
from pathlib import Path
sys.path.append(os.getcwd())

import matplotlib.pyplot as plt
import numpy as np

from program.backtest.Function import *
import multiprocessing as mp
from multiprocessing import Pool as ProcessPool
from multiprocessing.dummy import Pool as ThreadPool
import random
import warnings
from dateutil.relativedelta import relativedelta
from icecream import ic
import time
from program.backtest.Signal_fun.Signal_cta import *
from multiprocessing import Pool, freeze_support, cpu_count
from functools import partial
warnings.filterwarnings("ignore")
from warnings import simplefilter
# ic.disable() # 禁用输出
def Timestamp():
    return '%s |> ' % time.strftime("%Y-%m-%d %T")

# 定制输出格式
ic.configureOutput(prefix=Timestamp)

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# ===参数设定
select_coin_num = 50  # 选币数量
c_rate = 6 / 10000  # 手续费
leverage_rate = 1
slippage = 10 / 10000
n_jobs = 40 # 并行进程数
factor_threshold = 2 # 目标函数阈值
symbol_type = 'swap'
factor_type = 'factor_norm'
hdf_file_list = glob.glob(base_output_dir_SSD + f'/output/base_data/{symbol_type}/*USDT_1h.pkl') # 改成自己数据目录

# ===单次循环
def calculate_by_one_strategy(symbol_path, factors_list, start_time='2020-01-01', end_time='2021-08-01', pre_time=30):
    # 计算对应的因子值
    # 读取单品种
    df = pd.read_pickle(symbol_path) # 读取单品种数据
    symbol = df['symbol'].iloc[0]
    if pd.to_datetime(df['candle_begin_time'].iloc[0]) > pd.to_datetime(start_time):
        return pd.DataFrame()
    # 截取时间
    df = df[df['candle_begin_time'] >= (pd.to_datetime(start_time) - pd.Timedelta(days=pre_time))]  # 现货的时间的时间
    df = df[df['candle_begin_time'] <= pd.to_datetime(end_time)]
    if len(df) < 1000:
        return pd.DataFrame()
    factor_name, back_hour = factors_list
    # for factor_name, back_hour in factors_list:
    annual_return, max_draw_down, sharpe = eval(f'signal_{factor_name}')(df, back_hour=back_hour, start_time=start_time)

    rtn = pd.DataFrame()
    rtn.loc[0, 'symbol'] = symbol
    rtn.loc[0, '因子'] = sharpe
    rtn.loc[0, 'factor_name'] = factor_name
    rtn.loc[0, 'back_hour'] = back_hour
    rtn.loc[0, 'start_time'] = start_time
    rtn.loc[0, 'end_time'] = end_time

    return rtn

def main(factors, start_time, end_time, pre_time=30):
    """
    根据时间挑选最佳的CTA 品种
    """
    f = partial(calculate_by_one_strategy, factors_list=factors, start_time=start_time, end_time=end_time, pre_time=pre_time)
    with Pool(processes=n_jobs) as pl:
        all_result = pl.map(f, hdf_file_list)
    # 进行拼接
    all_df = pd.concat(all_result, axis=0)
    all_df.reset_index(inplace=True, drop=True)
    return all_df

if __name__ == '__main__':

    start_date = '2021-06-01'
    end_date = '2021-08-01'
    signal_name = 'adaptboll_with_mtm_v3'
    para = 11
    factors_list = (signal_name, para)
    pre_time = 30
    # 获取最佳品种
    all_df = main(factors_list, start_date, end_date, pre_time)
    all_df = all_df[all_df['因子'] > factor_threshold] # 过滤因子值太低的
    # # 排序 从大到小排名
    all_df['排名'] = all_df.groupby('start_time')['因子'].rank(ascending=False, method='first')
    all_df = all_df[all_df['排名'] <= select_coin_num].copy()

    save_dir = root_path + f'/data/output/滚动回测CTA择时结果_{symbol_type}/{signal_name}/'
    if os.path.exists(save_dir) is False:
        os.makedirs(save_dir)

    save_file = save_dir + f'{signal_name}_{para}_{symbol_type}_{start_date}_{end_date}.csv'
    all_df.to_csv(save_file, index=0)
