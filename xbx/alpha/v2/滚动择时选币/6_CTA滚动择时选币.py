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
# ic(sys.path)
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
n_jobs = 40 # 并行进程数
factor_threshold = 2 # 目标函数阈值
symbol_type = 'swap'

hdf_file_list = glob.glob(base_output_dir_SSD + f'/output/base_data/{symbol_type}/*USDT_1h.pkl') # 改成自己数据目录

def cal_val(symbol_list, factors_list, start_time='2020-01-01', end_time='2021-08-01', pre_time=30):

    annual_return_list = []
    max_draw_down_list = []
    sharpe_list = []
    for symbol in symbol_list:
        file_name = base_output_dir_SSD + f"/output/base_data/{symbol_type}/{symbol.split('-')[0].upper()}-USDT_1h.pkl"
        df = pd.read_pickle(file_name) # 读取单品种数据
        temp = df[df['candle_begin_time'] >= pd.to_datetime(start_time)]
        if len(temp) < 100:
            continue
        # 截取时间
        df = df[df['candle_begin_time'] >= (pd.to_datetime(start_time) - pd.Timedelta(days=pre_time))]  # 现货的时间的时间
        df = df[df['candle_begin_time'] <= pd.to_datetime(end_time)]
        factor_name, back_hour = factors_list
        annual_return, max_draw_down, sharpe = eval(f'signal_{factor_name}')(df, back_hour=back_hour,start_time=start_time)
        annual_return_list.append(annual_return)
        max_draw_down_list.append(max_draw_down)
        sharpe_list.append(sharpe)

    return np.mean(annual_return_list), np.mean(max_draw_down_list), np.mean(sharpe_list)

# ===单次循环
def calculate_by_one_strategy(symbol_path, factors_list, start_time='2020-01-01', end_time='2021-08-01', pre_time=30):
    # 计算对应的因子值
    # 读取单品种
    df = pd.read_pickle(symbol_path) # 读取单品种数据
    symbol = df['symbol'].iloc[0]
    if pd.to_datetime(df['candle_begin_time'].iloc[0]) > pd.to_datetime(start_time): # 若品种不在该时间段过滤
        return pd.DataFrame()
    # 截取时间
    df = df[df['candle_begin_time'] >= (pd.to_datetime(start_time) - pd.Timedelta(days=pre_time))]  # 需要添加一定的预热数据
    df = df[df['candle_begin_time'] <= pd.to_datetime(end_time)]
    if len(df) < 1000: # 过滤数据太少的数据
        return pd.DataFrame()
    factor_name, back_hour = factors_list
    # 计算CTA
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

    date_type = 'month' # day 或者month  表示以day天数平移或者月份进行平移
    if date_type == 'day':
        train_test_split = [(90, 30), (60, 30), (120, 30), (180, 30), (180, 60), (300, 90)]   # （训练集时间天数，验证集时间天数）
    else:
        # train_test_split = [(6, 1), (6, 2), (9, 3), (12, 3), (3, 1), (9,2)]   # （训练集时间月份数，验证集时间天数）
        train_test_split = [(2, 1)]   # （训练集时间月份数，验证集时间天数）
    start_date = '2020-06-01'
    end_date = '2021-08-01'
    signal_name = 'adaptboll_with_mtm_v3' # cta名称
    para = 11
    pre_time = 30 # 预热数据
    factors_list = (signal_name, para)
    for train_date, test_date in train_test_split:
        # train_date, 训练集时间切分  test_date 验证集时间切分
        ic('开始遍历该策略参数：', train_date, test_date)

        if date_type == 'day':
            date_list = list(pd.date_range(start=start_date, end=end_date, freq='d'))  # 获取时间列表
            len_date_list = len(date_list)
            if len(len_date_list) % test_date == 0:
                date_len = int(len(len_date_list) / test_date)
            else:
                date_len = int(len(len_date_list) / test_date) + 1
        else:
            date_list = list(pd.date_range(start=start_date, end=end_date, freq='M'))  # 以月数为时间列表
            len_date_list = len(date_list) + 1
            if len_date_list % test_date == 0:
                date_len = int(len_date_list / test_date)
            else:
                date_len = int(len_date_list / test_date) + 1

        ic(f"时间切分份数为:{date_len}")
        backtest_df = pd.DataFrame()
        all_test_df_list = []
        for i in range(date_len):
            if date_type == 'day':
                train_start_time = pd.to_datetime(start_date) + pd.Timedelta(days=test_date * i)
                train_end_time = pd.to_datetime(train_start_time) + pd.Timedelta(
                    days=train_date)  # 训练集时间周期
                test_end_time = pd.to_datetime(train_end_time) + pd.Timedelta(days=test_date)  # 验证集时间周期
            else:
                train_start_time = pd.to_datetime(start_date) + relativedelta(
                    months=test_date * i)  # 以月为单位

                train_end_time = pd.to_datetime(train_start_time) + relativedelta(
                    months=train_date)  # 训练集时间周期  以月为单位

                test_end_time = pd.to_datetime(train_end_time) + relativedelta(
                    months=test_date)  # 验证集时间周期   以月为单位
            if pd.to_datetime(train_end_time) > pd.to_datetime(end_date):
                ic(f"当前训练集数据超出最大数据时间,进行跳过,当前训练集结束时间为:{train_end_time}，数据最后时间为:{end_date}")
                continue

            # 获取最佳品种
            all_df = main(factors_list, train_start_time, train_end_time, pre_time=pre_time)

            all_df = all_df[all_df['因子'] > factor_threshold]  # 过滤因子值太低的
            # # 排序 从大到小排名
            all_df['排名'] = all_df.groupby('start_time')['因子'].rank(ascending=False, method='first')
            all_df = all_df[all_df['排名'] <= select_coin_num].copy()

            # 获取选中的symbol
            select_symbol_list = all_df['symbol'].values.tolist()

            # 放入验证集进行测试
            annual_return, max_draw_down, sharpe = cal_val(select_symbol_list, factors_list=factors_list, start_time=train_end_time, end_time=test_end_time, pre_time=pre_time)

            # 设置滚动数据文件
            l = len(backtest_df)
            backtest_df.loc[l, 'train_start_time'] = train_start_time
            backtest_df.loc[l, 'train_end_time'] = train_end_time
            backtest_df.loc[l, 'test_start_time'] = train_end_time
            backtest_df.loc[l, 'test_end_time'] = test_end_time
            backtest_df.loc[l, 'factor'] = str(factors_list)
            backtest_df.loc[l, 'symbol_list'] = str(select_symbol_list)
            backtest_df.loc[l, '验证集年化回撤比'] = sharpe
            backtest_df.loc[l, '验证集年化收益'] = annual_return
            backtest_df.loc[l, '验证集最大大回撤'] = max_draw_down

        save_dir = root_path + f'/data/output/滚动回测CTA择时_{symbol_type}/{signal_name}/'
        if os.path.exists(save_dir) is False:
            os.makedirs(save_dir)

        save_file = save_dir + f'{signal_name}_{para}_{symbol_type}_{date_type}_{train_date}_{test_date}_{start_date}_{end_date}.csv'
        backtest_df.to_csv(save_file, index=0)