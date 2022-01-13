#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

from utils import ind

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 50)  		# 最多显示数据的行数
import warnings
warnings.filterwarnings("ignore")
from warnings import simplefilter


def target_rank(df, c_rate, select_coin_num):
    select_c = ind.gen_select_df(df, c_rate, select_coin_num)
    if select_c.empty:
        return 0
    return round(select_c['排名1差值'].mean(), 2)


def target_return(df, c_rate, select_coin_num):
    select_c = ind.gen_select_df(df, c_rate, select_coin_num)
    if select_c.empty:
        return 0
    return round(select_c['资金曲线'].iloc[-1], 2)


def target_ratio(df, c_rate, select_coin_num):
    select_c = ind.gen_select_df(df, c_rate, select_coin_num)
    if select_c.empty:
        return 0
    # ===计算最大回撤
    select_c['max2here'] = select_c['资金曲线'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    select_c['dd2here']  = select_c['资金曲线']/select_c['max2here'] - 1

    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(select_c.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])

    final_r  = round(select_c['资金曲线'].iloc[-1], 2)
    return final_r/(-max_draw_down)


def target_annual_ratio(df, c_rate, select_coin_num):
    select_c = ind.gen_select_df(df, c_rate, select_coin_num)
    if select_c.empty:
        return 0
    # ===计算最大回撤
    select_c['max2here'] = select_c['资金曲线'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    select_c['dd2here']  = select_c['资金曲线']/select_c['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(select_c.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
    # 计算最大回撤总时长
    time_during   = select_c.iloc[-1]['candle_begin_time'] - select_c.iloc[0]['candle_begin_time']
    total_seconds = time_during.days * 24 * 3600 + time_during.seconds

    final_r = round(select_c['资金曲线'].iloc[-1], 2)
    annual_return = pow(final_r, 24 * 3600 * 365 / total_seconds) - 1

    return annual_return/(-max_draw_down)



def sharpe_annual(net_value, periods=252):
    periods = int(periods)
    returns = net_value.pct_change(periods=1)
    returns_std = returns.std()
    if returns_std:
        sharpe = np.sqrt(periods) * (returns.mean() / returns_std)
    else:
        sharpe = 0
    return sharpe


def target_sharpe(df, c_rate, select_coin_num):
    select_c = f_original.gen_select_df(df, c_rate, select_coin_num)
    if select_c.empty:
        return 0

    dailly_return = select_c[['资金曲线']].resample(rule='D').apply(lambda x: (1 + x).prod() - 1)
    sharpe_ratio  = sharpe_annual(dailly_return)
    annual_sharpe_ratio = pow(sharpe_ratio, 365/len(dailly_return))

    return annual_sharpe_ratio





    


