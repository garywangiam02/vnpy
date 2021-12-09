"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
12 事件策略 案例：中户资金流
"""
import pandas as pd


def cal_rank_factor(df: pd.DataFrame, extra_cols: list):
    df['rank_factor_market_value'] = df['总市值']
    df['rank_factor_market_value'] = df['rank_factor_market_value'].shift()
    extra_cols.append('rank_factor_market_value')

    df['中户资金买入额'] *= 10000
    df['rank_factor_money_flow'] = df['中户资金买入额'] / df['成交额']
    df['rank_factor_money_flow'] = df['rank_factor_money_flow'].shift()
    extra_cols.append('rank_factor_money_flow')

    return df, extra_cols
