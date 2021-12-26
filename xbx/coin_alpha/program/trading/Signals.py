"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import pandas as pd
import numpy as np


# 振幅策略
def signal_zhenfu(df, n=9):
    """
    振幅策略：选择近期振幅低的股票
    """

    high = df['high'].rolling(n, min_periods=1).max()
    low = df['low'].rolling(n, min_periods=1).min()
    df['zhenfu'] = high / low - 1

    return df


# 反转策略
def signal_contrarian(df, n=1):
    """
    反转策略：选择最近一段时间涨幅小的币种
    """
    df['contrarian'] = df['close'].pct_change(n)
    return df


# cmo指标
def signal_cmo(df, n=1):
    """
    """
    df['momentum'] = df['close'] - df['close'].shift(1)
    df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
    df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
    df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
    df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
    df['cmo'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
    return df
