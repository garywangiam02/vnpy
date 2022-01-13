#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['KDJD_K', 'KDJD_D', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # KDJD 指标
    """
    N=20
    M=60
    LOW_N=MIN(LOW,N)
    HIGH_N=MAX(HIGH,N)
    Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
    Stochastics_LOW=MIN(Stochastics,M)
    Stochastics_HIGH=MAX(Stochastics,M)
    Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
    K=SMA(Stochastics_DOUBLE,3,1)
    D=SMA(K,3,1)
    KDJD 可以看作 KDJ 的变形。KDJ 计算过程中的变量 Stochastics 用
    来衡量收盘价位于最近 N 天最高价和最低价之间的位置。而 KDJD 计
    算过程中的 Stochastics_DOUBLE 可以用来衡量 Stochastics 在最近
    N 天的 Stochastics 最大值与最小值之间的位置。我们这里将其用作
    动量指标。当 D 上穿 70/下穿 30 时，产生买入/卖出信号。
    """
    min_low = df['low'].rolling(n).min()
    max_high = df['high'].rolling(n).max()
    Stochastics = (df['close'] - min_low) / (max_high - min_low) * 100
    Stochastics_LOW = Stochastics.rolling(n*3).min()
    Stochastics_HIGH = Stochastics.rolling(n*3).max()
    Stochastics_DOUBLE = (Stochastics - Stochastics_LOW) / (Stochastics_HIGH - Stochastics_LOW)
    K = Stochastics_DOUBLE.ewm(com=2).mean()
    D = K.ewm(com=2).mean()
    df[f'KDJD_K_bh_{n}'] = K.shift(1)
    df[f'KDJD_D_bh_{n}'] = D.shift(1)

    return [f'KDJD_K_bh_{n}', f'KDJD_D_bh_{n}']


