#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['WR', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # WR 指标
    """
    HIGH(N)=MAX(HIGH,N)
    LOW(N)=MIN(LOW,N)
    WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
    WR 指标事实上就是 100-KDJ 指标计算过程中的 Stochastics。WR
    指标用来衡量市场的强弱和超买超卖状态。一般认为，当 WR 小于
    20 时，市场处于超买状态；当 WR 大于 80 时，市场处于超卖状态；
    当 WR 处于 20 到 80 之间时，多空较为平衡。
    如果 WR 上穿 80，则产生买入信号；
    如果 WR 下穿 20，则产生卖出信号。
    """
    df['max_high'] = df['high'].rolling(n, min_periods=1).max()
    df['min_low'] = df['low'].rolling(n, min_periods=1).min()
    df['WR'] = (df['max_high'] - df['close']) / (df['max_high'] - df['min_low']) * 100
    df[f'WR_bh_{n}'] = df['WR'].shift(1)
    
    del df['max_high']
    del df['min_low']
    del df['WR']

    return [f'WR_bh_{n}', ]