#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['RSIS', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # RSIS 指标
    """
    N=120
    M=20
    CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
    OSE,1),0)
    RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOS
    E,1)),N,1)*100
    RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
    RSISMA=EMA(RSIS,M)
    RSIS 反映当前 RSI 在最近 N 天的 RSI 最大值和最小值之间的位置，
    与 KDJ 指标的构造思想类似。由于 RSIS 波动性比较大，我们先取移
    动平均再用其产生信号。其用法与 RSI 指标的用法类似。
    RSISMA 上穿 40 则产生买入信号；
    RSISMA 下穿 60 则产生卖出信号。
    """
    N = 6 * n
    M = n
    df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1),0)
    df['sma_1'] = df['close_diff_pos'].rolling(N).sum()
    df['sma_2'] = abs(df['close'] - df['close'].shift(1)).rolling(N).sum()
    df['RSI'] = df['sma_1'] / df['sma_2'] * 100
    df['RSIS'] = (df['RSI'] - df['RSI'].rolling(N, min_periods=1).min()) / (df['RSI'].rolling(N, min_periods=1).max() - df['RSI'].rolling(N, min_periods=1).min()) * 100

    df['RSISMA'] = df['RSIS'].ewm(M, adjust=False).mean()
    df[f'RSIS_bh_{n}'] = df['RSISMA'].shift(1)

    del df['close_diff_pos']
    del df['sma_1']
    del df['sma_2']
    del df['RSI']
    del df['RSIS']
    del df['RSISMA']

    return [f'RSIS_bh_{n}', ]









        