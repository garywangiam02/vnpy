#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['HLMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # HLMA 指标
    """
    N1=20
    N2=20
    HMA=MA(HIGH,N1)
    LMA=MA(LOW,N2)
    HLMA 指标是把普通的移动平均中的收盘价换为最高价和最低价分
    别得到 HMA 和 LMA。当收盘价上穿 HMA/下穿 LMA 时产生买入/卖
    出信号。
    """
    hma = df['high'].rolling(n, min_periods=1).mean()
    lma = df['low'].rolling(n, min_periods=1).mean()
    df['HLMA'] = hma - lma
    df['HLMA_mean'] = df['HLMA'].rolling(n, min_periods=1).mean()

    # 去量纲
    df[f'HLMA_bh_{n}'] = df['HLMA'] / df['HLMA_mean'] - 1
    df[f'HLMA_bh_{n}'] = df[f'HLMA_bh_{n}'].shift(1)
    
    del df['HLMA']
    del df['HLMA_mean']

    return [f'HLMA_bh_{n}', ]