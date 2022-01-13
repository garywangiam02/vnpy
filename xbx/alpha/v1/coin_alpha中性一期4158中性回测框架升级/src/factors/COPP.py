#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['COPP', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # COPP 指标
    """
    RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
    COPP=WMA(RC,M)
    COPP 指标用不同时间长度的价格变化率的加权移动平均值来衡量
    动量。如果 COPP 上穿/下穿 0 则产生买入/卖出信号。
    """
    df['RC']   = 100 * ((df['close'] - df['close'].shift(n)) / df['close'].shift(n) + (df['close'] - df['close'].shift(2 * n)) / df['close'].shift(2 * n))
    df['COPP'] = df['RC'].rolling(n, min_periods=1).mean()
    df[f'COPP_bh_{n}'] = df['COPP'].shift(1)
    
    del df['RC']
    del df['COPP']

    return [f'COPP_bh_{n}', ]