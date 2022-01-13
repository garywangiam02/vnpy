#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['RMI', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # RMI 指标
    """
    N=7
    RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
    RMI 与 RSI 的计算方式类似，将 RSI 中的动量与前一天价格之差
    CLOSE-REF(CLOSE,1)项改为了与前四天价格之差 CLOSEREF(CLOSE,4)
    """
    df['max_close'] = np.where(df['close'] > df['close'].shift(4), df['close'] - df['close'].shift(4), 0)
    df['abs_close'] = df['close'] - df['close'].shift(1)
    df['sma_1'] = df['max_close'].rolling(n, min_periods=1).mean()
    df['sma_2'] = df['abs_close'].rolling(n, min_periods=1).mean()
    df['RMI'] = df['sma_1'] / df['sma_2'] * 100
    df[f'RMI_bh_{n}'] = df['RMI'].shift(1)
    
    del df['max_close']
    del df['abs_close']
    del df['sma_1']
    del df['sma_2']
    del df['RMI']

    return [f'RMI_bh_{n}', ]