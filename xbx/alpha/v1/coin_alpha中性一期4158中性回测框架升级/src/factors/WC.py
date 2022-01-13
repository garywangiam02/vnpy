#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['WC', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # WC 指标
    """
    WC=(HIGH+LOW+2*CLOSE)/4
    N1=20
    N2=40
    EMA1=EMA(WC,N1)
    EMA2=EMA(WC,N2)
    WC 也可以用来代替收盘价构造一些技术指标（不过相对比较少用
    到）。我们这里用 WC 的短期均线和长期均线的交叉来产生交易信号。
    """
    WC = (df['high'] + df['low'] + 2 * df['close']) / 4
    df['ema1'] = WC.ewm(n, adjust=False).mean()
    df['ema2'] = WC.ewm(2 * n, adjust=False).mean()
    # 去量纲
    df[f'WC_bh_{n}'] = df['ema1'] / df['ema2']
    df[f'WC_bh_{n}'] = df[f'WC_bh_{n}'].shift(1)
    
    del df['ema1']
    del df['ema2']

    return [f'WC_bh_{n}', ]