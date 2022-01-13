#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['LMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # LMA 指标
    """
    N=20
    LMA=MA(LOW,N)
    LMA 为简单移动平均把收盘价替换为最低价。如果最低价上穿/下穿
    LMA 则产生买入/卖出信号。
    """
    df['low_ma'] = df['low'].rolling(n, min_periods=1).mean()
    # 进行去量纲
    df[f'LMA_bh_{n}'] = df['low'] / df['low_ma'] - 1
    df[f'LMA_bh_{n}'] = df[f'LMA_bh_{n}'].shift(1)
    
    del df['low_ma']

    return [f'LMA_bh_{n}', ]