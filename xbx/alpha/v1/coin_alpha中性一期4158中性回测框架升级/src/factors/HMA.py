#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['HMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # HMA 指标
    """
    N=20
    HMA=MA(HIGH,N)
    HMA 指标为简单移动平均线把收盘价替换为最高价。当最高价上穿/
    下穿 HMA 时产生买入/卖出信号。
    """
    hma = df['high'].rolling(n, min_periods=1).mean()
    df[f'HMA_bh_{n}'] = df['high'] / hma - 1
    df[f'HMA_bh_{n}'] = df[f'HMA_bh_{n}'].shift(1)

    return [f'HMA_bh_{n}', ]