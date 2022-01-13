#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['TMA_BIAS', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # TMA 指标
    """
    N=20
    CLOSE_MA=MA(CLOSE,N)
    TMA=MA(CLOSE_MA,N)
    TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
    越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
    靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
    卖出信号。
    """
    ma = df['close'].rolling(n, min_periods=1).mean()
    tma = ma.rolling(n, min_periods=1).mean()
    df[f'TMA_BIAS_bh_{n}'] = df['close'] / tma - 1
    df[f'TMA_BIAS_bh_{n}'] = df[f'TMA_BIAS_bh_{n}'].shift(1)
    return [f'TMA_BIAS_bh_{n}', ]