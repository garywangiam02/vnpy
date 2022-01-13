#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['VMA_BIAS', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # VMA 指标
    """
    N=20
    PRICE=(HIGH+LOW+OPEN+CLOSE)/4
    VMA=MA(PRICE,N)
    VMA 就是简单移动平均把收盘价替换为最高价、最低价、开盘价和
    收盘价的平均值。当 PRICE 上穿/下穿 VMA 时产生买入/卖出信号。
    """
    price = (df['high'] + df['low'] + df['open'] + df['close']) / 4
    vma = price.rolling(n, min_periods=1).mean()
    df[f'VMA_BIAS_bh_{n}'] = price / vma - 1
    df[f'VMA_BIAS_bh_{n}'] = df[f'VMA_BIAS_bh_{n}'].shift(1)
    return [f'VMA_BIAS_bh_{n}', ]