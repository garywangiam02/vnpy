#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['POS', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # POS指标
    """
    N=100
    PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
    POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
    POS 指标衡量当前的 N 天收益率在过去 N 天的 N 天收益率最大值和
    最小值之间的位置。当 POS 上穿 80 时产生买入信号；当 POS 下穿
    20 时产生卖出信号。

    """
    ref = df['close'].shift(n)
    price = (df['close'] - ref) / ref
    min_price = price.rolling(n).min()
    max_price = price.rolling(n).max()
    pos = (price - min_price) / (max_price - min_price)
    df[f'POS_bh_{n}'] = pos.shift(1)
    
    return [f'POS_bh_{n}', ]