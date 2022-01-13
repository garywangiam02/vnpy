#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['MADisplaced', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # MADisplaced 指标
    """
    N=20
    M=10
    MA_CLOSE=MA(CLOSE,N)
    MADisplaced=REF(MA_CLOSE,M)
    MADisplaced 指标把简单移动平均线向前移动了 M 个交易日，用法
    与一般的移动平均线一样。如果收盘价上穿/下穿 MADisplaced 则产
    生买入/卖出信号。
    有点变种bias
    """

    ma = df['close'].rolling(2 * n, min_periods=1).mean()
    ref = ma.shift(n)

    df[f'MADisplaced_bh_{n}'] = df['close'] / ref - 1
    df[f'MADisplaced_bh_{n}'] = df[f'MADisplaced_bh_{n}'].shift(1)

    return [f'MADisplaced_bh_{n}', ]








