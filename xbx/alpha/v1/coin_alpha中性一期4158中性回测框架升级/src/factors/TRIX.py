#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['TRIX', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # TRIX 指标
    """
    TRIPLE_EMA=EMA(EMA(EMA(CLOSE,N),N),N)
    TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
    TRIX 求价格的三重指数移动平均的变化率。当 TRIX>0 时，当前可
    能处于上涨趋势；当 TRIX<0 时，当前可能处于下跌趋势。TRIX 相
    比于普通移动平均的优点在于它通过三重移动平均去除了一些小的
    趋势和市场的噪音。我们可以通过 TRIX 上穿/下穿 0 线产生买入/卖
    出信号。
    """
    df['ema'] = df['close'].ewm(n, adjust=False).mean()
    df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()
    df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean()

    df['TRIX'] = (df['ema_ema_ema'] - df['ema_ema_ema'].shift(1)) / df['ema_ema_ema'].shift(1)

    df[f'TRIX_bh_{n}'] = df['TRIX'].shift(1)
    
    del df['ema']
    del df['ema_ema']
    del df['ema_ema_ema']
    del df['TRIX']

    return [f'TRIX_bh_{n}', ]