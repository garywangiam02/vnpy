#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ZLMACD', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # ZLMACD 指标
    """
    N1=20
    N2=100
    ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EM
    A(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
    ZLMACD 指标是对 MACD 指标的改进，它在计算中使用 DEMA 而不
    是 EMA，可以克服 MACD 指标的滞后性问题。如果 ZLMACD 上穿/
    下穿 0，则产生买入/卖出信号。
    """
    ema1 = df['close'].ewm(n, adjust=False).mean()
    ema_ema_1 = ema1.ewm(n, adjust=False).mean()
    n2 = 5 * n
    ema2 = df['close'].ewm(n2, adjust=False).mean()
    ema_ema_2 = ema2.ewm(n2, adjust=False).mean()
    ZLMACD = (2 * ema1 - ema_ema_1) - (2 * ema2 - ema_ema_2)
    df[f'ZLMACD_bh_{n}'] = df['close'] / ZLMACD - 1
    df[f'ZLMACD_bh_{n}'] = df[f'ZLMACD_bh_{n}'].shift(1)

    return [f'ZLMACD_bh_{n}', ]