#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['T3', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # T3 指标
    """
    N=20
    VA=0.5
    T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
    T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
    T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
    当 VA 是 0 时，T3 就是三重指数平均线，此时具有严重的滞后性；当
    VA 是 0 时，T3 就是三重双重指数平均线（DEMA），此时可以快速
    反应价格的变化。VA 值是 T3 指标的一个关键参数，可以用来调节
    T3 指标的滞后性。如果收盘价上穿/下穿 T3，则产生买入/卖出信号。
    """
    va = 0.5
    ema = df['close'].ewm(n,adjust=False).mean()
    ema_ema = ema.ewm(n, adjust=False).mean()
    T1 = ema * (1 + va) - ema_ema * va
    T1_ema = T1.ewm(n, adjust=False).mean()
    T1_ema_ema = T1_ema.ewm(n, adjust=False).mean()
    T2 = T1_ema * (1 + va) - T1_ema_ema * va
    T2_ema = T2.ewm(n, adjust=False).mean()
    T2_ema_ema = T2_ema.ewm(n, adjust=False).mean()
    T3 = T2_ema * (1 + va) - T2_ema_ema * va
    df[f'T3_bh_{n}'] = df['close'] / T3 - 1
    df[f'T3_bh_{n}'] = df[f'T3_bh_{n}'].shift(1)
    return [f'T3_bh_{n}', ]