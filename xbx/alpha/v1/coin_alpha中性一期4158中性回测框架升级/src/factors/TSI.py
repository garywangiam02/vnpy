#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['TSI', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # TSI 指标
    """
    N1=25
    N2=13
    TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(
    CLOSE-REF(CLOSE,1)),N1),N2)*100
    TSI 是一种双重移动平均指标。与常用的移动平均指标对收盘价取移
    动平均不同，TSI 对两天收盘价的差值取移动平均。如果 TSI 上穿 10/
    下穿-10 则产生买入/卖出指标。
    """
    n1 = 2 * n
    df['diff_close'] = df['close'] - df['close'].shift(1)
    df['ema'] = df['diff_close'].ewm(n1, adjust=False).mean()
    df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()

    df['abs_diff_close'] = abs(df['diff_close'])
    df['abs_ema'] = df['abs_diff_close'].ewm(n1, adjust=False).mean()
    df['abs_ema_ema'] = df['abs_ema'].ewm(n, adjust=False).mean()

    df['TSI'] = df['ema_ema'] / df['abs_ema_ema'] * 100

    df[f'TSI_bh_{n}'] = df['TSI'].shift(1)
    
    del df['diff_close']
    del df['ema']
    del df['ema_ema']
    del df['abs_diff_close']
    del df['abs_ema']
    del df['abs_ema_ema']
    del df['TSI']

    return [f'TSI_bh_{n}', ]




