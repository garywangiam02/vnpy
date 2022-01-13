#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['TEMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # TEMA 指标
    """
    N=20,40
    TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
    TEMA 结合了单重、双重和三重的 EMA，相比于一般均线延迟性较
    低。我们用快、慢 TEMA 的交叉来产生交易信号。
    """
    df['ema'] = df['close'].ewm(n, adjust=False).mean()
    df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()
    df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean()
    df['TEMA'] = 3 * df['ema'] - 3 * df['ema_ema'] + df['ema_ema_ema']
    # 去量纲
    df[f'TEMA_bh_{n}'] = df['ema'] / df['TEMA']
    df[f'TEMA_bh_{n}'] = df[f'TEMA_bh_{n}'].shift(1)
    
    del df['ema']
    del df['ema_ema']
    del df['ema_ema_ema']
    del df['TEMA']

    return [f'TEMA_bh_{n}', ]