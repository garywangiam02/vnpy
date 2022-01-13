#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['DEMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # DEMA 指标
    """
    N=60
    EMA=EMA(CLOSE,N)
    DEMA=2*EMA-EMA(EMA,N)
    DEMA 结合了单重 EMA 和双重 EMA，在保证平滑性的同时减少滞后
    性。
    """
    ema = df['close'].ewm(n, adjust=False).mean()
    ema_ema = ema.ewm(n, adjust=False).mean()
    dema = 2 * ema - ema_ema
    # dema 去量纲
    df[f'DEMA_bh_{n}'] = dema / ema
    df[f'DEMA_bh_{n}'] = df[f'DEMA_bh_{n}'].shift(1)
    
    return [f'DEMA_bh_{n}', ]