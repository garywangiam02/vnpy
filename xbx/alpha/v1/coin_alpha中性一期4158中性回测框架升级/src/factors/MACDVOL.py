#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['MACDVOL', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # MACDVOL 指标
    """
    N1=20
    N2=40
    N3=10
    MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
    SIGNAL=MA(MACDVOL,N3)
    MACDVOL 是 MACD 的成交量版本。如果 MACDVOL 上穿 SIGNAL，
    则买入；下穿 SIGNAL 则卖出。
    """
    N1 = 2 * n
    N2 = 4 * n
    N3 = n
    df['ema_volume_1'] = df['volume'].ewm(N1, adjust=False).mean()
    df['ema_volume_2'] = df['volume'].ewm(N2, adjust=False).mean()
    df['MACDV'] = df['ema_volume_1'] - df['ema_volume_2']
    df['SIGNAL'] = df['MACDV'].rolling(N3, min_periods=1).mean()
    # 去量纲
    df['MACDVOL'] = df['MACDV'] / df['SIGNAL'] - 1
    df[f'MACDVOL_bh_{n}'] = df['MACDVOL'].shift(1)
    
    del df['ema_volume_1']
    del df['ema_volume_2']
    del df['MACDV']
    del df['SIGNAL']
    del df['MACDVOL']

    return [f'MACDVOL_bh_{n}', ]