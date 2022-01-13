#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['PPO', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # PPO 指标
    """
    N1=12
    N2=26
    N3=9
    PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
    PPO_SIGNAL=EMA(PPO,N3)
    PPO 是 MACD 的变化率版本。
    MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)，而
    PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)。
    PPO 上穿/下穿 PPO_SIGNAL 产生买入/卖出信号。
    """
    N3 = n
    N1 = int(n * 1.312)
    N2 = 3 * n
    df['ema_1'] = df['close'].ewm(N1, adjust=False).mean()
    df['ema_2'] = df['close'].ewm(N2, adjust=False).mean()
    df['PPO'] = (df['ema_1'] - df['ema_2']) / df['ema_2']
    df['PPO_SIGNAL'] = df['PPO'].ewm(N3, adjust=False).mean()

    df[f'PPO_bh_{n}'] = df['PPO_SIGNAL'].shift(1)
    
    del df['ema_1']
    del df['ema_2']
    del df['PPO']
    del df['PPO_SIGNAL']

    return [f'PPO_bh_{n}', ]