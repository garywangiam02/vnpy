#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['OSC', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # OSC 指标
    """
    N=40
    M=20
    OSC=CLOSE-MA(CLOSE,N)
    OSCMA=MA(OSC,M)
    OSC 反映收盘价与收盘价移动平均相差的程度。如果 OSC 上穿/下 穿 OSCMA 则产生买入/卖出信号。
    """
    df['ma'] = df['close'].rolling(2 * n, min_periods=1).mean()
    df['OSC'] = df['close'] - df['ma']
    df['OSCMA'] = df['OSC'].rolling(n, min_periods=1).mean()
    df[f'OSC_bh_{n}'] = df['OSCMA'].shift(1)
    
    del df['ma']
    del df['OSC']
    del df['OSCMA']

    return [f'OSC_bh_{n}', ]