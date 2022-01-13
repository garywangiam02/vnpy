#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['BIAS36', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # BIAS36
    """
    N=6
    BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
    MABIAS36=MA(BIAS36,N)
    类似于乖离用来衡量当前价格与移动平均价的差距，三六乖离用来衡
    量不同的移动平均价间的差距。当三六乖离上穿/下穿其均线时，产生
    买入/卖出信号。
    """
    df['ma3'] = df['close'].rolling(n, min_periods=1).mean()
    df['ma6'] = df['close'].rolling(2 * n, min_periods=1).mean()
    df['BIAS36'] = df['ma3'] - df['ma6']
    df['MABIAS36'] = df['BIAS36'].rolling(2 * n, min_periods=1).mean()
    # 去量纲
    df[f'BIAS36_bh_{n}'] = df['BIAS36'] / df['MABIAS36']
    df[f'BIAS36_bh_{n}'] = df[f'BIAS36_bh_{n}'].shift(1)
    
    del df['ma3']
    del df['ma6']
    del df['BIAS36']
    del df['MABIAS36'] 

    return [f'BIAS36_bh_{n}']