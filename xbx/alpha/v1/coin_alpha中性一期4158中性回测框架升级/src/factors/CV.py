#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['CV', ]

def signal(*args):
    df = args[0]
    n  = args[1]
    # CV 指标
    """
    N=10
    H_L_EMA=EMA(HIGH-LOW,N)
    CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
    CV 指标用来衡量股价的波动，反映一段时间内最高价与最低价之差
    （价格变化幅度）的变化率。如果 CV 的绝对值下穿 30，买入；
    如果 CV 的绝对值上穿 70，卖出。
    """
    df['H_L_ema'] = (df['high'] - df['low']).ewm(n, adjust=False).mean()
    df['CV'] = (df['H_L_ema'] - df['H_L_ema'].shift(n)) / df['H_L_ema'].shift(n) * 100
    df[f'CV_bh_{n}'] = df['CV'].shift(1)
    
    del df['H_L_ema']
    del df['CV']

    return [f'CV_bh_{n}', ]