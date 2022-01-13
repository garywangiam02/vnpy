#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['VIDYA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # VIDYA
    """
    N=10
    VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
    VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
    VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER
    （EfficiencyRatio）指标。在当前趋势较强时，ER 值较大，VIDYA
    会赋予当前价格更大的权重，使得 VIDYA 紧随价格变动，减小其滞
    后性；在当前趋势较弱（比如振荡市中）,ER 值较小，VIDYA 会赋予
    当前价格较小的权重，增大 VIDYA 的滞后性，使其更加平滑，避免
    产生过多的交易信号。
    当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
    """
    df['abs_diff_close'] = abs(df['close'] - df['close'].shift(n))
    df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n).sum()
    VI = df['abs_diff_close'] / df['abs_diff_close_sum']
    VIDYA = VI * df['close'] + (1- VI) * df['close'].shift(1)
    # 进行无量纲处理
    df[f'VIDYA_bh_{n}'] = VIDYA / df['close']
    df[f'VIDYA_bh_{n}'] = df[f'VIDYA_bh_{n}'].shift(1)
    
    del df['abs_diff_close']
    del df['abs_diff_close_sum']

    return [f'VIDYA_bh_{n}', ]