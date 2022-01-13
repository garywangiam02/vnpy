#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['DPO', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # 计算 DPO
    """
    N=20
    DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
    DPO 是当前价格与延迟的移动平均线的差值，通过去除前一段时间
    的移动平均价格来减少长期的趋势对短期价格波动的影响。DPO>0
    表示目前处于多头市场；DPO<0 表示当前处于空头市场。我们通过
    DPO 上穿/下穿 0 线来产生买入/卖出信号。

    """
    ma = df['close'].rolling(n, min_periods=1).mean()
    ref = ma.shift(int(n/2 + 1))
    df[f'DPO_bh_{n}'] = df['close'] / ref - 1
    df[f'DPO_bh_{n}'] = df[f'DPO_bh_{n}'].shift(1)

    return [f'DPO_bh_{n}', ]