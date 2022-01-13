#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['MAAMT', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # MAAMT 指标
    """
    N=40
    MAAMT=MA(AMOUNT,N)
    MAAMT 是成交额的移动平均线。当成交额上穿/下穿移动平均线时产
    生买入/卖出信号。
    """
    df['MAAMT'] = df['volume'].rolling(n, min_periods=1).mean()
    df[f'MAAMT_bh_{n}'] = df['MAAMT'].shift(1)
    
    del df['MAAMT']

    return [f'MAAMT_bh_{n}', ]