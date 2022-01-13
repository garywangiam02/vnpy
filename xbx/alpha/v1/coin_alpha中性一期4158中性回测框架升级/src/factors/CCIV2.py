#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['CCIV2', ]

def signal(*args):
    df = args[0]
    n  = args[1]
    # 计算魔改CCI指标
    open_ma  = df['open'].rolling(n, min_periods=1).mean()
    high_ma  = df['high'].rolling(n,min_periods=1).mean()
    low_ma   = df['low'].rolling(n, min_periods=1).mean()
    close_ma = df['close'].rolling(n, min_periods=1).mean()
    tp = (high_ma + low_ma + close_ma) / 3
    ma = tp.rolling(n, min_periods=1).mean()
    md = abs(ma - close_ma).rolling(n, min_periods=1).mean()
    df[f'CCIV2_bh_{n}'] = ((tp - ma) / md / 0.015).shift(1)

    return [f'CCIV2_bh_{n}', ]