#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ATRUpper', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    tr = np.max(np.array([
        (df['high'] - df['low']).abs(),
        (df['high'] - df['close'].shift(1)).abs(),
        (df['low']  - df['close'].shift(1)).abs()
    ]), axis=0)  # 三个数列取其大值
    atr = pd.Series(tr).ewm(alpha=1/n, adjust=False).mean().shift(1)
    df[f'ATRUpper_bh_{n}'] = df['low'].rolling(int(n / 2), min_periods=1).min() + 3 * atr  # ===ATRUpper_bh_3
    df[f'ATRUpper_bh_{n}'] = df[f'ATRUpper_bh_{n}'].shift(1)

    return [f'ATRUpper_bh_{n}', ]