#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['MACD', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # 计算macd指标
    short_windows = n
    long_windows = 3 * n
    macd_windows = int(1.618 * n)

    df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()
    df['ema_long']  = df['close'].ewm(span=long_windows, adjust=False).mean()
    df['dif']  = df['ema_short'] - df['ema_long']
    df['dea']  = df['dif'].ewm(span=macd_windows, adjust=False).mean()
    df['macd'] = 2 * (df['dif'] - df['dea'])

    df[f'MACD_bh_{n}'] = df['macd'] / df['macd'].rolling(macd_windows, min_periods=1).mean() - 1
    df[f'MACD_bh_{n}'] = df[f'MACD_bh_{n}'].shift(1)

    del df['ema_short']
    del df['ema_long']
    del df['dif']
    del df['dea']
    del df['macd']

    return [f'MACD_bh_{n}', ]