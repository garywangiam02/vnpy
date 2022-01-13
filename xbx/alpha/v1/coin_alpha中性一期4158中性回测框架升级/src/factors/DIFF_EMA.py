#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['DIFF_EMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    short_windows   = n
    long_windows    = 3 * n
    df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()
    df['ema_long']  = df['close'].ewm(span=long_windows, adjust=False).mean()
    df['diff_ema']  = df['ema_short'] - df['ema_long']

    df['diff_ema_mean'] = df['diff_ema'].ewm(span=n, adjust=False).mean()

    df[f'DIFF_EMA_bh_{n}'] = df['diff_ema'] / df['diff_ema_mean'] - 1
    df[f'DIFF_EMA_bh_{n}'] = df[f'DIFF_EMA_bh_{n}'].shift(1)
    
    del df['ema_short']
    del df['ema_long']
    del df['diff_ema']
    del df['diff_ema_mean']

    return [f'DIFF_EMA_bh_{n}', ]