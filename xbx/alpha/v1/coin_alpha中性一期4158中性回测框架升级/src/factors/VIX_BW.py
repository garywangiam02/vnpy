#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['VIX_BW', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    df['vix'] = df['close'] / df['close'].shift(n) - 1
    df['vix_median'] = df['vix'].rolling(window=n, min_periods=1).mean()
    df['vix_std']    = df['vix'].rolling(n, min_periods=1).std()
    df['vix_score']  = abs(df['vix'] - df['vix_median']) / df['vix_std']

    df['max'] = df['vix_score'].rolling(window=n, min_periods=1).max().shift(1)
    df['min'] = df['vix_score'].rolling(window=n, min_periods=1).min().shift(1)
    df['vix_upper'] = df['vix_median'] + df['max'] * df['vix_std']
    df['vix_lower'] = df['vix_median'] - df['max'] * df['vix_std']

    df[f'VIX_BW_bh_{n}'] = (df['vix_upper'] - df['vix_lower'])*np.sign(df['vix_median'].diff(n))

    condition1 = np.sign(df['vix_median'].diff(n)) != np.sign(df['vix_median'].diff(1))
    condition2 = np.sign(df['vix_median'].diff(n)) != np.sign(df['vix_median'].diff(1).shift(1))
    df.loc[condition1, f'VIX_BW_bh_{n}'] = 0
    df.loc[condition2, f'VIX_BW_bh_{n}'] = 0

    df[f'VIX_BW_bh_{n}'] = df[f'VIX_BW_bh_{n}'].shift(1)

    del df['vix']
    del df['vix_median']
    del df['vix_std']
    del df['vix_score']
    del df['max']
    del df['min']
    del df['vix_upper']
    del df['vix_lower']

    return [f'VIX_BW_bh_{n}', ]




    