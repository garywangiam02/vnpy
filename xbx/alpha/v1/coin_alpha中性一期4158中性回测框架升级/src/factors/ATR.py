#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ATR', ]

def signal(*args):
    df = args[0]
    n  = args[1]
    # ATR 因子
    df['c1'] = df['high'] - df['low']
    df['c2'] = abs(df['high'] - df['close'].shift(1))
    df['c3'] = abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()
    df['middle'] = df['close'].rolling(n, min_periods=1).mean()

    # ATR指标去量纲
    df[f'ATR_bh_{n}'] = df['ATR'] / df['middle']
    df[f'ATR_bh_{n}'] = df[f'ATR_bh_{n}'].shift(1)

    del df['c1']
    del df['c2']
    del df['c3']
    del df['TR']
    del df['ATR']
    del df['middle']

    return [f'ATR_bh_{n}', ]