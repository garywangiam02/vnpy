#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['CCI', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
    df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
    df[f'CCI_bh_{n}'] = (df['tp'] - df['ma']) / df['md'] / 0.015
    df[f'CCI_bh_{n}'] = df[f'CCI_bh_{n}'].shift(1)

    del df['tp']
    del df['ma']
    del df['md']

    return [f'CCI_bh_{n}', ]