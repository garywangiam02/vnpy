#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['GAP', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    ma  = df['close'].rolling(n, min_periods=1).mean()
    ema = df['close'].ewm(span=n, adjust=False).mean()
    gap = ema - ma
    df[f'GAP_bh_{n}'] = gap / abs(gap).rolling(n).sum()
    df[f'GAP_bh_{n}'] = df[f'GAP_bh_{n}'].shift(1)

    return [f'GAP_bh_{n}', ]