#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['RSI', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    # --- RSI ---  在期货市场很有效
    close_dif  = df['close'].diff()
    df['up']   = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)

    a = df['up'].rolling(n).sum()
    b = df['down'].rolling(n).sum()
    df[f'RSI_bh_{n}'] = (a / (a + b)).shift(1)  # RSI

    del df['up']
    del df['down']

    return [f'RSI_bh_{n}', ]


