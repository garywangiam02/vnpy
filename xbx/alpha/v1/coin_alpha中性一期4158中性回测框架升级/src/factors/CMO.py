#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd

factors = ['CMO', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    df['momentum'] = df['close'] - df['close'].shift(1)
    df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
    df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
    df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
    df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
    
    df[f'CMO_bh_{n}'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
    df[f'CMO_bh_{n}'] = df[f'CMO_bh_{n}'].shift(1)

    return [f'CMO_bh_{n}', ]



    