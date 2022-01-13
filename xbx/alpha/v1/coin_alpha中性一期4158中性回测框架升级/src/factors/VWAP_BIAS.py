#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['VWAP_BIAS', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    df['vwap'] = df['volume'] / df['quote_volume']
    ma = df['vwap'].rolling(n, min_periods=1).mean()
    df[f'VWAP_BIAS_bh_{n}'] = df['vwap'] / ma - 1
    df[f'VWAP_BIAS_bh_{n}'] = df[f'VWAP_BIAS_bh_{n}'].shift(1)
    
    del df['vwap']

    return [f'VWAP_BIAS_bh_{n}', ]