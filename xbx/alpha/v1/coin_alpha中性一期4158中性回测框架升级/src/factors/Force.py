#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['Force', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    df['force'] = df['quote_volume'] * (df['close'] - df['close'].shift(1))
    df[f'Force_bh_{n}'] = df['force'].rolling(n, min_periods=1).mean().shift(1)

    del df['force']

    return [f'Force_bh_{n}', ]