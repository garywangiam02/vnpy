#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
from utils.diff import add_diff


def signal(*args):
    # CCI
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    tp = (df['high'] + df['low'] + df['close']) / 3
    ma = tp.rolling(window=n, min_periods=1).mean()
    md = abs(df['close'] - ma).rolling(window=n, min_periods=1).mean()
    df[factor_name] = (tp - ma) / md / 0.015

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
