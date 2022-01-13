#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['BBI_BIAS', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # 计算BBI 的bias
    ma1 = df['close'].rolling(n, min_periods=1).mean()
    ma2 = df['close'].rolling(2 * n, min_periods=1).mean()
    ma3 = df['close'].rolling(4 * n, min_periods=1).mean()
    ma4 = df['close'].rolling(8 * n, min_periods=1).mean()
    bbi = (ma1 + ma2 + ma3 + ma4) / 4
    df[f'BBI_BIAS_bh_{n}'] = df['close'] / bbi - 1
    df[f'BBI_BIAS_bh_{n}'] = df[f'BBI_BIAS_bh_{n}'].shift(1)

    return [f'BBI_BIAS_bh_{n}', ]