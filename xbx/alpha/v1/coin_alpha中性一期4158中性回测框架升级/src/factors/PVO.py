#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['PVO', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # PVO 指标
    """
    N1=12
    N2=26
    PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
    PVO 用成交量的指数移动平均来反应成交量的变化。PVO 上穿 0 线
    买入；PVO 下穿 0 线卖出。
    """
    df['emap_1'] = df['volume'].ewm(n, min_periods=1).mean()
    df['emap_2'] = df['volume'].ewm(n * 2, min_periods=1).mean()
    df['PVO'] = (df['emap_1'] - df['emap_2']) / df['emap_2']
    df[f'PVO_bh_{n}'] = df['PVO'].shift(1)

    del df['emap_1']
    del df['emap_2']
    del df['PVO']

    return [f'PVO_bh_{n}', ]