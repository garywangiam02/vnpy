#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['CCIV3', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # 计算魔改CCI指标
    df['oma'] = df['open'].ewm(span=n, adjust=False).mean()
    df['hma'] = df['high'].ewm(span=n, adjust=False).mean()
    df['lma'] = df['low'].ewm(span=n, adjust=False).mean()
    df['cma'] = df['close'].ewm(span=n, adjust=False).mean()
    
    df['tp'] = (df['oma'] + df['hma'] + df['lma'] + df['cma']) / 4
    df['ma'] = df['tp'].ewm(span=n, adjust=False).mean()
    df['abs_diff_close'] = abs(df['cma'] - df['ma'])
    df['md'] = df['abs_diff_close'].ewm(span=n, adjust=False).mean()

    tp = df['tp']
    ma = df['ma']
    md = df['md']

    df[f'CCIV3_bh_{n}'] = ((tp-ma)/md).shift(1)
    
    del df['oma']
    del df['hma']
    del df['lma']
    del df['cma']
    del df['tp']
    del df['ma']
    del df['abs_diff_close']
    del df['md']

    return [f'CCIV3_bh_{n}', ]