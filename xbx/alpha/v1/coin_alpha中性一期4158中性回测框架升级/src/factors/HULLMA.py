#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['HULLMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # HULLMA 指标
    """
    N=20,80
    X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
    HULLMA=EMA(X,[√𝑁])
    HULLMA 也是均线的一种，相比于普通均线有着更低的延迟性。我们
    用短期均线上/下穿长期均线来产生买入/卖出信号。
    """
    ema1 = df['close'].ewm(n, adjust=False).mean()
    ema2 = df['close'].ewm(n * 2, adjust=False).mean()
    df['X'] = 2 * ema1 - ema2
    df['HULLMA'] = df['X'].ewm(int(np.sqrt(2 * n)), adjust=False).mean()

    df[f'HULLMA_bh_{n}'] = df['X'] / df['HULLMA']
    df[f'HULLMA_bh_{n}'] = df[f'HULLMA_bh_{n}'].shift(1)
    
    del df['X']
    del df['HULLMA']

    return [f'HULLMA_bh_{n}', ]