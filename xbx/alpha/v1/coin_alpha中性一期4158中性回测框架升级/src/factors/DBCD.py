#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['DBCD', ]


def signal(*args):
    # DBCD 指标
    """
    N=5
    M=16
    T=17
    BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
    BIAS_DIF=BIAS-REF(BIAS,M)
    DBCD=SMA(BIAS_DIFF,T,1)
    DBCD（异同离差乖离率）为乖离率离差的移动平均。我们用 DBCD
    上穿 5%/下穿-5%来产生买入/卖出信号。
    """
    df = args[0]
    n  = args[1]

    df['ma'] = df['close'].rolling(n, min_periods=1).mean()

    df['BIAS'] = (df['close'] - df['ma']) / df['ma'] * 100
    df['BIAS_DIF'] = df['BIAS'] - df['BIAS'].shift(3 * n)
    df['DBCD'] = df['BIAS_DIF'].rolling(3 * n + 2, min_periods=1).mean()
    df[f'DBCD_bh_{n}'] = df['DBCD'].shift(1)
    
    del df['ma']
    del df['BIAS']
    del df['BIAS_DIF']
    del df['DBCD']

    return [f'DBCD_bh_{n}', ]
