#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['SROC', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # SROC 指标
    """
    N=13
    M=21
    EMAP=EMA(CLOSE,N)
    SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
    SROC 与 ROC 类似，但是会对收盘价进行平滑处理后再求变化率。
    """
    ema = df['close'].ewm(n, adjust=False).mean()
    ref = ema.shift(2 * n)
    df[f'SROC_bh_{n}'] = (ema - ref) / ref
    df[f'SROC_bh_{n}'] = df[f'SROC_bh_{n}'].shift(1)

    return [f'SROC_bh_{n}', ]