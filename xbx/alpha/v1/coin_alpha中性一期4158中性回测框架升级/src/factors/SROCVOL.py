#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['SROCVOL', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # SROCVOL 指标
    """
    N=20
    M=10
    EMAP=EMA(VOLUME,N)
    SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
    SROCVOL 与 ROCVOL 类似，但是会先对成交量进行移动平均平滑
    处理之后再取其变化率。（SROCVOL 是 SROC 的成交量版本。）
    SROCVOL 上穿 0 买入，下穿 0 卖出。
    """
    df['emap'] = df['volume'].ewm(2 *n, adjust=False).mean()
    df['SROCVOL'] = (df['emap'] - df['emap'].shift(n)) / df['emap'].shift(n)
    df[f'SROCVOL_bh_{n}'] = df['SROCVOL'].shift(1)
    
    del df['emap']
    del df['SROCVOL']

    return [f'SROCVOL_bh_{n}', ]