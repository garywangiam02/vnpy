#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ROCVOL', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # ROCVOL 指标
    """
    N = 80
    ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)
    ROCVOL 是 ROC 的成交量版本。如果 ROCVOL 上穿 0 则买入，下
    穿 0 则卖出。
    """
    df['ROCVOL'] = df['volume'] / df['volume'].shift(n) - 1
    df[f'ROCVOL_bh_{n}'] = df['ROCVOL'].shift(1)
    
    return [f'ROCVOL_bh_{n}', ]