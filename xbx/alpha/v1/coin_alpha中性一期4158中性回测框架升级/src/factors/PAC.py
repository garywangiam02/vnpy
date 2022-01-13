#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['PAC', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # PAC 指标
    """
    N1=20
    N2=20
    UPPER=SMA(HIGH,N1,1)
    LOWER=SMA(LOW,N2,1)
    用最高价和最低价的移动平均来构造价格变化的通道，如果价格突破
    上轨则做多，突破下轨则做空。
    """
    upper = df['high'].rolling(n, min_periods=1).mean()
    lower = df['low'].rolling(n, min_periods=1).mean()
    width = upper - lower
    df[f'PAC_bh_{n}'] = width / width.shift(n) - 1
    df[f'PAC_bh_{n}'] = df[f'PAC_bh_{n}'].shift(1)

    return [f'PAC_bh_{n}', ]