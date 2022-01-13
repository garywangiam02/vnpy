#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['FI', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # FI 指标
    """
    N=13
    FI=(CLOSE-REF(CLOSE,1))*VOLUME
    FIMA=EMA(FI,N)
    FI 用价格的变化来衡量价格的趋势，用成交量大小来衡量趋势的强
    弱。我们先对 FI 取移动平均，当均线上穿 0 线时产生买入信号，下
    穿 0 线时产生卖出信号。
    """
    df['FI'] = (df['close'] - df['close'].shift(1)) * df['volume']
    df['FIMA'] = df['FI'].ewm(n, adjust=False).mean()
    # 去量纲
    df[f'FI_bh_{n}'] = df['FI'] / df['FIMA'] - 1
    df[f'FI_bh_{n}'] = df[f'FI_bh_{n}'].shift(1)
    
    del df['FI']
    del df['FIMA']

    return [f'FI_bh_{n}', ]