#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['DMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # DMA 指标
    """
    DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
    AMA=MA(DMA,N1)
    DMA 衡量快速移动平均与慢速移动平均之差。用 DMA 上穿/下穿其
    均线产生买入/卖出信号。
    """
    df['ma1'] = df['close'].rolling(n, min_periods=1).mean()
    df['ma2'] = df['close'].rolling(n * 2, min_periods=1).mean()
    df['DMA'] = df['ma1'] - df['ma2']
    df['AMA'] = df['DMA'].rolling(n, min_periods=1).mean()
    # 去量纲
    df[f'DMA_bh_{n}'] = df['DMA'] / df['AMA']
    df[f'DMA_bh_{n}'] = df[f'DMA_bh_{n}'].shift(1)
    
    del df['ma1']
    del df['ma2']
    del df['DMA']
    del df['AMA']

    return [f'DMA_bh_{n}', ]