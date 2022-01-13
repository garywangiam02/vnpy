#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['PVT', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # PVT 指标
    """
    PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
    PVT_MA1=MA(PVT,N1)
    PVT_MA2=MA(PVT,N2)
    PVT 指标用价格的变化率作为权重求成交量的移动平均。PVT 指标
    与 OBV 指标的思想类似，但与 OBV 指标相比，PVT 考虑了价格不
    同涨跌幅的影响，而 OBV 只考虑了价格的变化方向。我们这里用 PVT
    短期和长期均线的交叉来产生交易信号。
    如果 PVT_MA1 上穿 PVT_MA2，则产生买入信号；
    如果 PVT_MA1 下穿 PVT_MA2，则产生卖出信号。
    """
    df['PVT'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * df['volume']
    df['PVT_MA1'] = df['PVT'].rolling(n, min_periods=1).mean()
    # df['PVT_MA2'] = df['PVT'].rolling(2 * n, min_periods=1).mean()

    # 去量纲
    df[f'PVT_bh_{n}'] = df['PVT'] / df['PVT_MA1'] - 1
    df[f'PVT_bh_{n}'] = df[f'PVT_bh_{n}'].shift(1)
    
    return [f'PVT_bh_{n}', ]