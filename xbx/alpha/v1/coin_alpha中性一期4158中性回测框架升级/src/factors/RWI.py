#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['RWIH', 'RWIL', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # RWI 指标
    """
    N=14
    TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(
    CLOSE,1)-LOW))
    ATR=MA(TR,N)
    RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
    RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
    RWI（随机漫步指标）对一段时间股票的随机漫步区间与真实运动区
    间进行比较以判断股票价格的走势。
    如果 RWIH>1，说明股价长期是上涨趋势，则产生买入信号；
    如果 RWIL>1，说明股价长期是下跌趋势，则产生卖出信号。
    """
    df['c1'] = abs(df['high'] - df['low'])
    df['c2'] = abs(df['close'] - df['close'].shift(1))
    df['c3'] = abs(df['high'] - df['close'].shift(1))
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()
    df['RWIH'] = (df['high'] - df['low'].shift(1)) / (df['ATR'] * np.sqrt(n))
    df['RWIL'] = (df['high'].shift(1) - df['low']) / (df['ATR'] * np.sqrt(n))
    df[f'RWIH_bh_{n}'] = df['RWIH'].shift(1)
    df[f'RWIL_bh_{n}'] = df['RWIL'].shift(1)

    del df['c1']
    del df['c2']
    del df['c3']
    del df['TR']
    del df['ATR']
    del df['RWIH']
    del df['RWIL']

    return [f'RWIH_bh_{n}', f'RWIL_bh_{n}', ]



