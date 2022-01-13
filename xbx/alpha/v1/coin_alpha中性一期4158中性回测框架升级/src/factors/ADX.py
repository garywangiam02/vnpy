#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ADX', 'ADX_DI+', 'ADX_DI-', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # ADX 指标
    """
    N1=14
    MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
    MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
    XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
    PDM=SUM(XPDM,N1)
    XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
    NDM=SUM(XNDM,N1)
    TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
    TR=SUM(TR,N1)
    DI+=PDM/TR
    DI-=NDM/TR
    ADX 指标计算过程中的 DI+与 DI-指标用相邻两天的最高价之差与最
    低价之差来反映价格的变化趋势。当 DI+上穿 DI-时，产生买入信号；
    当 DI+下穿 DI-时，产生卖出信号。
    """
    df['max_high'] = np.where(df['high'] > df['high'].shift(1), df['high'] - df['high'].shift(1), 0)
    df['max_low'] = np.where(df['low'].shift(1) > df['low'], df['low'].shift(1) - df['low'], 0)
    df['XPDM'] = np.where(df['max_high'] > df['max_low'], df['high'] - df['high'].shift(1), 0)
    df['PDM'] = df['XPDM'].rolling(n).sum()
    df['XNDM'] = np.where(df['max_low'] > df['max_high'], df['low'].shift(1) - df['low'], 0)
    df['NDM'] = df['XNDM'].rolling(n).sum()
    df['c1'] = abs(df['high'] - df['low'])
    df['c2'] = abs(df['high'] - df['close'])
    df['c3'] = abs(df['low'] - df['close'])
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['TR_sum'] = df['TR'].rolling(n).sum()
    df['DI+'] = df['PDM'] / df['TR']
    df['DI-'] = df['NDM'] / df['TR']

    df[f'ADX_DI+_bh_{n}'] = df['DI+'].shift(1)
    df[f'ADX_DI-_bh_{n}'] = df['DI-'].shift(1)

    df['ADX'] = (df['PDM'] + df['NDM']) / df['TR']

    df[f'ADX_bh_{n}'] = df['ADX'].shift(1)

    del df['max_high']
    del df['max_low']
    del df['XPDM']
    del df['PDM']
    del df['XNDM']
    del df['NDM']
    del df['c1']
    del df['c2']
    del df['c3']
    del df['TR']
    del df['TR_sum']
    del df['DI+']
    del df['DI-']
    del df['ADX']

    return [f'ADX_bh_{n}', f'ADX_DI+_bh_{n}', f'ADX_DI-_bh_{n}']



