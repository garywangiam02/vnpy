#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['WAD', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    #  WAD 指标
    """
    TRH=MAX(HIGH,REF(CLOSE,1))
    TRL=MIN(LOW,REF(CLOSE,1))
    AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH) 
    AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))  # 该指标怀疑有误
    WAD=CUMSUM(AD)
    N=20
    WADMA=MA(WAD,N)
    我们用 WAD 上穿/下穿其均线来产生买入/卖出信号。
    """
    df['ref_close'] = df['close'].shift(1)
    df['TRH']   = df[['high', 'ref_close']].max(axis=1)
    df['TRL']   = df[['low', 'ref_close']].min(axis=1)
    df['AD']    = np.where(df['close'] > df['close'].shift(1), df['close'] - df['TRL'], df['close'] - df['TRH'])
    df['AD']    = np.where(df['close'] > df['close'].shift(1), 0, df['close'] - df['close'].shift(1))
    df['WAD']   = df['AD'].cumsum()
    df['WADMA'] = df['WAD'].rolling(n, min_periods=1).mean()
    # 去量纲
    df[f'WAD_bh_{n}'] = df['WAD'] / df['WADMA']
    df[f'WAD_bh_{n}'] = df[f'WAD_bh_{n}'].shift(1)
    
    del df['ref_close']
    del df['TRH']
    del df['TRL']
    del df['AD']
    del df['WAD']
    del df['WADMA'] 

    return [f'WAD_bh_{n}', ]









