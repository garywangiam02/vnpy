#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['VRAMT', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # VRAMT 指标
    """
    N=40
    AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
    BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
    CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
    AVS=SUM(AV,N)
    BVS=SUM(BV,N)
    CVS=SUM(CV,N)
    VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
    VRAMT 的计算与 VR 指标（Volume Ratio）一样，只是把其中的成
    交量替换成了成交额。
    如果 VRAMT 上穿 180，则产生买入信号；
    如果 VRAMT 下穿 70，则产生卖出信号。
    """
    df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0)
    df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0)
    df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0)
    df['AVS'] = df['AV'].rolling(n).sum()
    df['BVS'] = df['BV'].rolling(n).sum()
    df['CVS'] = df['CV'].rolling(n).sum()
    df['VRAMT'] = (df['AVS'] + df['CVS'] / 2 ) / (df['BVS'] + df['CVS'] / 2)
    df[f'VRAMT_bh_{n}'] = df['VRAMT'].shift(1)
    
    del df['AV']
    del df['BV']
    del df['CV']
    del df['AVS']
    del df['BVS']
    del df['CVS']
    del df['VRAMT']

    return [f'VRAMT_bh_{n}', ]





