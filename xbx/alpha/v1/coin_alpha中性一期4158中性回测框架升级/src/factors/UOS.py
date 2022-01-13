#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['UOS', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # UOS 指标
    """
    M=7
    N=14
    O=28
    TH=MAX(HIGH,REF(CLOSE,1))
    TL=MIN(LOW,REF(CLOSE,1))
    TR=TH-TL
    XR=CLOSE-TL
    XRM=SUM(XR,M)/SUM(TR,M)
    XRN=SUM(XR,N)/SUM(TR,N)
    XRO=SUM(XR,O)/SUM(TR,O)
    UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
    UOS 的用法与 RSI 指标类似，可以用来反映市场的超买超卖状态。
    一般来说，UOS 低于 30 市场处于超卖状态；UOS 高于 30 市场处于
    超买状态。
    如果 UOS 上穿 30，则产生买入信号；
    如果 UOS 下穿 70，则产生卖出信号。
    """
    M = n
    N = 2 * n
    O = 4 * n
    df['ref_close'] = df['close'].shift(1)
    df['TH'] = df[['high', 'ref_close']].max(axis=1)
    df['TL'] = df[['low', 'ref_close']].min(axis=1)
    df['TR'] = df['TH'] - df['TL']
    df['XR'] = df['close'] - df['TL']
    df['XRM'] = df['XR'].rolling(M).sum() / df['TR'].rolling(M).sum()
    df['XRN'] = df['XR'].rolling(N).sum() / df['TR'].rolling(N).sum()
    df['XRO'] = df['XR'].rolling(O).sum() / df['TR'].rolling(O).sum()
    df['UOS'] = 100 * (df['XRM'] * N * O + df['XRN'] * M * O + df['XRO'] * M * N) / (M * N + M * O + N * O)
    df[f'UOS_bh_{n}'] = df['UOS'].shift(1)
    
    del df['ref_close']
    del df['TH']
    del df['TL']
    del df['TR']
    del df['XR']
    del df['XRM']
    del df['XRN']
    del df['XRO']
    del df['UOS']

    return [f'UOS_bh_{n}', ]





    


