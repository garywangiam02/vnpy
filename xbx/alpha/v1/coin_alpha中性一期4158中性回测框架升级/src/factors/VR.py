#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['VR', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # VR 指标
    """
    N=40
    AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
    BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
    CV=IF(CLOSE=REF(CLOSE,1),VOLUME,0)
    AVS=SUM(AV,N)
    BVS=SUM(BV,N)
    CVS=SUM(CV,N)
    VR=(AVS+CVS/2)/(BVS+CVS/2)
    
    VR 用过去 N 日股价上升日成交量与下降日成交量的比值来衡量多空
    力量对比。当 VR 小于 70 时，表示市场较为低迷；上穿 70 时表示市
    场可能有好转；上穿 250 时表示多方力量压倒空方力量。当 VR>300
    时，市场可能过热、买方力量过强，下穿 300 表明市场可能要反转。
    如果 VR 上穿 250，则产生买入信号；
    如果 VR 下穿 300，则产生卖出信号。
    """
    df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0)
    df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0)
    df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0)
    df['AVS'] = df['AV'].rolling(n).sum()
    df['BVS'] = df['BV'].rolling(n).sum()
    df['CVS'] = df['CV'].rolling(n).sum()
    df['VR'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2)
    df[f'VR_bh_{n}'] = df['VR'].shift(1)
    
    del df['AV']
    del df['BV']
    del df['CV']
    del df['AVS']
    del df['BVS']
    del df['CVS']
    del df['VR']

    return [f'VR_bh_{n}', ]




