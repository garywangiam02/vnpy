#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['CR', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # CR 指标
    """
    N=20
    TYP=(HIGH+LOW+CLOSE)/3
    H=MAX(HIGH-REF(TYP,1),0)
    L=MAX(REF(TYP,1)-LOW,0)
    CR=SUM(H,N)/SUM(L,N)*100
    CR 与 AR、BR 类似。CR 通过比较最高价、最低价和典型价格来衡
    量市场人气，其衡量昨日典型价格在今日最高价、最低价之间的位置。
    CR 超过 200 时，表示股价上升强势；CR 低于 50 时，表示股价下跌
    强势。如果 CR 上穿 200/下穿 50 则产生买入/卖出信号。
    """
    df['TYP']   = (df['high'] + df['low'] + df['close']) / 3
    df['H_TYP'] = df['high'] - df['TYP'].shift(1)
    df['H']     = np.where(df['high'] > df['TYP'].shift(1), df['H_TYP'], 0)
    df['L_TYP'] = df['TYP'].shift(1) - df['low']
    df['L']     = np.where(df['TYP'].shift(1) > df['low'], df['L_TYP'], 0)
    df['CR']    = df['H'].rolling(n).sum() / df['L'].rolling(n).sum() * 100
    
    df[f'CR_bh_{n}'] = df['CR'].shift(1)
    
    del df['TYP']
    del df['H_TYP']
    del df['H']
    del df['L_TYP']
    del df['L']
    del df['CR']

    return [f'CR_bh_{n}', ]