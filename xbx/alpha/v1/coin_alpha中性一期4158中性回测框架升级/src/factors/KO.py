#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['KO', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # KO 指标
    """
    N1=34
    N2=55
    TYPICAL=(HIGH+LOW+CLOSE)/3
    VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
    VOLUME_EMA1=EMA(VOLUME,N1)
    VOLUME_EMA2=EMA(VOLUME,N2)
    KO=VOLUME_EMA1-VOLUME_EMA2
    这个技术指标的目的是为了观察短期和长期股票资金的流入和流出
    的情况。它的主要用途是确认股票价格趋势的方向和强度。KO 与
    OBV,VPT 等指标类似，都是用价格对成交量进行加权。KO 用的是典
    型价格的变化（只考虑变化方向，不考虑变化量），OBV 用的是收
    盘价的变化（只考虑变化方向，不考虑变化量），VPT 用的是价格的
    变化率（即考虑方向又考虑变化幅度）。
    如果 KO 上穿 0，则产生买入信号；
    如果 KO 下穿 0，则产生卖出信号。
    """
    df['price'] = (df['high'] + df['low'] + df['close']) / 3
    df['V'] = np.where(df['price'] > df['price'].shift(1), df['volume'], -df['volume'])
    df['V_ema1'] = df['V'].ewm(n, adjust=False).mean()
    df['V_ema2'] = df['V'].ewm(int( n* 1.618), adjust=False).mean()
    df['KO'] = df['V_ema1'] - df['V_ema2']
    # 标准化
    df[f'KO_bh_{n}'] = (df['KO'] - df['KO'].rolling(n, min_periods=1).min()) / (df['KO'].rolling(n, min_periods=1).max() - df['KO'].rolling(n, min_periods=1).min())
    df[f'KO_bh_{n}'] = df[f'KO_bh_{n}'].shift(1)
    
    del df['price']
    del df['V']
    del df['V_ema1']
    del df['V_ema2']
    del df['KO']

    return [f'KO_bh_{n}']




        