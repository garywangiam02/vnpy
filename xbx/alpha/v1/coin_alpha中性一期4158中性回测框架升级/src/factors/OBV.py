#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['OBV', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # OBV 指标
    """
    N1=10
    N2=30
    VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
    VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
    OBV=REF(OBV,1)+VOL
    OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
    OBV 指标把成交量分为正的成交量（价格上升时的成交量）和负的
    成交量（价格下降时）的成交量。OBV 就是分了正负之后的成交量
    的累计和。如果 OBV 和价格的均线一起上涨（下跌），则上涨（下
    跌）趋势被确认。如果 OBV 上升（下降）而价格的均线下降（上升），
    说明价格可能要反转，可能要开始新的下跌（上涨）行情。
    如果 OBV_HISTOGRAM 上穿 0 则买入，下穿 0 则卖出。
    """
    df['VOL'] = np.where(df['close'] > df['close'].shift(1), df['volume'], -df['volume'])
    df['VOL'] = np.where(df['close'] != df['close'].shift(1), df['VOL'], 0)

    df['VOL1']        = df['VOL']
    df['OBV_SIGNAL']  = df['VOL'] + df['VOL1'].shift(1)
    df[f'OBV_bh_{n}'] = df['OBV_SIGNAL'].ewm(n, adjust=False).mean() - df['OBV_SIGNAL'].ewm(3 * n, adjust=False).mean()
    df[f'OBV_bh_{n}'] = df[f'OBV_bh_{n}'].shift(1)
    
    del df['VOL']
    del df['VOL1']
    del df['OBV_SIGNAL']

    return [f'OBV_bh_{n}', ]







