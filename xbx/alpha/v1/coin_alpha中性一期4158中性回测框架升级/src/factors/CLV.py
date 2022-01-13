#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['CLV', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # CLV 指标
    """
    N=60
    CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
    CLVMA=MA(CLV,N)
    CLV 用来衡量收盘价在最低价和最高价之间的位置。当
    CLOSE=HIGH 时，CLV=1;当 CLOSE=LOW 时，CLV=-1;当 CLOSE
    位于 HIGH 和 LOW 的中点时，CLV=0。CLV>0（<0），说明收盘价
    离最高（低）价更近。我们用 CLVMA 上穿/下穿 0 来产生买入/卖出
    信号。
    """
    df['CLV'] = (2 * df['close'] - df['low'] - df['high']) / (df['high'] - df['low'])
    df['CLVMA'] = df['CLV'].rolling(n, min_periods=1).mean()
    df[f'CLV_bh_{n}'] = df['CLVMA'].shift(1)
    
    del df['CLV']
    del df['CLVMA']  

    return [f'CLV_bh_{n}', ]