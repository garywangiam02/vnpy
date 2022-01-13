#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['DDI', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # DDI 指标
    """
    n = 40
    HL=HIGH+LOW
    HIGH_ABS=ABS(HIGH-REF(HIGH,1))
    LOW_ABS=ABS(LOW-REF(LOW,1))
    DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
    DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
    DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
    DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
    DDI=DIZ-DIF
    DDI 指标用来比较向上波动和向下波动的比例。如果 DDI 上穿/下穿 0
    则产生买入/卖出信号。
    """
    df['hl'] = df['high'] + df['low']
    df['abs_high'] = abs(df['high'] - df['high'].shift(1))
    df['abs_low'] = abs(df['low'] - df['low'].shift(1))
    max_value1 = df[['abs_high', 'abs_low']].max()
    df.loc[df['hl'] > df['hl'].shift(1), 'DMZ'] = max_value1
    df['DMZ'].fillna(value=0, inplace=True)
    df.loc[df['hl'] < df['hl'].shift(1), 'DMF'] = max_value1
    df['DMF'].fillna(value=0, inplace=True)

    DMZ_SUM = df['DMZ'].rolling(n).sum()
    DMF_SUM = df['DMF'].rolling(n).sum()
    DIZ = DMZ_SUM / (DMZ_SUM + DMF_SUM)
    DIF = DMF_SUM / (DMZ_SUM + DMF_SUM)
    df[f'DDI_bh_{n}'] = DIZ - DIF
    df[f'DDI_bh_{n}'] = df[f'DDI_bh_{n}'].shift(1)
    
    del df['hl']
    del df['abs_high']
    del df['abs_low']
    del df['DMZ']
    del df['DMF']

    return [f'DDI_bh_{n}', ]



    