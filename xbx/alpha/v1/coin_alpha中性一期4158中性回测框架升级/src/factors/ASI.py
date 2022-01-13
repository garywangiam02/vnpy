#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ASI', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # ASI 指标
    """
    A=ABS(HIGH-REF(CLOSE,1))
    B=ABS(LOW-REF(CLOSE,1))
    C=ABS(HIGH-REF(LOW,1))
    D=ABS(REF(CLOSE,1)-REF(OPEN,1))
    N=20
    K=MAX(A,B)
    M=MAX(HIGH-LOW,N)
    R1=A+0.5*B+0.25*D
    R2=B+0.5*A+0.25*D
    R3=C+0.25*D
    R4=IF((A>=B) & (A>=C),R1,R2)
    R=IF((C>=A) & (C>=B),R3,R4)
    SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
    M=20
    ASI=CUMSUM(SI)
    ASIMA=MA(ASI,M)
    由于 SI 的波动性比较大，所以我们一般对 SI 累计求和得到 ASI 并捕
    捉 ASI 的变化趋势。一般我们不会直接看 ASI 的数值（对 SI 累计求
    和的求和起点不同会导致求出 ASI 的值不同），而是会观察 ASI 的变
    化方向。我们利用 ASI 与其均线的交叉来产生交易信号,上穿/下穿均
    线时买入/卖出。
    """
    df['A'] = abs(df['high'] - df['close'].shift(1))
    df['B'] = abs(df['low']  - df['close'].shift(1))
    df['C'] = abs(df['high'] - df['low'].shift(1))
    df['D'] = abs(df['close'].shift(1) - df['open'].shift(1))
    df['K'] = df[['A', 'B']].max(axis=1)

    df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D']
    df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D']
    df['R3'] = df['C'] + 0.25 * df['D']
    df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'], df['R2'])
    df['R'] = np.where((df['C'] > df['A']) & (df['C'] >= df['B']), df['R3'], df['R4'])
    df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                     0.5 * (df['close'] - df['open'])) / df['R'] * df['K'] / n

    df['ASI'] = df['SI'].cumsum()
    df['ASI_MA'] = df['ASI'].rolling(n, min_periods=1).mean()

    df[f'ASI_bh_{n}'] = df['ASI'] / df['ASI_MA'] - 1
    df[f'ASI_bh_{n}'] = df[f'ASI_bh_{n}'].shift(1)
    

    del df['A']
    del df['B']
    del df['C']
    del df['D']
    del df['K']
    del df['R1']
    del df['R2']
    del df['R3']
    del df['R4']
    del df['R']
    del df['SI']
    del df['ASI']
    del df['ASI_MA']

    return [f'ASI_bh_{n}', ]




    