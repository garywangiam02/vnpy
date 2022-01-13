#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ARBR_AR', 'ARBR_BR', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # ARBR指标
    """
    AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
    BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100
    AR 衡量开盘价在最高价、最低价之间的位置；BR 衡量昨日收盘价在
    今日最高价、最低价之间的位置。AR 为人气指标，用来计算多空双
    方的力量对比。当 AR 值偏低（低于 50）时表示人气非常低迷，股价
    很低，若从 50 下方上穿 50，则说明股价未来可能要上升，低点买入。
    当 AR 值下穿 200 时卖出。
    """
    df['HO'] = df['high'] - df['open']
    df['OL'] = df['open'] - df['low']
    df['AR'] = df['HO'].rolling(n).sum() / df['OL'].rolling(n).sum() * 100
    df['HC'] = df['high'] - df['close'].shift(1)
    df['CL'] = df['close'].shift(1) - df['low']
    df['BR'] = df['HC'].rolling(n).sum() / df['CL'].rolling(n).sum() * 100

    df[f'ARBR_AR_bh_{n}'] = df['AR'].shift(1)
    df[f'ARBR_BR_bh_{n}'] = df['BR'].shift(1)

    del df['HO']
    del df['OL']
    del df['AR']
    del df['HC']
    del df['CL']
    del df['BR']

    return [f'ARBR_AR_bh_{n}', f'ARBR_BR_bh_{n}']



