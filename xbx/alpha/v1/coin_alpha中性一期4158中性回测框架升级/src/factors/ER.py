#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ER_bull', 'ER_bear', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # 计算 ER
    """
    N=20
    BullPower=HIGH-EMA(CLOSE,N)
    BearPower=LOW-EMA(CLOSE,N)
    ER 为动量指标。用来衡量市场的多空力量对比。在多头市场，人们
    会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量
    越强；而在空头市场，人们可能因为恐惧而在接近低价的地方卖出。
    BearPower 越低则当前空头力量越强。当两者都大于 0 时，反映当前
    多头力量占据主导地位；两者都小于0则反映空头力量占据主导地位。
    如果 BearPower 上穿 0，则产生买入信号；
    如果 BullPower 下穿 0，则产生卖出信号。

    """
    ema = df['close'].ewm(n, adjust=False).mean()
    bull_power = df['high'] - ema  # 越高表示上涨
    bear_power = df['low'] - ema # 越低表示下降越厉害
    df[f'ER_bull_bh_{n}'] = bull_power / ema
    df[f'ER_bear_bh_{n}'] = bear_power / ema
    df[f'ER_bull_bh_{n}'] = df[f'ER_bull_bh_{n}'].shift(1)
    df[f'ER_bear_bh_{n}'] = df[f'ER_bear_bh_{n}'].shift(1)

    return [f'ER_bull_bh_{n}', f'ER_bear_bh_{n}']
