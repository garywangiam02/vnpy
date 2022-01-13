#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['PSY', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # PSY 指标
    """
    N=12
    PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
    PSY 指标为过去 N 天股价上涨的天数的比例*100，用来衡量投资者
    心理和市场的人气。当 PSY 处于 40 和 60 之间时，多、空力量相对
    平衡，当 PSY 上穿 60 时，多头力量比较强，产生买入信号；当 PSY
    下穿 40 时，空头力量比较强，产生卖出信号。
    """
    df['P'] = np.where(df['close'] > df['close'].shift(1), 1, 0)

    df['PSY'] = df['P'] / n * 100
    df[f'PSY_bh_{n}'] = df['PSY'].shift(1)
    
    del df['P']
    del df['PSY']

    return [f'PSY_bh_{n}', ]