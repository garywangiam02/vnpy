#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['ROC', ]


def signal(*args):
    # ROC 指标
    """
    ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
    ROC 衡量价格的涨跌幅。ROC 可以用来反映市场的超买超卖状态。
    当 ROC 过高时，市场处于超买状态；当 ROC 过低时，市场处于超
    卖状态。这些情况下，可能会发生反转。
    如果 ROC 上穿 5%，则产生买入信号；
    如果 ROC 下穿-5%，则产生卖出信号。
    """
    df = args[0]
    n  = args[1]

    df['ROC'] = df['close'] / df['close'].shift(n) - 1

    df[f'ROC_bh_{n}'] = df['ROC'].shift(1)
    del df['ROC']

    return [f'ROC_bh_{n}', ]