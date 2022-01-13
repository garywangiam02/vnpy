#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['Qstick', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # Qstick 指标
    """
    N=20
    Qstick=MA(CLOSE-OPEN,N)
    Qstick 通过比较收盘价与开盘价来反映股价趋势的方向和强度。如果
    Qstick 上穿/下穿 0 则产生买入/卖出信号。
    """
    cl = df['close'] - df['open']
    Qstick = cl.rolling(n, min_periods=1).mean()
    # 进行无量纲处理
    df[f'Qstick_bh_{n}'] = cl / Qstick - 1
    df[f'Qstick_bh_{n}'] = df[f'Qstick_bh_{n}'].shift(1)

    return [f'Qstick_bh_{n}', ]