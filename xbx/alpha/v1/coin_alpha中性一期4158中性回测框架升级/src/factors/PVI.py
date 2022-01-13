#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['PVI', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # PVI 指标
    """
    N=40
    PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/
    CLOSE,0)
    PVI=CUM_SUM(PVI_INC)
    PVI_MA=MA(PVI,N)
    PVI 是成交量升高的交易日的价格变化百分比的累积。
    PVI 相关理论认为，如果当前价涨量增，则说明散户主导市场，PVI
    可以用来识别价涨量增的市场（散户主导的市场）。
    如果 PVI 上穿 PVI_MA，则产生买入信号；
    如果 PVI 下穿 PVI_MA，则产生卖出信号。
    """
    df['ref_close'] = (df['close'] - df['close'].shift(1)) / df['close']
    df['PVI_INC'] = np.where(df['volume'] > df['volume'].shift(1), df['ref_close'], 0)
    df['PVI'] = df['PVI_INC'].cumsum()
    df['PVI_INC_MA'] = df['PVI'].rolling(n, min_periods=1).mean()

    df[f'PVI_bh_{n}'] = df['PVI_INC_MA'].shift(1)
    
    del df['ref_close']
    del df['PVI_INC']
    del df['PVI']
    del df['PVI_INC_MA']

    return [f'PVI_bh_{n}', ]