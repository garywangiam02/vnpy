#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['RSIH', ]


def signal(*args):
    df = args[0]
    n  = args[1]
    # RSIH
    """
    N1=40
    N2=120
    CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
    OSE,1),0)
    RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLO
    SE,1)),N1,1)*100
    RSI_SIGNAL=EMA(RSI,N2)
    RSIH=RSI-RSI_SIGNAL
    RSI 指标的一个缺点波动性太大，为了使其更平滑我们可以对其作移
    动平均处理。类似于由 MACD 产生 MACD_SIGNAL 并取其差得到
    MACD_HISTOGRAM，我们对 RSI 作移动平均得到 RSI_SIGNAL，
    取两者的差得到 RSI HISTOGRAM。当 RSI HISTORGRAM 上穿 0
    时产生买入信号；当 RSI HISTORGRAM 下穿 0 产生卖出信号。
    """
    df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
    sma_diff_pos = df['close_diff_pos'].rolling(n, min_periods=1).mean()
    abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).rolling(n, min_periods=1).mean()
    df['RSI'] = sma_diff_pos / abs_sma_diff_pos * 100
    df['RSI_ema'] = df['RSI'].ewm(4 * n, adjust=False).mean()
    df['RSIH'] = df['RSI'] - df['RSI_ema']

    df[f'RSIH_bh_{n}'] = df['RSIH'].shift(1)
    
    del df['close_diff_pos']
    del df['RSI']
    del df['RSI_ema']
    del df['RSIH']

    return [f'RSIH_bh_{n}', ]






