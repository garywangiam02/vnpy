#!/usr/bin/python3
# -*- coding: utf-8 -*-

import talib
import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
    # TRRQ
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    tp = (df['high'] + df['low'] + df['close']) / 3
    #tp.fillna(method='ffill', inplace=True)
    nom_qv      = df['quote_volume'] / df['quote_volume'].rolling(n).mean()
    reg_price   = talib.LINEARREG(tp, timeperiod=n)
    #tp_reg_pctc = reg_price.pct_change(n) 
    tp_reg_pctc = reg_price/reg_price.shift(n) - 1

    df[factor_name] = (tp_reg_pctc / nom_qv).rolling(n).mean() 

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df