#!/usr/bin/python3
# -*- coding: utf-8 -*-

import talib as ta
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

factors = ['REG', ]


def signal(*args):
    # REG 指标
    """
    N=40
    X=[1,2,...,N]
    Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
    做回归得 REG_CLOSE=aX+b
    REG=(CLOSE-REG_CLOSE)/REG_CLOSE
    在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
    价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
    出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
    """
    # df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
    # df['reg'] = df['close'] / df['ref_close'] - 1
    df = args[0]
    n  = args[1]

    # sklearn 线性回归
    def reg_ols(_y):
        _x = np.arange(n) + 1
        model = LinearRegression().fit(_x.reshape(-1, 1), _y) # 线性回归训练
        y_pred = model.coef_ * _x + model.intercept_ # y = ax + b
        return y_pred[-1]
    
    #df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y))
    df['reg_close'] = ta.LINEARREG(df['close'], timeperiod=n)

    df['reg'] = df['close'] / df['reg_close'] - 1


    df[f'REG_bh_{n}'] = df['reg'].shift(1)
    del df['reg']
    del df['reg_close']

    return [f'REG_bh_{n}', ]




    

    