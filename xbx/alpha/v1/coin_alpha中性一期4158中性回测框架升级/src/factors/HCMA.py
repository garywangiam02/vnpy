#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['HCMA', ]


def signal(*args):
    df = args[0]
    n  = args[1]

    high  = df['high'].rolling(n, min_periods=1).mean()
    close = df['close']
    df[f'HCMA_bh_{n}'] = (close - high) / high

    # 去量纲
    df[f'HCMA_bh_{n}'] = df[f'HCMA_bh_{n}'].shift(1)
    
    return [f'HCMA_bh_{n}', ]