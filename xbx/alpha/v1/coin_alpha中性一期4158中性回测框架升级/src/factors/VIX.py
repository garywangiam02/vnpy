#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['VIX', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	df['vix1'] = df['close'] / df['close'].shift(n) - 1
	df['up']   = df['vix1'].rolling(window=n).max().shift(1)
	df[f'VIX_bh_{n}'] = (df['vix1'] - df['up']).shift(1)

	del df['vix1']
	del df['up']
	
	return [f'VIX_bh_{n}', ]


