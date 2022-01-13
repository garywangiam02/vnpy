#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['振幅2', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	high = df[['close', 'open']].max(axis=1)
	low = df[['close', 'open']].min(axis=1)
	high = high.rolling(n, min_periods=1).max()
	low = low.rolling(n, min_periods=1).min()
	df[f'振幅2_bh_{n}'] = (high / low - 1).shift(1)

	return [f'振幅2_bh_{n}', ]


