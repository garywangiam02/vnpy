#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['BIAS', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	ma = df['close'].rolling(n, min_periods=1).mean()
	df[f'BIAS_bh_{n}'] = (df['close'] / ma - 1).shift(1)

	return [f'BIAS_bh_{n}', ]


