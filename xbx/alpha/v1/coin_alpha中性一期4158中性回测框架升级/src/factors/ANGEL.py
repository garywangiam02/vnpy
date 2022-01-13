#!/usr/bin/python3
# -*- coding: utf-8 -*-

import talib
import numpy as np
import pandas as pd

factors = ['ANGEL', ]


def signal(*args):
	df = args[0]
	n  = args[1]
	try:
		ma = df['close'].rolling(window=n).mean()
		df[f'ANGEL_bh_{n}'] = talib.LINEARREG_ANGLE(ma, n)
		df[f'ANGEL_bh_{n}'] = df[f'ANGEL_bh_{n}'].shift(1)
	except Exception as ex:
		df[f'ANGEL_bh_{n}'] = 0

	return [f'ANGEL_bh_{n}', ]