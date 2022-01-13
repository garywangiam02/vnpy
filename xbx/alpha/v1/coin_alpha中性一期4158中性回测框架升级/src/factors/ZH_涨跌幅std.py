#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['涨跌幅std', ]


def signal(*args):
	df = args[0]
	n  = args[1]
	# 涨跌幅std，振幅的另外一种形式
	change = df['close'].pct_change()

	df[f'涨跌幅std_bh_{n}'] = change.rolling(n).std()
	df[f'涨跌幅std_bh_{n}'] = df[f'涨跌幅std_bh_{n}'].shift(1)

	return [f'涨跌幅std_bh_{n}', ]