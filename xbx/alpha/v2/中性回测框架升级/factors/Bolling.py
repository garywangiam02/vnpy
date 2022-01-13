#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
	# Bolling_V2
	df = args[0]
	n  = args[1]
	diff_num = args[2]
	factor_name = args[3]

	median  = df['close'].rolling(n, min_periods=1).mean()
	std     = df['close'].rolling(n, min_periods=1).std(ddof=0)
	z_score = abs(df['close'] - median) / std
	m       = z_score.rolling(n, min_periods=1).max().shift(1)

	up = median + std * m
	dn = median - std * m

	df[factor_name] = (df['close'] - dn)/(up - dn)

	if diff_num > 0:
		return add_diff(df, diff_num, factor_name)
	else:
		return df


