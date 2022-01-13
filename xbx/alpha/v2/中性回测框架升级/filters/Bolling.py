#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
	# Bolling
	df = args[0]
	n  = args[1]
	factor_name = args[2]

	median  = df['close'].rolling(n, min_periods=1).mean()
	std     = df['close'].rolling(n, min_periods=1).std(ddof=0)

	up = median + std * 2
	dn = median - std * 2

	condition_0 = (df['close'] <= up) & (df['close'] >= dn)
	condition_1 = df['close'] > up
	condition_2 = df['close'] < dn

	df.loc[condition_0, 'distance'] =  0
	df.loc[condition_1, 'distance'] =  1
	df.loc[condition_2, 'distance'] = -1

	df[factor_name] = df['distance']

	del df['distance']

	return df


