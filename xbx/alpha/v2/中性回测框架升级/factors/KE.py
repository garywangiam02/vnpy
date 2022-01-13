#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
	# KE
	df = args[0]
	n  = args[1]
	diff_num = args[2]
	factor_name = args[3]

	volume_avg       = df['volume'].rolling(n).mean()
	volume_stander   = df['volume']/volume_avg
	#price_change     = df['close'].pct_change(n)
	price_change	 = df['close']/df['close'].shift(n) - 1


	df[factor_name]  = (price_change/abs(price_change)) * volume_stander*price_change**2

	if diff_num > 0:
		return add_diff(df, diff_num, factor_name)
	else:
		return df