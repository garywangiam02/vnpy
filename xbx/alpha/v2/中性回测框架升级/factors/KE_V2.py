#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
	# KE_V2
	df = args[0]
	n  = args[1]
	diff_num = args[2]
	factor_name = args[3]

	volume_avg       = df['volume'].rolling(n).mean()
	mass_re   		 = df['volume']/volume_avg
	#price_change     = df['close'].pct_change(n)
	price_change	 = df['close']/df['close'].shift(n) - 1

	velocity         = price_change / n

	df[factor_name] = (price_change / abs(price_change)) * mass_re * velocity ** 2

	if diff_num > 0:
		return add_diff(df, diff_num, factor_name)
	else:
		return df