#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['KE', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	volume_avg       = df['volume'].rolling(n).mean()
	volume_stander   = df['volume']/volume_avg
	price_change     = df['close'].pct_change(n)
	df[f'KE_bh_{n}'] = (price_change/abs(price_change)) * volume_stander*price_change**2
	df[f'KE_bh_{n}'] = df[f'KE_bh_{n}'].shift(1)

	return [f'KE_bh_{n}', ]


