#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['KEV2', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	volume_avg       = df['volume'].rolling(n).mean()
	mass_re   		 = df['volume']/volume_avg
	price_change     = df['close'].pct_change(n)
	velocity         = price_change / n

	df[f'KEV2_bh_{n}'] = (price_change / abs(price_change)) * mass_re * velocity ** 2
	df[f'KEV2_bh_{n}'] = df[f'KEV2_bh_{n}'].shift(1)

	return [f'KEV2_bh_{n}', ]


