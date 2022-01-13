#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['MTM_Mean', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	df[f'MTM_Mean_bh_{n}'] = (df['close']/df['close'].shift(n)-1).rolling(n, min_periods=1).mean()
	df[f'MTM_Mean_bh_{n}'] = df[f'MTM_Mean_bh_{n}'].shift(1)

	return [f'MTM_Mean_bh_{n}', ]


