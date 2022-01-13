#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['Bolling', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	df['median'] = df['close'].rolling(n, min_periods=1).mean()
	df['std']    = df['close'].rolling(n, min_periods=1).std(ddof=0)

	df['up'] = df['median'] + df['std']
	df['dn'] = df['median'] - df['std']

	condition_0 = (df['close'] <= df['up']) & (df['close'] >= df['dn'])
	condition_1 = df['close'] > df['up']
	condition_2 = df['close'] < df['dn']
	df.loc[condition_0, 'distance'] = 0
	df.loc[condition_1, 'distance'] = df['close'] - df['up']
	df.loc[condition_2, 'distance'] = df['close'] - df['dn']

	df[f'Bolling_bh_{n}'] = (df['distance']/df['std']).pct_change(n).shift(1)

	return [f'Bolling_bh_{n}', ]


