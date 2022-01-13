#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['资金流入比例', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	volume = df['quote_volume'].rolling(n, min_periods=1).sum()
	buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
	df[f'资金流入比例_bh_{n}'] = (buy_volume / volume).shift(1)

	return [f'资金流入比例_bh_{n}', ]


