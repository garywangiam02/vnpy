#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['成交笔数', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	df[f'成交笔数_bh_{n}'] = df['trade_num'].rolling(n, min_periods=1).sum() 
	df[f'成交笔数_bh_{n}'] = df[f'成交笔数_bh_{n}'].shift(1)

	return [f'成交笔数_bh_{n}', ]


