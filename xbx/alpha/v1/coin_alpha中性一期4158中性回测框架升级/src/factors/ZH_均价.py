#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['均价', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	# --- 均价 ---  对应低价股策略(预计没什么用)
	# 策略改进思路：以下所有用到收盘价的因子，都可尝试使用均价代替
	df[f'均价_bh_{n}'] = (df['quote_volume'].rolling(n).sum() / df['volume'].rolling(n).sum()).shift(1)
	return [f'均价_bh_{n}', ]



