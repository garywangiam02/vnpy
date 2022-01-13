#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['量价相关系数', ]


def signal(*args):
	df = args[0]
	n  = args[1]
	df[f'量价相关系数_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume']).shift(1)

	return [f'量价相关系数_bh_{n}', ]