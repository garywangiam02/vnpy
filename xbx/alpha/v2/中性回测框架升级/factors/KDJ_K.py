#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
	# KDJ_K
	df = args[0]
	n  = args[1]
	diff_num = args[2]
	factor_name = args[3]

	low_list  = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
	high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
	rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
	df[factor_name] = rsv.ewm(com=2).mean()  # K

	if diff_num > 0:
		return add_diff(df, diff_num, factor_name)
	else:
		return df