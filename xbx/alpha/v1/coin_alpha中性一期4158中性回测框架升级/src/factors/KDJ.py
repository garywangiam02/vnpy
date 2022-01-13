#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['K', 'D', 'J']


def signal(*args):
	df = args[0]
	n  = args[1]

	# 正常K线数据 计算 KDJ
	low_list  = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
	high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
	rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
	df[f'K_bh_{n}'] = rsv.ewm(com=2).mean().shift(1)  # K
	df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D
	df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}']  # J

	return [f'K_bh_{n}', f'D_bh_{n}', f'J_bh_{n}']


