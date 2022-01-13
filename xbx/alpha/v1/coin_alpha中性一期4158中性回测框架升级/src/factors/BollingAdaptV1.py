#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

factors = ['BollingAdaptV1_UP', 'BollingAdaptV1_DN', ]


def signal(*args):
	df = args[0]
	n  = args[1]

	# 计算动量因子
	mtm = df['close'] / df['close'].shift(n) - 1
	mtm_mean = mtm.rolling(window=n, min_periods=1).mean()

	# 基于价格atr，计算波动率因子wd_atr
	c1 = df['high'] - df['low']
	c2 = abs(df['high'] - df['close'].shift(1))
	c3 = abs(df['low'] - df['close'].shift(1))
	tr = np.max(np.array([c1, c2, c3]), axis=0)  # 三个数列取其大值
	atr = pd.Series(tr).rolling(window=n, min_periods=1).mean()
	avg_price = df['close'].rolling(window=n, min_periods=1).mean()
	wd_atr = atr / avg_price  # === 波动率因子

	# 参考ATR，对MTM指标，计算波动率因子
	mtm_l = df['low'] / df['low'].shift(n) - 1
	mtm_h = df['high'] / df['high'].shift(n) - 1
	mtm_c = df['close'] / df['close'].shift(n) - 1
	mtm_c1 = mtm_h - mtm_l
	mtm_c2 = abs(mtm_h - mtm_c.shift(1))
	mtm_c3 = abs(mtm_l - mtm_c.shift(1))
	mtm_tr = np.max(np.array([mtm_c1, mtm_c2, mtm_c3]), axis=0)  # 三个数列取其大值
	mtm_atr = pd.Series(mtm_tr).rolling(window=n, min_periods=1).mean()  # === mtm 波动率因子

	# 参考ATR，对MTM mean指标，计算波动率因子
	mtm_l_mean = mtm_l.rolling(window=n, min_periods=1).mean()
	mtm_h_mean = mtm_h.rolling(window=n, min_periods=1).mean()
	mtm_c_mean = mtm_c.rolling(window=n, min_periods=1).mean()
	mtm_c1 = mtm_h_mean - mtm_l_mean
	mtm_c2 = abs(mtm_h_mean - mtm_c_mean.shift(1))
	mtm_c3 = abs(mtm_l_mean - mtm_c_mean.shift(1))
	mtm_tr = np.max(np.array([mtm_c1, mtm_c2, mtm_c3]), axis=0)  # 三个数列取其大值
	mtm_atr_mean = pd.Series(mtm_tr).rolling(window=n, min_periods=1).mean()  # === mtm_mean 波动率因子

	indicator = mtm_mean
	# mtm_mean指标分别乘以三个波动率因子
	indicator *= mtm_atr * mtm_atr_mean * wd_atr
	indicator = pd.Series(indicator)

	# 对新策略因子计算自适应布林
	median = indicator.rolling(window=n).mean()
	std = indicator.rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
	z_score = abs(indicator - median) / std
	m = pd.Series(z_score).rolling(window=n).max().shift(1)
	up = median + std * m
	dn = median - std * m

	df[f'BollingAdaptV1_UP_bh_{n}'] = 1e8 * (indicator - up)  # 上轨因子
	df[f'BollingAdaptV1_DN_bh_{n}'] = 1e8 * (indicator - dn)  # 下轨因子

	df[f'BollingAdaptV1_UP_bh_{n}'] = df[f'BollingAdaptV1_UP_bh_{n}'].shift(1)
	df[f'BollingAdaptV1_DN_bh_{n}'] = df[f'BollingAdaptV1_DN_bh_{n}'].shift(1)

	return [f'BollingAdaptV1_UP_bh_{n}', f'BollingAdaptV1_DN_bh_{n}', ]










