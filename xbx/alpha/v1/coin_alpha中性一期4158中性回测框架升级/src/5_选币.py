#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import glob
import itertools
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from config  import data_path
from utils   import ind
from utils.commons import read_factors_async

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows',  100)  # 最多显示数据的行数


# ===参数设定
select_coin_num = 1
c_rate 			= 4/10000
trade_type 		= 'spot'
hold_hour  		= '6H'  # 持币周期
factor_list = [
	# factor, reverse, back_hour, diff_num, weight
	('BIAS', False, 4,  0,  1.0),
	('CCI',  True,  36, 0,  0.3),

]


def _cal(df, select_coin_num, c_rate, offset):
	df = df.copy()
	
	df['因子'] = 0
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
		reverse_factor = -1 if if_reverse else 1
		if d_num == 0:
			_factor = f'{factor_name}_bh_{back_hour}'
		else:
			_factor = f'{factor_name}_bh_{back_hour}_diff_{d_num}'

		df[_factor + '_因子'] = reverse_factor * df[_factor] 
		df[_factor + '_排名'] = df.groupby('candle_begin_time')[_factor + '_因子'].rank()
		# 处理空值
		df[_factor + '_排名'].fillna(value=0, inplace=True)
		df['因子'] += df[_factor + '_排名'] * weight

	df = df[df['因子']!=0]
	df_offset = df[df['offset'] == offset].copy()

	return ind.gen_select_df(df_offset, c_rate, select_coin_num)


# ===输出
print()
print('trade_type ---', trade_type)
print('hold_hour  ---', hold_hour)
print('c_rate     ---', c_rate)
print('factor_list\n    ', factor_list)
print()

# ===读数据
df = read_factors_async(trade_type, factor_list, hold_hour)
if trade_type=='swap':
    df = df[df['candle_begin_time'] >= pd.to_datetime('2020-06-01')]
df = df[df['candle_begin_time'] <= pd.to_datetime('2021-02-01')]
# =删除某些行数据
df = df[df['volume'] > 0]  # 该周期不交易的币种
df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
print('文件读取完毕!\n')

rtn_list    = []
ratio_list  = [] 	# 盈亏比　
return_list = [] 	# 净值

for offset in range(int(hold_hour[:-1])):
	select_coin = _cal(df, select_coin_num, c_rate, offset)
	rtn = ind.cal_ind(select_coin)
	if rtn is not None:
		r1 = rtn['累积净值'].values[0]
		r2 = rtn['最大回撤'].values[0]
		r2 = abs(float(r2.replace('%', '').strip()) / 100.)
		_ind = r1 / r2

		rtn_list.append(rtn)
		ratio_list.append(_ind)
		return_list.append(r1)

results = pd.DataFrame()
results = pd.concat(rtn_list, ignore_index=True)

# ===输出
print(results)
print()
print('平均盈亏比', np.array(ratio_list).mean())
print('年化盈亏比', results['年化收益/回撤比'].mean())
print('平均净值  ', np.array(return_list).mean())
print()






