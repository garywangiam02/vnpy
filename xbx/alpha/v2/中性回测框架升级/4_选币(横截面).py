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
pd.set_option('display.max_rows',  50)  # 最多显示数据的行数

#plt.rcParams['font.sans-serif']    = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False      # 用来正常显示负号
plt.figure(figsize=(8,8), dpi=80)
plt.figure(1)


# ===参数设定
symbol_filters  = [
	# 涨幅最多币种
	#'THETA-USDT', 'ADA-USDT',  'BNB-USDT', 'CHZ-USDT', 'MATIC-USDT',
	# 跌幅最多币种
	#'YFII-USDT',  'XEM-USDT',  'CRV-USDT', 'ALICE-USDT', 'CELR-USDT',
	# 独立走势币种
	#'CHZ', 'ANKR', 'ENJ', 'AXS', 'FIL',
	# 下架币种
	#'LEND', 'DOTECO', 'SC',
]
select_coin_num = 1
mdd_std         = 0.2  		## 设定回撤标准值
c_rate 			= 4/10000
trade_type 		= 'spot'
hold_hour  		= '6H'      # 持币周期
#filter_factor   = 'DC_filter_8'
filter_factor   = None
factor_list = [
	# factor, reverse, back_hour, diff_num, weight
	('BIAS', False, 4,  0,  1.0), ('CCI',  True,  36, 0,  0.3),

]


def _cal(df, select_coin_num, c_rate, offset):
	df = df[df['offset'] == offset].copy()

	df['因子'] = 0
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
		reverse_factor = -1 if if_reverse else 1
		if d_num == 0:
			_factor = f'{factor_name}_bh_{back_hour}'
		else:
			_factor = f'{factor_name}_bh_{back_hour}_diff_{d_num}'

		# 处理因子空值
		df = df.replace([np.inf, -np.inf], np.nan)  # 替换异常值并且删除
		df.dropna(subset=[_factor], inplace=True)

		df[_factor + '_因子'] = reverse_factor * df[_factor] 
		df[_factor + '_排名'] = df.groupby('candle_begin_time')[_factor + '_因子'].rank()

		# 处理空值
		df[_factor + '_排名'].fillna(value=0, inplace=True)
		# 计算
		df['因子'] += df[_factor + '_排名'] * weight
		# 删除
		del df[_factor + '_因子']
		del df[_factor + '_排名']
		
	df = df[df['因子']!=0]
	return ind.gen_select_df(df, c_rate, select_coin_num)



# ===输出
print()
print('trade_type ---', trade_type)
print('hold_hour  ---', hold_hour)
print('c_rate     ---', c_rate)
print('factor_list\n    ', factor_list)
print()

# ===读数据
df = read_factors_async(trade_type, factor_list, hold_hour, filter_factor=filter_factor, symbol_filters=symbol_filters)
# =过滤时间
if trade_type == 'swap':
	df = df[df['candle_begin_time'] >= pd.to_datetime('2020-06-01')]
	df = df[df['candle_begin_time'] <  pd.to_datetime('2021-02-01')]
else:
	df = df[df['candle_begin_time'] >= pd.to_datetime('2020-06-01')]
	df = df[df['candle_begin_time'] <  pd.to_datetime('2021-02-01')]

	
# =删除某些行数据
df = df[df['volume'] > 0]  # 该周期不交易的币种
df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
df.reset_index(drop=True, inplace=True)
print('文件读取完毕!\n')




rtn_list    = []
ratio_list  = [] 	# 盈亏比　
return_list = [] 	# 净值
select_merge_list = []


for offset in range(int(hold_hour[:-1])):
	select_coin = _cal(df, select_coin_num, c_rate, offset)
	rtn, select_c = ind.cal_ind(select_coin)
	select_merge_list.append(select_c)

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


select_c = pd.concat(select_merge_list, ignore_index=True)
select_c.sort_values(by=['candle_begin_time'], inplace=True)
select_c.reset_index(inplace=True)
select_c['本周期实现涨跌幅'] = select_c['本周期多空涨跌幅']/int(hold_hour[:-1])
select_c['本周期实现涨跌幅'].fillna(0, inplace=True)
select_c['合成资金曲线'] = (select_c['本周期实现涨跌幅'] + 1).cumprod()
del select_c['本周期多空涨跌幅']
del select_c['资金曲线']
select_c.rename(columns={"本周期实现涨跌幅": "本周期多空涨跌幅", "合成资金曲线":"资金曲线"}, inplace=True)
#print(select_c)

rtn, select_c = ind.cal_ind(select_c)
from utils.target  import target_sharpe0 as target
sharp_ratio = target(select_c)
print('sharp_ratio:', sharp_ratio)
print(rtn)
print('\n')

condition = (select_c['dd2here'] >= -mdd_std) & (select_c['dd2here'].shift(1) < -mdd_std)
select_c[f'回撤上穿{mdd_std}次数'] = 0
select_c.loc[condition, f'回撤上穿{mdd_std}次数'] = 1
mdd_num = int(select_c[f'回撤上穿{mdd_std}次数'].sum())
ax = plt.subplot(2, 1, 1)
plt.subplots_adjust(hspace=1)  # 调整子图间距
plt.title(f'Back draw{mdd_std} Number: {mdd_num}', fontsize='large', fontweight = 'bold',color='blue', loc='center')  # 设置字体大小与格式
ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])
ax2 = ax.twinx() # 设置y轴次轴
ax2.plot(select_c["candle_begin_time"], -select_c['dd2here'], color='red', alpha=0.4)
plt.show()




