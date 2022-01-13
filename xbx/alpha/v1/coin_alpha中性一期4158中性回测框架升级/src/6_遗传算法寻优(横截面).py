#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import glob
import pandas as pd
import numpy  as np
import geatpy as ea
import matplotlib.pyplot as plt
from multiprocessing import Pool as ProcessPool
import traceback

from config import data_path
from config import head_columns
from utils.target  import target_annual_ratio as target
from utils.commons import read_factors_async, reduce_mem_usage

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 50)  # 最多显示数据的行数
import warnings
warnings.filterwarnings("ignore")
from warnings import simplefilter


NIND  = 20    # 种群规模
njobs = NIND  # 并发线程

# ===参数设定
select_coin_num = 1
c_rate 			= 4/10000
trade_type 		= 'spot'
hold_hour  		= '6H'  
#back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96]  		
back_hour_list  = [4, 36, ]  
# 指定主因子
d_factor_list  = [
	# (factor, reverse, back_hour, diff_num, weight)

]
factor_classes = [
	'BIAS',
	'CCI',
]



factors = []
for cls_name in factor_classes:
    factor_cls = __import__('factors.%s' % cls_name,  fromlist=('', ))
    _factors = getattr(factor_cls, 'factors')
    for _name in _factors:
        factors.append(_name)


# =====读所有因子数据并压缩
all_factor_list = d_factor_list.copy()
for factor_name in factors:
	for back_hour in back_hour_list:
		all_factor_list.append((factor_name, True, back_hour, 0, 1.0))
all_factor_list.extend(d_factor_list)

all_data = read_factors_async(trade_type, all_factor_list, hold_hour)
print('读取数据完毕!!!\n')
if all_data is None or all_data.empty:
	exit()
all_data = reduce_mem_usage(all_data)
all_data['symbol'] = all_data['symbol'].astype('object')
print('压缩完毕!!!\n')


def _factor_columns(factor_list):
	_columns = []
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
		if d_num > 0:
			_f = f'{factor_name}_bh_{back_hour}_diff_{d_num}'
		else:
			_f = f'{factor_name}_bh_{back_hour}'
		_columns.append(_f)

	return _columns


def evaluate(arg):
	factor_list = arg
	factor_list.extend(d_factor_list)

	hold_peroid = int(hold_hour[:-1])
	# ===读取数据
	factor_name = []
	factor_name.extend(head_columns + ['offset'])
	factor_name.extend(_factor_columns(factor_list))
	df = all_data[factor_name].copy()

	result = 0
	for offset in range(hold_peroid):
		result += evaluate_offset((df[df['offset'] == offset], factor_list, offset))

	return result/hold_peroid, factor_list


def evaluate_offset(arg):
	df, factor_list, offset = arg

	try:
		df['因子'] = 0
		cnt_weight_zero = 0
		for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
			if weight == 0:
				cnt_weight_zero += 1
				continue

			reverse_factor = -1 if if_reverse else 1
			if d_num > 0:
				df[factor_name + '_因子'] = reverse_factor * df[f'{factor_name}_bh_{back_hour}_diff_{d_num}'] 
			else:
				df[factor_name + '_因子'] = reverse_factor * df[f'{factor_name}_bh_{back_hour}'] 

			df[factor_name + '_排名'] = df.groupby('candle_begin_time')[factor_name + '_因子'].rank()
			# 处理空值
			df[factor_name + '_排名'].fillna(value=0, inplace=True)
			df['因子'] += df[factor_name + '_排名'] * weight

		if cnt_weight_zero == len(factor_list):
			return 0

		df = df[df['因子']!=0]
		net = target(df, c_rate, select_coin_num)
		if np.isnan(net):
			net = 0
	except Exception as ex:
		traceback.print_exc()
		net = 0
	finally:
		del df
	return net


cache = []
bhl   = len(back_hour_list) - 1
_lb   = [0, 0,   0] * len(factors)
_ub   = [1, bhl, 10] * len(factors)
_lbin = [1, 1,   1] * len(factors)
_ubin = [1, 1,   1] * len(factors)


class alphaFactory(ea.Problem):  # 继承Problem父类
	def __init__(self):
		name 	  = 'alphaFactory'  # 初始化name（函数名称，可以随意设置）
		M 		  = 1  				# 初始化M（目标维数）
		maxormins = [-1]  			# 初始化maxormins（目标最小最大化标记列表，1：最小化该目标；-1：最大化该目标）
		Dim 	  = len(factors)*3  # 初始化Dim（决策变量维数）
		varTypes  = [1] * Dim  		# 初始化varTypes（决策变量的类型，元素为0表示对应的变量是连续的；1表示是离散的）
		lb   	  = _lb  			# 决策变量下界
		ub   	  = _ub  			# 决策变量上界
		lbin 	  = _lbin  			# 决策变量下边界（0表示不包含该变量的下边界，1表示包含）
		ubin 	  = _ubin  			# 决策变量上边界（0表示不包含该变量的上边界，1表示包含）
		# 调用父类构造方法完成实例化
		ea.Problem.__init__(self, name, M, maxormins, Dim, varTypes, lb, ub, lbin, ubin)
		self.pool       = ProcessPool(njobs)  # 设置池的大小
		self.best_score = 0
		self.cache  	= None

	def aimFunc(self, pop):  # 目标函数
		Vars = pop.Phen  # 得到决策变量矩阵
		hold_peroid = int(hold_hour[:-1])
		
		args = []
		for i in range(NIND):
			factor_list = []
			for j in range(len(factors)):
				factor     = factors[j]
				is_reverse = int(Vars[:, [j*3]][i])
				back_hour  = back_hour_list[int(Vars[:, [j*3+ 1]][i])]
				weight 	   = int(Vars[:, [j*3 + 2]][i]) * 0.1
				factor_list.append((factor, is_reverse, back_hour, 0, weight))
			args.append(factor_list)

		result_list = self.pool.map(evaluate, args)

		target_list = []
		for target, factor_list in result_list:
			if target > self.best_score:
				self.best_score = target
				self.cache      = factor_list
				# 跟踪打印最优结果
				show_r = []
				for _f, _reverse_num, back_hour, d_num, weight in self.cache:
					_reverse = False if _reverse_num == 0 else True
					if weight > 0:
						show_r.append((_f, _reverse, back_hour, d_num, weight))

				print('best_score',self.best_score, show_r)
			target_list.append(target)

		pop.ObjV = np.array([target_list]).transpose()   # 累积净值

	def calReferObjV(self):  # 设定目标数参考值（本问题目标函数参考值设定为理论最优值）
		referenceObjV = np.array([[10000]])
		return referenceObjV


if __name__ == '__main__':
	"""================================实例化问题对象==========================="""
	problem = alphaFactory()  # 生成问题对象
	"""==================================种群设置=============================="""
	Encoding   = 'RI'  # 编码方式
	Field      = ea.crtfld(Encoding, problem.varTypes, problem.ranges, problem.borders)  # 创建区域描述器
	population = ea.Population(Encoding, Field, NIND)  # 实例化种群对象（此时种群还没被初始化，仅仅是完成种群对象的实例化）
	"""================================算法参数设置============================="""
	myAlgorithm = ea.soea_DE_rand_1_bin_templet(problem, population)  # 实例化一个算法模板对象
	myAlgorithm.MAXGEN       	= 1000	  	# 1000 最大进化代数
	myAlgorithm.mutOper.F    	= 0.5  		# 差分进化中的参数F 0.5
	myAlgorithm.recOper.XOVR 	= 0.7  		# 重组概率 0.7
	myAlgorithm.logTras      	= 1  		# 设置每隔多少代记录日志，若设置成0则表示不记录日志
	myAlgorithm.verbose      	= True  	# 设置是否打印输出日志信息
	myAlgorithm.drawing      	= 0 		# 设置绘图方式（0：不绘图；1：绘制结果图；2：绘制目标空间过程动画；3：绘制决策空间过程动画）
	#myAlgorithm.trappedValue 	= 1e-6  	# “进化停滞”判断阈值
	#myAlgorithm.maxTrappedCount = 50  		# 进化停滞计数器最大上限值，如果连续maxTrappedCount代被判定进化陷入停滞，则终止进化
	"""===========================调用算法模板进行种群进化========================"""
	try:
		[BestIndi, population] = myAlgorithm.run()  # 执行算法模板，得到最优个体以及最后一代种群
		#BestIndi.save()  # 把最优个体的信息保存到文件中
	except Exception as reason:
		traceback.print_exc()
		print(problem.best_score)
		print(problem.cache)
		exit()
	"""==================================输出结果=============================="""
	print('评价次数：%s' % myAlgorithm.evalsNum)
	print('时间已过 %s 秒' % myAlgorithm.passTime)




