import pandas as pd
import numpy as np
from Funtion import *
# ===缺失检查参数
contract_type = 'spot'
contract_type = 'swap'

# 可以接受的每日最高缺失值比例
miss_thres = 0.05

# 读取data_for_select 的因子文件
data = pd.read_pickle(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/combined/basic_factor.pkl')
# print(data)
# 分离存储需要分别检查,加个循环即可
# separated_path_list = glob.glob(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/separated/*')

# 获得因子列表
del_list = ['candle_begin_time', 'symbol', 'open', 'avg_price', 'close','volume']
factors_list = [x for x in data.columns.tolist() if x not in del_list]

# ===因子缺失值检查统计
# 将inf 替换为pandas 容易处理的Nan
data = data.replace([np.inf, -np.inf], np.nan)
# 计算因子各日期各因子缺失情况
a = data.groupby('candle_begin_time')[factors_list].apply(lambda x: x.isnull().sum())
check_df = a/np.expand_dims(data.groupby('candle_begin_time')[factors_list].size(),0).repeat(a.shape[1],axis=0).T



print('因子缺失值异常详情:\n')
miss_factor_list = check_df.max()[check_df.max()>miss_thres].index.tolist()
miss_list = []
for factor in miss_factor_list:
    temp = check_df[check_df[factor]>miss_thres][factor].reset_index()
    miss_list.append(temp.T)
    print(factor,':')
    print(temp,'\n')
# pd.concat(miss_list).T.to_csv(f'{contract_type}_因子缺失值异常详情.csv',encoding='gbk')

'''
对于单日缺失比例大于阈值的因子及参数，需要检查因子定义和币种数据,进行检查、修复或者剔除。
之后才能进行下一步的因子去极值和缺失值填充.
当然,跳过后对数据缺陷因子不使用也是一种剔除方式.
'''


# 针对目前因子库的检查结果
'''
因子缺失性检查条件: 日最大缺失比例小于5%, 基本通过. 特殊因子说明如下:
zhang_die_fu_skew_bh_3 出现一些空值最早18年6月最晚19年12月 原因是涨幅连续三H 一样,skew函数返回Nan,日最大缺失比例6% 主要为特殊币种成交清淡导致 正常填充
quanlity_price_corr_bh_4 _3 出现空值,因为多个h close一致, 日最大缺失比例6%  正常填充

'''

