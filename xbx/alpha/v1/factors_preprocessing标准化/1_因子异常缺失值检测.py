import pandas as pd
import numpy as np

# ===缺失检查参数
contract_type = 'spot'
# 过滤各币种初上市若干小时
live_thres = 300
# 可以接受的每日最高缺失值比例
miss_thres = 0.05

# 读取data_for_select 的因子文件
data = pd.read_pickle(fr'..\..\data\backtest\{contract_type}\output\data_for_select\all_coin_data_hold_hour_6H.pkl')


# 计算各币种上市小时数并过滤
data.sort_values(['symbol', 'candle_begin_time'], inplace=True)
data['live_hours'] = data.groupby('symbol')['candle_begin_time'].apply(lambda x: x.expanding().count())
data = data[data.live_hours > live_thres]
# 获得因子列表
del data['live_hours']
del_list = ['candle_begin_time', 'symbol', '周期开始时间', 'open', 'avg_price', 'close','下个周期_avg_price', 'volume','offset']
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
pd.concat(miss_list).T.to_csv('因子缺失值异常详情.csv',encoding='gbk')

'''
对于单日缺失比例大于阈值的因子及参数，需要检查因子定义和币种数据,进行检查、修复或者剔除。
之后才能进行下一步的因子去极值和缺失值填充.
当然,跳过后对数据缺陷因子不使用也是一种剔除方式.
'''

exit()

# 保存进行检查后的因子文件
data.to_pickle(fr'..\..\data\backtest\{contract_type}\output\data_for_select\all_coin_data_hold_hour_6H_check.pkl')
