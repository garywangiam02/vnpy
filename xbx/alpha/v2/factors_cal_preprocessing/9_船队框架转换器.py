import pandas as pd
import numpy as np
from Funtion import *
import gc
def fun(df, hold_hour):
    df['下个周期_avg_price'] = df['avg_price'].shift(-1)  # 计算下根K线开盘买入涨跌幅
    df = df.loc[0:0, :].append(df, ignore_index=True)
    df.loc[0, 'candle_begin_time'] = pd.to_datetime('2010-01-01')
    # ===将数据转化为需要的周期
    df['周期开始时间'] = df['candle_begin_time']
    df.set_index('candle_begin_time', inplace=True)
    period_df_list = []
    for offset in range(int(hold_hour[:-1])):
        # 转换周期
        period_df = df.resample(hold_hour, offset=f'{offset}h').agg(agg_dict)
        period_df['offset'] = offset

        # resample 中编写指标

        period_df.reset_index(inplace=True)
    # 合并数据
        period_df_list.append(period_df)
    # 将不同offset的数据，合并到一张表
    period_df = pd.concat(period_df_list, ignore_index=True)
    return period_df.dropna(subset=['symbol'])
"暂时只提供因子合并存储的转换,分隔存储转换牵涉到回测中的调用问题,后续优化后根据老板的内存需求再提供转换方式"
# 选择合约类型
contract_type = 'spot'
# contract_type = 'swap'
# 持币周期
hold_hour_list = ['1H', '2H', '3H', '4H', '6H', '8H', '12H', '16H', '24H']

data = pd.read_pickle(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/combined/basic_factor.pkl').iloc[:,:6]

data.reset_index(drop=True,inplace=True)
data.sort_values('candle_begin_time',inplace=True)

# 必备字段
agg_dict = {
    'symbol': 'first',
    '周期开始时间': 'first',
    'open': 'first',
    'avg_price': 'first',
    'close': 'last',
    '下个周期_avg_price': 'last',
    'volume': 'sum',
}
# 只保留关键 resample 信息
all_period_list = []
for hold_hour in hold_hour_list:
    print('开始 resample',hold_hour)
    period_df = data.groupby('symbol',group_keys=False).apply(lambda x:fun(x, hold_hour))
    period_df['hold_hour'] = hold_hour
    all_period_list.append(period_df)
period_df = pd.concat(all_period_list, ignore_index=True)
# 删除一些数据
period_df = period_df[period_df['candle_begin_time'] >= pd.to_datetime('2018-01-01')]  # 删除2018年之前的数据
# period_df.candle_begin_time = period_df.candle_begin_time.apply(lambda x:x.replace(nanosecond=0))


period_df.to_pickle(root_path + f'/factor_bank/{contract_type}/resample_info.pkl')

exit()
'''
划重点, 所有回测最开始载入数据时,加入下方代码运行即可
'''

def merge_hold_hour_factors(hold_hour):
    import gc
    temp = pd.read_pickle(root_path + f'/factor_bank/{contract_type}/resample_info.pkl')
    resample = temp[temp['hold_hour'] == hold_hour]
    del temp
    del resample['hold_hour']
    gc.collect()
    data = pd.read_pickle(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/combined/basic_factor.pkl')
    data.reset_index(drop=True,inplace=True)
    data.sort_values('candle_begin_time',inplace=True)
    factors_list = data.iloc[0:1,6:].columns.tolist()
    data[factors_list] = data.groupby('symbol')[factors_list].apply(lambda x:x.shift(1)) # 迎合刑大原始框架
    data = resample.merge(data[['candle_begin_time','symbol']+factors_list],on=['candle_begin_time','symbol'],how='left')
    return data
all_coin_data = merge_hold_hour_factors(hold_hour = '3H')
