import pandas as pd
import numpy as np
import os
import factors_cal


def data_filter(data,live_thres=350):
    '''排序 过滤成交量 上市不足120h'''
    data.sort_values('candle_begin_time',inplace=True)
    data.reset_index(drop=True,inplace=True)
    data = data[data['volume']>0]
    # 过滤小时成交量为0 与上市时间过短的币种
    data['live_hours'] = data.groupby('symbol')['candle_begin_time'].apply(lambda x:x.expanding().count())
    del_live_list = data.groupby('symbol')['live_hours'].max()[data.groupby('symbol')['live_hours'].max()<live_thres+50].index.tolist()
    data = data[~data.symbol.isin(del_live_list)]
#     del data['live_hours']
    return data

def single_factors_cal(data, f_name, back_hour_list):
    params_out_list = ['candle_begin_time', 'symbol','open','close','volume','avg_price', 'live_hours']
    try:
        getattr(factors_cal, f_name)(data,back_hour_list)
    except Exception as e:
        print(e)
        print(f_name)
    params_out_list.extend([f'{f_name[14:]}_bh_{n}' for n in back_hour_list])
    factors = data[params_out_list]
    return factors
def batch_factors_cal(data, batch_factors_cal_list, back_hour_list,):
    params_out_list = ['candle_begin_time', 'symbol','open','close','volume','avg_price', 'live_hours']
    for f_name in batch_factors_cal_list:
        try:
            getattr(factors_cal, f_name)(data,back_hour_list)
        except Exception as e:
            print(e)
            print(f_name)
        params_out_list.extend([f'{f_name[14:]}_bh_{n}' for n in back_hour_list])
    factors = data[params_out_list]
    factors[factors.select_dtypes(include='float64').columns] = factors[factors.select_dtypes(include='float64').columns].astype('float32')
    return factors

# 百分位去极值 中位数填充同时处理函数
def extreme_process_quantile(data,down=0.05,up=0.95):
    data_=data.copy()  # 为不破坏原始数据，先对其进行拷贝
    feature_names = [i for i in data_.columns.tolist() if i not in ['candle_begin_time', 'symbol', '周期开始时间', 'open', 'avg_price', 'close','下个周期_avg_price', 'volume','offset']]   #获取数据集中需测试的因子名
    min_thres = data_.quantile(q=down)
    max_thres = data_.quantile(q=up)
    data_.loc[:,feature_names]=data_.loc[:,feature_names].clip(lower=min_thres,upper=max_thres,axis=1) #利用clip()函数，将因子取值限定在上下限范围内，即用上下限来代替异常值
    data_ = data_.replace([np.inf, -np.inf], np.nan)
    data_[feature_names]=data_[feature_names].apply(lambda factor: factor.fillna(factor.median()))
    return data_
# rank
def data_scale_rank_Score(data,asc=True,pct=True):
    data_=data.copy()  # 为不破坏原始数据，先对其进行拷贝
    feature_names = [i for i in data_.columns.tolist() if i not in ['candle_begin_time', 'symbol', '周期开始时间', 'open', 'avg_price', 'close','下个周期_avg_price', 'volume','offset']]   #获取数据集中需测试的因子名
    data_.loc[:,feature_names] = data_.loc[:,feature_names].apply(lambda x: x.rank(ascending=asc, pct=pct,method='first'))
    return data_
# Standardization
def data_scale_Z_Score(data):
    data_=data.copy()  # 为不破坏原始数据，先对其进行拷贝
    feature_names = [i for i in data_.columns.tolist() if i not in ['candle_begin_time','symbol']]   #获取数据集中需测试的因子名
    data_.loc[:,feature_names] = (data_.loc[:,feature_names]-data_.loc[:,feature_names].mean())/data_.loc[:,feature_names].std()
    return data_
