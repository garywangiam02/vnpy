import pandas as pd
import numpy as np
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
