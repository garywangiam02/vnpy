import pandas as pd
import numpy as np
from preprocess import *
from joblib import Parallel, delayed
import multiprocessing as mp


contract_type = 'spot'
# 读取异常 缺失检查修复后的因子文件
data = pd.read_pickle(fr'..\..\data\backtest\{contract_type}\output\data_for_select\all_coin_data_hold_hour_6H_check.pkl')

# 对因子数据进行百分位去极值和中位数填充
# 根据电脑性能选择合适的并行数量
num_cores = int(mp.cpu_count())  # 获得计算机的核心数
# num_cores = 4
data = pd.concat(Parallel(n_jobs=num_cores,verbose=10)(delayed(extreme_process_quantile)(v.copy()) for k,v in \
                                                       data.groupby('candle_begin_time')))

# 根据需求,决定对因子进行哪种特征缩放处理.船队框架使用rank 是合适的
data = pd.concat(Parallel(n_jobs=num_cores,verbose=10)(delayed(data_scale_rank_Score)(v.copy()) for k,v in \
                                                       data.groupby('candle_begin_time')))

# 保存完成预处理的因子数据
data.to_pickle(fr'..\..\data\backtest\{contract_type}\output\data_for_select\all_coin_data_hold_hour_6H_processed.pkl')


