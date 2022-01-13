import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import glob
import os
from tqdm import tqdm
import datetime
import factors_cal
import time
from joblib import Parallel, delayed
from cal_and_preprocess import *
import gc
from Funtion import *
from functools import partial
from multiprocessing import Pool, cpu_count
# 并行进程数
process_num = 20
# 过滤因子初上市的N小时,尽量和实盘一致,不建议太小
live_thres = 300

# 选择合约类型
contract_type = 'spot'
contract_type = 'swap'

# 选择因子存储方式
factor_store_type = 'separated'
# factor_store_type = 'combined'

# 计算各个指标的回溯周期
back_hour_list = [3, 4, 6, 8, 9, 12, 24, 36, 48, 72, 96]
# 计算用列表生成
batch_factors_cal_list = [x for x in dir(factors_cal) if x.startswith('signal_factor')]
factors_names_list = [f'{x[14:]}_bh_{n}' for x in batch_factors_cal_list for n in back_hour_list]

# 读取基础行情数据
data = pd.read_pickle(root_path + f'/factor_bank/{contract_type}/basic_vol_price.pkl')
del data['avg_price_5m'], data['avg_price_1m']
data = data_filter(data, live_thres)

def main():
    if factor_store_type == 'separated':
        '''因子分离存储'''
        print(f'因子分离存储计算: 共 {len(batch_factors_cal_list)} 个\n')
        for f_name in batch_factors_cal_list:
            print(f_name)
            # 并行计算
            f = partial(single_factors_cal, f_name=f_name, back_hour_list=back_hour_list)
            # 并行计算
            pl = Pool(process_num)
            all_coin_data_list = pl.map(f, tqdm([v for k, v in data.groupby('symbol', group_keys=False)]))
            pl.close()
            pl.join()
            single_factors = pd.concat(all_coin_data_list)
            single_factors = single_factors[single_factors['live_hours']>300]
            if f_name == batch_factors_cal_list[0]:
                temp = single_factors[['candle_begin_time', 'symbol', 'open', 'close', 'volume', 'avg_price']]
                temp[temp.select_dtypes(include='float64').columns] = temp[temp.select_dtypes(include='float64').columns].astype('float32')
                temp.to_pickle(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/separated/import_info.pkl')
            single_factors.drop(['open', 'close', 'volume', 'avg_price', 'live_hours'], axis = 1, inplace = True)
            single_factors[single_factors.select_dtypes(include='float64').columns] = single_factors[ \
                single_factors.select_dtypes(include='float64').columns].astype('float32')
            single_factors.to_pickle(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/separated/{f_name[14:]}.pkl')

    else:
        '''因子合并存储'''
        print(f'因子合并存储计算: 共 {len(batch_factors_cal_list)} 个\n', batch_factors_cal_list)

        f = partial(batch_factors_cal, batch_factors_cal_list=batch_factors_cal_list,back_hour_list=back_hour_list)
        # 并行计算
        pl = Pool(process_num)
        all_coin_data_list = pl.map(f, tqdm([v for k, v in data.groupby('symbol', group_keys=False)]))
        pl.close()
        pl.join()
        combined_factors = pd.concat(all_coin_data_list)
        combined_factors = combined_factors[combined_factors['live_hours'] > 300]
        del combined_factors['live_hours']
        combined_factors.to_pickle(
            root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/combined/basic_factor.pkl')

if __name__ == '__main__':
    t = time.time()
    main()
    print(f'\n 因子计算耗时{time.time() - t}:s')
