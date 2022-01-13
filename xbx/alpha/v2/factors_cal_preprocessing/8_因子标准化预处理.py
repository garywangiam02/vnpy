import pandas as pd
import numpy as np
from cal_and_preprocess import *
from joblib import Parallel, delayed
import multiprocessing as mp
from Funtion import *
import time
from functools import partial
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
t =time.time()

# 选择合约类型
contract_type = 'spot'
contract_type = 'swap'

# 选择因子存储方式
# factor_store_type = 'separated'
factor_store_type = 'combined'

# 根据电脑性能选择合适的并行数量
process_num = int(mp.cpu_count())-1  # 获得计算机的核心数
# process_num = 4
def main():
    if factor_store_type == 'separated':
        # 读取异常 缺失检查修复后的因子文件
        separated_path_list = glob.glob(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/separated/*')
        print(f'合约类型:{contract_type},{factor_store_type}因子异常缺失处理:{len(separated_path_list)-1}个因子')
        for f_path in separated_path_list:
            f_name = f_path.split('\\')[-1][:-4]
            if f_name == 'import_info': continue
            print(f_name)
            data = pd.read_pickle(f_path)
            # 对因子数据进行百分位去极值和中位数填充
            # 并行计算
            pl = Pool(process_num)
            data_list = pl.map(extreme_process_quantile, tqdm([v for k, v in data.groupby('candle_begin_time', group_keys=False)]))
            pl.close()
            pl.join()
            data = pd.concat(data_list)
            # 根据需求,决定对因子进行哪种特征缩放处理,暂时跳过,不影响船队回测框架
            # data_scale_rank_Score data_scale_Z_Score
            # data = pd.concat(Parallel(n_jobs=num_cores,verbose=10)(delayed(data_scale_rank_Score)(v.copy()) for k,v in \
            #                                                        data.groupby('candle_begin_time')))
            # 保存处理好的数据
            f_path = f_path.replace('before_preprocessing','after_preprocessing')
            data[data.select_dtypes(include='float64').columns] = data[data.select_dtypes(include='float64').columns].astype('float32')
            # 保存处理好的数据
            data.to_pickle(f_path)
    else:
        print(f'{factor_store_type}因子异常缺失处理:')

        data = pd.read_pickle(root_path + f'/factor_bank/{contract_type}/all_category/before_preprocessing/combined/basic_factor.pkl')
        # 对因子数据进行百分位去极值和缺失值中位数填充
        # 并行计算
        pl = Pool(process_num)
        data_list = pl.map(extreme_process_quantile,tqdm([v for k, v in data.groupby('candle_begin_time', group_keys=False)]))
        pl.close()
        pl.join()
        data = pd.concat(data_list)
        # 根据需求,决定对因子进行哪种特征缩放处理,暂时跳过,不影响船队回测框架
        # data_scale_rank_Score 与 data_scale_Z_Score 可以自己编写其他处理方式
        # data = pd.concat(Parallel(n_jobs=num_cores,verbose=10)(delayed(data_scale_rank_Score)(v.copy()) for k,v in \
        #                                                        data.groupby('candle_begin_time')))
        data[data.select_dtypes(include='float64').columns] = data[data.select_dtypes(include='float64').columns].astype('float32')
        # 保存处理好的数据
        data.to_pickle(root_path + f'/factor_bank/{contract_type}/all_category/after_preprocessing/combined/basic_factor.pkl')
    print(f'\n 耗时{time.time() - t}:s')
if __name__ == '__main__':
    main()

