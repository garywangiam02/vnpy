import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import glob
import os
from tqdm import tqdm
import datetime
import time
from joblib import Parallel, delayed
import gc
from Funtion import *
from functools import partial
from multiprocessing import Pool, cpu_count
def read_single_csv(path,time_interval):
    try:
        df = pd.read_csv(path, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
    except:
        df = pd.read_csv(path, header=0, encoding="GBK", parse_dates=['candle_begin_time'])
    df['symbol'] = os.path.splitext(os.path.basename(path))[0].split('_')[0].lower()
    # 仅提取有效信息
    if time_interval == '1m': df = df[df.candle_begin_time.dt.minute == 0]
    # 减少内存占用
    if RAM < 16: df[df.select_dtypes(include='float64').columns] = df[df.select_dtypes(include='float64').columns].astype('float32')
    return df


# 输入电脑内存大小,和并行进程数
# 16G以下会选择不耗内存的整理方式
RAM = 64
# 数值8以上开始边际效应递减
process_num = 8


# 选择合约类型
contract_type = 'spot'
contract_type = 'swap'


_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '../..'))  # 返回根目录文件夹
def main():
    t = time.time()
    # 整理5分钟
    time_interval ='5m'
    path_list = glob.glob(root_path + f'/data/backtest/{contract_type}/binance/*/*/*.csv')
    path_list = list(filter(lambda x: time_interval in x, path_list))

    # multiprocess并行
    f = partial(read_single_csv, time_interval=time_interval)
    pl = Pool(process_num)
    data_list = pl.map(f, tqdm(path_list))
    pl.close()
    pl.join()
    m5_data = pd.concat(data_list,ignore_index=True)
    del data_list
    gc.collect()
    # 排序并重新索引
    m5_data.sort_values(by='candle_begin_time', inplace=False)
    m5_data.reset_index(drop=True, inplace=True)
    # 5m处理
    m5_data['avg_price_5m'] = m5_data['quote_volume'] / m5_data['volume']  # 均价
    m5_data.set_index('candle_begin_time', inplace=True)

    # 整理1分钟
    time_interval ='1m'
    path_list = glob.glob(root_path + f'/data/backtest/{contract_type}/binance/*/*/*.csv')
    path_list = list(filter(lambda x: time_interval in x, path_list))

    # multiprocess并行
    f = partial(read_single_csv, time_interval=time_interval)
    pl = Pool(process_num)
    data_list = pl.map(f, tqdm(path_list))
    pl.close()
    pl.join()

    m1_data = pd.concat(data_list,ignore_index=True)
    # 排序并重新索引
    m1_data.sort_values(by='candle_begin_time', inplace=False)
    m1_data.reset_index(drop=True, inplace=True)
    # 1m处理
    m1_data['avg_price_1m'] = m1_data['quote_volume'] / m1_data['volume']  # 均价
    m1_data = m1_data[m1_data.volume>0]

    # 合成小时数据
    agg_dict = {
        'symbol': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
        'avg_price_5m': 'first'
    }

    h1_data = m5_data.groupby('symbol',group_keys=False).apply(lambda x:x.resample(rule='1H').agg(agg_dict))
    del m5_data
    del data_list
    gc.collect()

    # 现货异常数据过滤
    h1_data.dropna(subset=['symbol'],inplace=True)
    h1_data.reset_index(inplace=True)
    blacklist = ['cocos-usdt', 'btcst-usdt', 'drep-usdt', 'sun-usdt']
    leverage_suffix = ['up-usdt', 'down-usdt', 'bear-usdt', 'bull-usdt']
    filter_suffix = blacklist + leverage_suffix
    whitelist = [s for s in set(h1_data['symbol'].tolist()) if not any(s.endswith(y) for y in filter_suffix)]
    h1_data = h1_data[h1_data['symbol'].isin(whitelist)]

    # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
    h1_data['close']=h1_data.groupby('symbol',group_keys=False).apply(lambda x:x['close'].fillna(method='ffill'))
    h1_data['open']=h1_data.groupby('symbol',group_keys=False).apply(lambda x:x['open'].fillna(value=x['close']))
    h1_data['high']=h1_data.groupby('symbol',group_keys=False).apply(lambda x:x['high'].fillna(value=x['close']))
    h1_data['low']=h1_data.groupby('symbol',group_keys=False).apply(lambda x:x['low'].fillna(value=x['close']))

    # 将停盘时间的某些列，数据填补为0
    fill_0_list = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    h1_data.loc[:, fill_0_list] = h1_data[fill_0_list].fillna(value=0)
    h1_data['avg_price_5m'].fillna(value=h1_data['open'], inplace=True)  # 没有5分钟均价就使用开盘价

    h1_data = h1_data.merge(m1_data[['candle_begin_time','symbol','avg_price_1m']],on=['candle_begin_time','symbol'],how='left')
    h1_data['avg_price_1m'].fillna(value=h1_data['avg_price_5m'], inplace=True)  # 没有5分钟均价就使用开盘价
    h1_data['avg_price'] = h1_data['avg_price_1m']
    h1_data = h1_data[h1_data['volume']>0]

    h1_data.sort_values(['symbol','candle_begin_time'],inplace=True)
    h1_data = h1_data.drop_duplicates(subset=['candle_begin_time','symbol'])
    h1_data[h1_data.select_dtypes(include='float64').columns] = h1_data[h1_data.select_dtypes(include='float64').columns].astype('float32')

    store_path = root_path + f'/factor_bank/{contract_type}/basic_vol_price.pkl'
    h1_data.to_pickle(store_path)
    print(f'\n 数据整理耗时{time.time()-t}:s')
if __name__ == '__main__':
    main()