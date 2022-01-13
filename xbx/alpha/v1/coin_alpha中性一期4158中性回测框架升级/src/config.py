#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os

_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹


njobs = 16  # 并行数量

trade_type_list = ['spot', 'swap']			    
#back_hour_list = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96]  		
#diff_d = [0.3, 0.5, 0.7, 0.9]   

#trade_type_list = ['spot', ]
back_hour_list  = [4, 36, ]
diff_d = []  # 差分阶数


select_coin_num = 1  # 选币数量
c_rate = 4 / 10000  # 手续费


head_columns = [
    'candle_begin_time', 
    'symbol', 
    'avg_price',
    '下个周期_avg_price', 
    'volume', 
]   
delete_columns = [
    'open',
    'high',
    'low',
    'close', 
    'quote_volume',
    'trade_num',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
]

# 创建目录
pickle_path = os.path.join(root_path, 'data', 'pickle_data')
if not os.path.exists(pickle_path):
	os.mkdir(pickle_path)

output_path = os.path.join(_, 'output')
if not os.path.exists(output_path):
	os.mkdir(output_path)

data_path = os.path.join(output_path, 'data')
if not os.path.exists(data_path):
	os.mkdir(data_path)

result_path = os.path.join(output_path, 'result')
if not os.path.exists(result_path):
	os.mkdir(result_path)

ga_path = os.path.join(output_path, 'ga')
if not os.path.exists(ga_path):
	os.mkdir(ga_path)












