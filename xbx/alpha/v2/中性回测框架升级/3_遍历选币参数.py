#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import time
import pandas as pd
import numpy  as np
from joblib import Parallel, delayed
from utils  import ind
from utils  import diff

pd_display_rows = 20
pd_display_cols = 100
pd_display_width = 1000
pd.set_option('display.max_rows', pd_display_rows)
pd.set_option('display.min_rows', pd_display_rows)
pd.set_option('display.max_columns', pd_display_cols)
pd.set_option('display.width', pd_display_width)
pd.set_option('display.max_colwidth', pd_display_width)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('expand_frame_repr', False)
os.environ['NUMEXPR_MAX_THREADS'] = "256"
import warnings
warnings.filterwarnings("ignore")
from warnings import simplefilter


from config import c_rate, select_coin_num
from config import data_path, result_path
from config import head_columns

from factors import class_list
from utils.commons import read_factors_async

def cal_one_factor(data_pkl, _trade_type, _hold_hour, _factor, _reverse):
    print(f'    factor --- {_factor} reverse --- {_reverse}')
    rtn_one_hold_one_factor_list = []

    # ===读数据
    data_pkl = data_pkl[head_columns + ['offset', 'filter', _factor]]

    for _offset in [_ for _ in range(int(_hold_hour[:-1]))]:
        data_offset = data_pkl[data_pkl['offset'] == _offset].copy()
        # =删除某些行数据
        data_offset = data_offset[data_offset['volume'] > 0]  # 该周期不交易的币种
        data_offset.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
        df = data_offset.copy()

        ind.cal_factor(df, _factor, _reverse)
        select_c = ind.gen_select_df(df, c_rate, select_coin_num)

        # =====计算统计指标
        if select_c.empty:
            rtn_one_hold_one_factor_list.append(
                [_hold_hour, _offset, _factor, _reverse, 0, '0%', '0%', 0, 0, 0, np.nan, np.nan, 0]
            )
            return rtn_one_hold_one_factor_list

        rtn, select_c = ind.cal_ind(_select_c=select_c)
        ind1 = rtn['累积净值'].values[0]
        ind2 = rtn['最大回撤'].values[0]
        ind3 = rtn['胜率'].values[0]
        ind4 = rtn['盈亏收益比'].values[0]
        ind5 = rtn['最大连续盈利周期数'].values[0]
        ind6 = rtn['最大连续亏损周期数'].values[0]
        ind7 = rtn['最大回撤开始时间'].values[0]
        ind8 = rtn['最大回撤结束时间'].values[0]
        ind9 = rtn['年化收益/回撤比'].values[0]

        rtn_one_hold_one_factor_list.append(
            [_hold_hour, _offset, _factor, _reverse, ind1, ind2, ind3, ind4, ind5, ind6, ind7, ind8, ind9]
        )

    return rtn_one_hold_one_factor_list


def cal_one_hold(_trade_type, _hold_hour):
    print()
    print()
    print(f'trade_type --- {_trade_type} hold_hour --- {_hold_hour}')

    # 读文件
    df = read_factors_async(_trade_type, factors_config_list, _hold_hour)
    # 时间周期过滤
    if _trade_type == 'swap':
        df = df[df['candle_begin_time'] >= pd.to_datetime('2020-06-01')]
    df = df[df['candle_begin_time'] <= pd.to_datetime('2021-02-01')]
    # =删除某些行数据
    df = df[df['volume'] > 0]  # 该周期不交易的币种
    df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
    print('数据读取完毕!!!')

    rtn_factor_list = []
    rtn_factor_list += Parallel(n_jobs=8)(
        delayed(cal_one_factor)(
            df, _trade_type, _hold_hour, factor, reverse
        )
        for factor  in factors_list
        for reverse in reverse_list
    )
    # 展平list
    rtn_list = []
    for _one_factor_rtn in rtn_factor_list:
        for _one_offset_rtn in _one_factor_rtn:
            rtn_list.append(_one_offset_rtn)

    # 将回测结果保存到文件
    rtn_df = pd.DataFrame(
        rtn_list, 
        columns=['持币周期', 'offset', '因子名称', '是否反转', '累积净值', 
            '最大回撤', '胜率', '盈亏收益比', '最大连盈', '最大连亏',
            '最大回撤开始时间', '最大回撤结束时间', '年化收益/回撤比']
    )
    rtn_df.sort_values(by=['累积净值'], inplace=True, ascending=False)
    rtn_df.to_csv(
        os.path.join(result_path, f'{_trade_type}_{_hold_hour}_select({select_coin_num}).csv'), 
        encoding='utf-8-sig'
    )
    print(f'持币周期 {_hold_hour} 回测所有因子完成\n\n')


# 参数设定
reverse_list    = [True, False]
trade_type_list = ['spot',  ]
hold_hour_list  = ['4H', '6H', '8H', '12H', '16H', '24H', ]
back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96, 120, 240, 480]        
#diff_list       = [0.1, 0.3, 0.5, 0.7, 0.9]   
diff_list       = [0, ]  

# 所有因子名称
factors_list        = []
factors_config_list = []
for _factor_name in class_list:
    for _back_hour in back_hour_list:
        for d_num in diff_list:
            if d_num > 0:
                factors_config_list.append((_factor_name, True, _back_hour, d_num, 1.0))
                factors_list.append(f'{_factor_name}_bh_{_back_hour}_diff_{d_num}')
            else:
                factors_list.append(f'{_factor_name}_bh_{_back_hour}')
                factors_config_list.append((_factor_name, True, _back_hour, 0, 1.0))


# 遍历所有周期
for _trade_type in trade_type_list:
    for hold_hour in hold_hour_list:
        cal_one_hold(_trade_type=_trade_type, _hold_hour=hold_hour)






