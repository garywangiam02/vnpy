"""
邢不行2020策略分享会
0705数字货币多空选币中性策略
邢不行微信：xbx9025
"""
import pandas as pd
import numpy as np
import glob
import os
from xbx.coin_alpha.program.backtest.Function import *
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数


# 持币周期
hold_hour_list = ['3H', '4H', '6H', '8H', '12H', '24H']
# 计算各个指标的回溯周期
back_hour_list = [3, 4, 6, 8, 9, 12, 24, 36, 48, 72, 96]

# 获取每个币种的数据路径
hdf_file_list = glob.glob(root_path + '/data/backtest/output/pickle_data/*USDT.pkl')

# ===遍历每个持币周期
for hold_hour in hold_hour_list:
    all_coin_data_list = []
    # ===遍历每个币种
    for file_name in hdf_file_list:
        print(hold_hour, file_name)

        # ==读取数据
        df = pd.read_pickle(file_name)

        df['涨跌幅'] = df['close'].pct_change()  # 计算涨跌幅
        df['下个周期_avg_price'] = df['avg_price'].shift(-1)  # 计算下根K线开盘买入涨跌幅

        # ==找出不交易的周期
        df.loc[df['volume'] == 0, '是否交易'] = 0
        df['是否交易'].fillna(value=1, inplace=True)

        # ========需要修改的代码
        # ===计算各项选币指标
        extra_agg_dict = {}
        # com
        for n in back_hour_list:
            df['momentum'] = df['close'] - df['close'].shift(1)
            df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
            df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
            df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
            df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
            df[f'cmo_bh_{n}'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
            df[f'cmo_bh_{n}'] = df[f'cmo_bh_{n}'].shift(1)
            extra_agg_dict[f'cmo_bh_{n}'] = 'first'
        # # 涨跌幅
        # for n in back_hour_list:
        #     df['前%dh涨跌幅' % n] = df['close'].pct_change(n)
        #     df['前%dh涨跌幅' % n] = df['前%dh涨跌幅' % n].shift(1)
        #     extra_agg_dict['前%dh涨跌幅' % n] = 'first'
        # # 振幅
        # for n in back_hour_list:
        #     high = df['high'].rolling(n, min_periods=1).max()
        #     low = df['low'].rolling(n, min_periods=1).min()
        #     df['前%dh振幅' % n] = high / low - 1
        #     df['前%dh振幅' % n] = df['前%dh振幅' % n].shift(1)
        #     extra_agg_dict['前%dh振幅' % n] = 'first'
        # ========需要修改的代码

        # ===将数据转化为需要的周期
        df['周期开始时间'] = df['candle_begin_time']
        df.set_index('candle_begin_time', inplace=True)
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
        agg_dict = dict(agg_dict, **extra_agg_dict)  # 需要保留的列
        # 不同的offset，进行resample
        period_df_list = []
        for offset in range(int(hold_hour[:-1])):
            # 转换周期
            period_df = df.resample(hold_hour, base=offset).agg(agg_dict)
            period_df['offset'] = offset
            period_df.reset_index(inplace=True)
            # 合并数据
            period_df_list.append(period_df)
        # 将不同offset的数据，合并到一张表
        period_df = pd.concat(period_df_list, ignore_index=True)

        # 删除一些数据
        period_df = period_df.iloc[24:]  # 刚开始交易前24个周期删除
        period_df = period_df[period_df['candle_begin_time'] >= pd.to_datetime('2018-01-01')]  # 删除2018年之前的数据

        # 合并数据
        all_coin_data_list.append(period_df)

    # ===将不同的币种数据合并到一张表，并且存储
    all_coin_data = pd.concat(all_coin_data_list, ignore_index=True)
    all_coin_data.sort_values(by=['offset', 'candle_begin_time', 'symbol'], inplace=True)
    period_df.reset_index(drop=True, inplace=True)
    all_coin_data.to_pickle(root_path + '/data/backtest/output/data_for_select/all_coin_data_hold_hour_%s.pkl' % hold_hour)
    print()
