"""
邢不行2020策略分享会
0705数字货币多空选币中性策略
邢不行微信：xbx9025
"""
import pandas as pd
import glob
import os
import matplotlib.pyplot as plt
from xbx.coin_alpha.program.backtest.Function import *
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


# ===参数设定
select_coin_num = 1  # 选币数量
c_rate = 4 / 10000  # 手续费

rtn = pd.DataFrame()

# 持币周期
hold_hour_list = ['1H', '2H', '3H', '4H', '6H', '8H', '12H', '24H']

# 计算各个指标的回溯周期
back_hour_list = [1, 2, 3, 4, 6, 8, 12, 24]

if_reverse = True

for if_reverse in [True, False]:
    for factor_name in ['涨跌幅', '振幅']:
        for back_hour in back_hour_list:
            for hold_hour in hold_hour_list:
                if back_hour < int(hold_hour[:-1]):
                    continue

                # =读取数据
                all_coin_data = pd.read_pickle(
                    root_path + '/data/backtest/output/data_for_select/all_coin_data_hold_hour_%s.pkl' % hold_hour)

                # =删除某些行数据
                all_coin_data = all_coin_data[all_coin_data['volume'] > 0]  # 该周期不交易的币种
                all_coin_data.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空

                # =遍历所有的offest
                for offset, df in all_coin_data.groupby('offset'):
                    print(back_hour, hold_hour, factor_name, offset)

                    # 选币因子
                    if if_reverse:
                        df['因子'] = -df['前%dh%s' % (back_hour, factor_name)]
                    else:
                        df['因子'] = df['前%dh%s' % (back_hour, factor_name)]

                    # 根据因子对比进行排名
                    # 从小到大排序
                    df['排名1'] = df.groupby('candle_begin_time')['因子'].rank()
                    df1 = df[df['排名1'] <= select_coin_num]
                    df1['方向'] = 1
                    # 从大到小排序
                    df['排名2'] = df.groupby('candle_begin_time')['因子'].rank(ascending=False)
                    df2 = df[df['排名2'] <= select_coin_num]
                    df2['方向'] = -1
                    # 合并排序结果
                    df = pd.concat([df1, df2], ignore_index=True)
                    df.sort_values(by=['candle_begin_time', '方向'], inplace=True)
                    df['本周期涨跌幅'] = -(1 * c_rate) + 1 * (1 + (df['下个周期_avg_price'] /
                                                             df['avg_price'] - 1) * df['方向']) * (1 - c_rate) - 1

                    # 整理选中币种数据
                    select_coin = pd.DataFrame()
                    df['symbol'] += ' '
                    select_coin['做多币种'] = df[df['方向'] == 1].groupby('candle_begin_time')['symbol'].sum()
                    select_coin['做空币种'] = df[df['方向'] == -1].groupby('candle_begin_time')['symbol'].sum()
                    select_coin['本周期多空涨跌幅'] = df.groupby('candle_begin_time')['本周期涨跌幅'].mean()

                    # 计算整体资金曲线
                    select_coin.reset_index(inplace=True)
                    select_coin['资金曲线'] = (select_coin['本周期多空涨跌幅'] + 1).cumprod()

                    # 计算最大回撤
                    select_coin['max2here'] = select_coin['资金曲线'].expanding().max()
                    select_coin['dd2here'] = select_coin['资金曲线'] / select_coin['max2here'] - 1
                    end_date, max_draw_down = tuple(select_coin.sort_values(
                        by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])

                    # 统计策略收益
                    l = len(rtn)
                    rtn.loc[l, 'factor_name'] = factor_name
                    rtn.loc[l, 'if_reverse'] = if_reverse
                    rtn.loc[l, 'back_hour'] = back_hour
                    rtn.loc[l, 'hold_hour'] = hold_hour
                    rtn.loc[l, 'offset'] = offset
                    rtn.loc[l, '最终收益'] = select_coin.iloc[-1]['资金曲线']
                    rtn.loc[l, '最大回撤'] = max_draw_down
                    print(rtn)

print(rtn)
rtn.to_csv('rtn.csv')
