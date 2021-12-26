"""
邢不行2020策略分享会
0705数字货币多空选币中性策略
邢不行微信：xbx9025
"""
import pandas as pd
import glob
import os
import itertools
import numpy as np
import matplotlib.pyplot as plt
from xbx.coin_alpha.program.backtest.Function import *
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# ===参数设定
select_coin_num = 1  # 选币数量
c_rate = 4 / 10000  # 手续费

rtn = pd.DataFrame()

hold_hour = '2H'  # 持币周期
offset = 0
back_hour = 12  # 计算各个指标的回溯周期
factor_name = 'momentum'
if_reverse = True
print(back_hour, hold_hour, factor_name, offset)

# =读取数据
all_coin_data = pd.read_pickle(root_path + '/data/backtest/output/data_for_select/all_coin_data_hold_hour_%s.pkl' % hold_hour)
all_coin_data = all_coin_data[all_coin_data['offset'] == offset]


# =删除某些行数据
all_coin_data = all_coin_data[all_coin_data['volume'] > 0]  # 该周期不交易的币种
all_coin_data.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空

df = all_coin_data

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
df['本周期涨跌幅'] = -(1 * c_rate) + 1 * (1 + (df['下个周期_avg_price'] / df['avg_price'] - 1) * df['方向']) * (1 - c_rate) - 1

# 整理选中币种数据
select_coin = pd.DataFrame()
df['symbol'] += ' '
select_coin['做多币种'] = df[df['方向'] == 1].groupby('candle_begin_time')['symbol'].sum()
select_coin['做空币种'] = df[df['方向'] == -1].groupby('candle_begin_time')['symbol'].sum()
select_coin['本周期多空涨跌幅'] = df.groupby('candle_begin_time')['本周期涨跌幅'].mean()

# 计算整体资金曲线
select_coin.reset_index(inplace=True)
select_coin['资金曲线'] = (select_coin['本周期多空涨跌幅'] + 1).cumprod()
print(select_coin)

# =====计算统计指标
results = pd.DataFrame()
results.loc[0, '累积净值'] = round(select_coin['资金曲线'].iloc[-1], 2)

# ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
# 计算当日之前的资金曲线的最高点
select_coin['max2here'] = select_coin['资金曲线'].expanding().max()
# 计算到历史最高值到当日的跌幅，drowdwon
select_coin['dd2here'] = select_coin['资金曲线'] / select_coin['max2here'] - 1
# 计算最大回撤，以及最大回撤结束时间
end_date, max_draw_down = tuple(select_coin.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
# 计算最大回撤开始时间
start_date = select_coin[select_coin['candle_begin_time'] <= end_date].sort_values(by='资金曲线', ascending=False).iloc[0][
    'candle_begin_time']
# 将无关的变量删除
select_coin.drop(['max2here', 'dd2here'], axis=1, inplace=True)
results.loc[0, '最大回撤'] = format(max_draw_down, '.2%')
results.loc[0, '最大回撤开始时间'] = str(start_date)
results.loc[0, '最大回撤结束时间'] = str(end_date)

# ===统计每个周期
results.loc[0, '盈利周期数'] = len(select_coin.loc[select_coin['本周期多空涨跌幅'] > 0])  # 盈利笔数
results.loc[0, '亏损周期数'] = len(select_coin.loc[select_coin['本周期多空涨跌幅'] <= 0])  # 亏损笔数
results.loc[0, '胜率'] = format(results.loc[0, '盈利周期数'] / len(select_coin), '.2%')  # 胜率
results.loc[0, '每周期平均收益'] = format(select_coin['本周期多空涨跌幅'].mean(), '.2%')  # 每笔交易平均盈亏
results.loc[0, '盈亏收益比'] = round(select_coin.loc[select_coin['本周期多空涨跌幅'] > 0]['本周期多空涨跌幅'].mean() /
                                select_coin.loc[select_coin['本周期多空涨跌幅'] <= 0]['本周期多空涨跌幅'].mean() * (-1), 2)  # 盈亏比
results.loc[0, '单周期最大盈利'] = format(select_coin['本周期多空涨跌幅'].max(), '.2%')  # 单笔最大盈利
results.loc[0, '单周期大亏损'] = format(select_coin['本周期多空涨跌幅'].min(), '.2%')  # 单笔最大亏损

# ===连续盈利亏损
results.loc[0, '最大连续盈利周期数'] = max([len(list(v)) for k, v in itertools.groupby(
    np.where(select_coin['本周期多空涨跌幅'] > 0, 1, np.nan))])  # 最大连续盈利次数
results.loc[0, '最大连续亏损周期数'] = max([len(list(v)) for k, v in itertools.groupby(
    np.where(select_coin['本周期多空涨跌幅'] <= 0, 1, np.nan))])  # 最大连续亏损次数

# ===每年、每月收益率
select_coin.set_index('candle_begin_time', inplace=True)
year_return = select_coin[['本周期多空涨跌幅']].resample(rule='A').apply(lambda x: (1 + x).prod() - 1)
monthly_return = select_coin[['本周期多空涨跌幅']].resample(rule='M').apply(lambda x: (1 + x).prod() - 1)

print(results.T)
print(year_return)
print(monthly_return)

# ===画图
plt.plot(select_coin['资金曲线'])
plt.legend(loc='best')
plt.show()
