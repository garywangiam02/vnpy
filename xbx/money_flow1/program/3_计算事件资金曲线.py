"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
12 事件策略 案例：中户资金流
"""
import pandas as pd
from program.Function import *
from program.Config import *
from datetime import datetime
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数

# =====导入数据
day_event_df = pd.read_pickle(root_path + '/data/数据整理/day_event_df.pkl')
index_df = import_index_data(root_path + '/data/trade_data/index/sh000300.csv', date_start, date_end)

# =====创建df来记录持仓信息、资金曲线
df = pd.DataFrame()
df['交易日期'] = sorted(index_df['交易日期'].tolist())[1:]  # 指数的所有交易日期
df[['总资金', '可用资金', '在投份数']] = None
df['投出资金'] = 0

# 给每份资金创建对应的列
cap_num_cols = []  # 用于循环，记录资金曲线的列
left_days_cols = []  # 用于循环，记录持仓时间的列
for i in range(1, max_cap_num + 1):
    df[['%s_资金' % i, '%s_余数' % i]] = 0
    df['%s_股票' % i] = None
    cap_num_cols.append('%s_资金' % i)
    left_days_cols.append('%s_余数' % i)


# =====每个循环每个周期
start_time = datetime.now()
print('=' * 20, '开始循环每个周期', '=' * 20)
for i in df.index:

    # ===获取当周期的发生的事件
    date = df.at[i, '交易日期']
    today_event = day_event_df[day_event_df.index == date].copy()
    tag = '无事件；' if today_event.empty else '有事件；'

    # ===计算当日结束投资的资金编号、用于投资的资金编号，
    due_cap_num, next_cap_num = None, None  # 当日结束投资的资金编号、用于投资的资金编号
    left_days_list = list(df.loc[i, left_days_cols])
    if 1 in left_days_list:  # 如果有资金到期
        due_cap_num = left_days_list.index(1) + 1  # 一天不可能有>=2份资金到期
        tag += '%d资金到期；' % due_cap_num
    if 0 in left_days_list:  # 如果有可投资资金资金
        next_cap_num = left_days_list.index(0) + 1
        tag += '%d资金待使用；' % next_cap_num
    print(i, date.date(), tag, left_days_list)

    # ===处理第一个周期的特殊情况:
    if i == 0:
        # 如果第一天有开仓
        if not today_event.empty:
            # 更新资金曲线
            df.loc[i, '投出资金'] = 1
            _df = update_df(today_event, start_index=i, cap_num=1, cap=1)
            df.update(_df)
            # 更新当日资金
            df.loc[i, '在投份数'] = 1
            df.loc[i, '可用资金'] = float(max_cap_num) - 1
            df.loc[i, '总资金'] = df.loc[i, cap_num_cols + ['可用资金']].sum()
        else:
            # 更新当日资金
            df.loc[i, '在投份数'] = 0
            df.loc[i, '总资金'] = float(max_cap_num)
            df.loc[i, '可用资金'] = float(max_cap_num)

    # ===非第一个周期的情况
    if i != 0:
        # 如果有资金开仓
        if (df.loc[i - 1, '在投份数'] < max_cap_num) & (not today_event.empty):
            # 计算当前每份投出多少钱 = min(总资金/资金分数，可用资金/可投份数）
            cap1 = df.loc[i - 1, '总资金'] / max_cap_num
            cap2 = df.loc[i - 1, '可用资金'] / (max_cap_num - df.loc[i - 1, '在投份数'])
            cap = min(cap1, cap2)
            df.loc[i, '投出资金'] = cap
            # 更新资金曲线
            update_info = update_df(today_event, start_index=i, cap_num=next_cap_num, cap=cap)
            df.update(update_info)

        # 更新当日资金
        if due_cap_num:  # 如果当周期有到期资金
            # 在投份数 = 上周期在投份数 - 1（到期份数） + 0或1（当周期投出份数）
            df.loc[i, '在投份数'] = df.loc[i - 1, '在投份数'] - 1 + (df.loc[i, '投出资金'] > 0)
            # 可用资金 = 上周期可用资金 + 本周期到期资金 - 本周期投出资金
            df.loc[i, '可用资金'] = df.loc[i - 1, '可用资金'] + df.loc[i, '%s_资金' % due_cap_num] - df.loc[i, '投出资金']
            # 总资金
            df.loc[i, '总资金'] = df.loc[i, cap_num_cols + ['可用资金']].sum()
            df.loc[i, '总资金'] -= df.loc[i, '%s_资金' % due_cap_num]  # 减去本周期到期资金（重复计算了一次，已经并入了可用资金）
        else:  # 如果当周期没有有到期资金
            # 在投份数 = 上周期在投份数 + 0或1（当周期投出份数）
            df.loc[i, '在投份数'] = df.loc[i - 1, '在投份数'] + (df.loc[i, '投出资金'] > 0)
            # 可用资金 = 上周期可用资金 - 本周期投出资金
            df.loc[i, '可用资金'] = df.loc[i - 1, '可用资金'] - df.loc[i, '投出资金']
            # 总资金
            df.loc[i, '总资金'] = df.loc[i, cap_num_cols + ['可用资金']].sum()

print('耗时:', datetime.now() - start_time)

print(df)
