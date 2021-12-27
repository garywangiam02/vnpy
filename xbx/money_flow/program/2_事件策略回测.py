"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
事件小组基础代码
"""
import sys

from xbx.money_flow.program.Function import *
from xbx.money_flow.program.Config import *
from xbx.money_flow.program.Evaluate import *
from datetime import datetime

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

not_ergodic = True

if len(sys.argv) == 3:
    hold_period, stk_num_limit = sys.argv[1:]
    if stk_num_limit == 'None':
        stk_num_limit = None
    else:
        stk_num_limit = int(stk_num_limit)
    hold_period = int(hold_period)
    max_cap_num = hold_period
    # not_ergodic = False
    print('\n事件名称：%s  持有天数：%s  选股数：%s' % (event, hold_period, stk_num_limit))

# =====读取数据
all_stock_data = pd.read_pickle(root_path + '/data/数据整理/all_stock_data.pkl')
all_stock_data = all_stock_data[all_stock_data[event] == 1]  # 只保留选中的事件
# 导入指数数据
index_df = import_index_data(root_path + '/data/trade_data/index/sh000300.csv', date_start, date_end)

# 如果不是进行循环就统计事件的频率
if not_ergodic:
    # 统计事件的频率
    freq_res = frequency_statistics(all_stock_data, index_df, event)
    print(freq_res.to_markdown())

# =====当一个周期有多个股票的时候，按照排序规则，保留指定数量的股票
if stk_num_limit:
    # 排序方法可以有很多很多：甚至所有的选股策略都可以作为此处的排序方法。
    all_stock_data['factor_rank'] = all_stock_data.groupby('交易日期')[rank_factor].rank(method='first',
                                                                                     ascending=ascending)
    all_stock_data = all_stock_data[all_stock_data['factor_rank'] <= stk_num_limit]
    del all_stock_data['factor_rank']

# =====将一个周期的多个股票，转换到一行中。
all_stock_data['股票代码'] += ' '
group = all_stock_data.groupby('交易日期')
day_event_df = pd.DataFrame()
day_event_df['股票数量'] = group['股票代码'].size()
day_event_df['买入股票代码'] = group['股票代码'].sum()

# =====计算买入每天所有事件股票后的资金曲线
# 第一天的涨跌幅，替换为开盘买入涨跌幅
day_event_df['持仓每日净值'] = group['未来N日涨跌幅'].apply(cal_today_stock_cap_line, hold_period=hold_period)
# 扣除买入手续费
day_event_df['持仓每日净值'] = day_event_df['持仓每日净值'].apply(lambda x: np.array(x) * (1 - c_rate))
# 扣除卖出手续费
day_event_df['持仓每日净值'] = day_event_df['持仓每日净值'].apply(lambda x: list(x[:-1]) + [x[-1] * (1 - c_rate - t_rate)])

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
    # print(i, date.date(), tag, left_days_list)

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

# ===== 评估策略
# 1、评估前的准备
# 计算净值 = 总资金 / 资金分数
df['净值'] = df['总资金'] / max_cap_num
# 计算基准的净值
df['基准净值'] = (index_df['指数涨跌幅'] + 1).cumprod()
# 计算资金使用率 = 每份资金的总额 / 总资金
df['资金使用率'] = (df[cap_num_cols].sum(axis=1) / df['总资金']).apply(float)

# 2、计算资金曲线的各项评估指标(result) & 每年（月、季）的超额收益（excess return）
res, etn = evaluate_investment_for_event_driven(df, day_event_df, date_col='交易日期', rule_type='A')
print(res.to_markdown())
print(etn.to_markdown())

# 3、绘制资金曲线
pic_title = '事件：%s 持有期：%s 资金份数：%s 持股数:%s' % (event, hold_period, max_cap_num, stk_num_limit)
draw_equity_curve(df, date_col='交易日期', data_dict={'策略表现': '净值', '沪深300': '基准净值'},
                  right_axis={'资金使用率': '资金使用率'}, title=pic_title)

# 如果不是进行循环就统计最大盈亏
if not_ergodic:
    # 4、计算资金曲线的盈利（亏损）最大的交易
    profit_max, loss_max = get_max_trade(all_stock_data, df, hold_period, view_count=5)
    print(profit_max.to_markdown())
    print(loss_max.to_markdown())

back_test_path = root_path + '/data/回测结果/回测详情/回测结果_%s_%s_%s_%s.csv' % (event, hold_period, max_cap_num, stk_num_limit)
df.to_csv(back_test_path, encoding='gbk', index=False)

# 遍历结果
ergodic = res.T.copy()
ergodic.loc[0, '事件名称'] = event
ergodic.loc[0, '持有天数'] = hold_period
ergodic.loc[0, '选股数'] = str(stk_num_limit)

ergodic = ergodic[['事件名称', '持有天数', '选股数', '累积净值', '年化收益', '最大回撤', '年化收益/回撤比', '资金使用率_mean', '年化收益/资金占用',
                   '每笔交易平均盈亏', '胜率', '盈亏比', '最大回撤开始时间', '最大回撤结束时间']]
ergodic_path = root_path + '/data/回测结果/遍历回测.csv'
if os.path.exists(ergodic_path):
    ergodic.to_csv(ergodic_path, encoding='gbk', index=False, header=False, mode='a')
else:
    ergodic.to_csv(ergodic_path, encoding='gbk', index=False)
