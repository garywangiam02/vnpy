import pandas as pd
import numpy as np
import itertools
import matplotlib.pyplot as plt


# 绘制策略曲线
def draw_equity_curve(df, data_dict, date_col=None, right_axis=None, pic_size=[16, 9], dpi=72, font_size=25, log=False,
                      chg=False, title=None, y_label='净值'):
    """
    绘制策略曲线
    :param df: 包含净值数据的df
    :param data_dict: 要展示的数据字典格式：｛图片上显示的名字:df中的列名｝
    :param date_col: 时间列的名字，如果为None将用索引作为时间列
    :param right_axis: 右轴数据 ｛图片上显示的名字:df中的列名｝
    :param pic_size: 图片的尺寸
    :param dpi: 图片的dpi
    :param font_size: 字体大小
    :param chg: datadict中的数据是否为涨跌幅，True表示涨跌幅，False表示净值
    :param log: 是都要算对数收益率
    :param title: 标题
    :param y_label: Y轴的标签
    :return:
    """
    # 复制数据
    draw_df = df.copy()
    # 模块基础设置
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False
    plt.tick_params(labelsize=font_size)
    # plt.style.use('dark_background')
    fig = plt.figure(figsize=(pic_size[0], pic_size[1]), dpi=dpi)
    ax_l = fig.add_subplot(111)
    # 获取时间轴
    if date_col:
        time_data = draw_df[date_col]
    else:
        time_data = draw_df.index
    # 绘制左轴数据
    for key in data_dict:
        if chg:
            draw_df[data_dict[key]] = (draw_df[data_dict[key]] + 1).fillna(1).cumprod()
        if log:
            draw_df[data_dict[key]] = np.log(draw_df[data_dict[key]])
        ax_l.plot(time_data, draw_df[data_dict[key]], linewidth=2, label=str(key))
    # 设置坐标轴信息等
    ax_l.set_ylabel(y_label, fontsize=font_size)
    ax_l.legend(loc=0, fontsize=font_size)
    plt.tick_params(labelsize=font_size)
    ax_l.grid()
    if title:
        plt.title(title, fontsize=font_size)

    # 绘制右轴数据
    if right_axis:
        # 生成右轴
        ax_r = ax_l.twinx()
        # 获取数据
        key = list(right_axis.keys())[0]
        ax_r.plot(time_data, draw_df[right_axis[key]], 'y', linewidth=1, label=str(key))
        # 设置坐标轴信息等
        ax_r.legend(loc=0)
        ax_r.set_ylabel(key, fontsize=font_size)
        ax_r.legend(loc=1, fontsize=font_size)
        plt.tick_params(labelsize=font_size)
    plt.show()
    # fig.show()


def frequency_statistics(all_data, index_df, event, date_col='交易日期'):
    """
    统计事件发生的频率
    :param all_data:读取all_stock_data的数据
    :param index_df:指数数据
    :param event:事件名称
    :param date_col:时间列
    :return:
    """
    df = all_data.copy()

    # 统计每天发生事件的次数
    df = df.groupby([date_col])[[event]].sum()

    # 将事件和指数数据merge合并
    df = pd.merge(left=index_df, right=df[event], on=[date_col], how='left')
    df.fillna(value=0, inplace=True)  # 将没有发生事件的日期填充为0

    # 统计事件的数据
    result = pd.DataFrame()
    result.loc[event, '总次数'] = df[event].sum()  # 计算事件的总次数
    result['日均次数'] = result['总次数'] / df.shape[0]  # 计算日均次数 = 总次数/交易日数
    result.loc[event, '最大值'] = df[event].max()  # 区间内单日发生事件的最大值
    result.loc[event, '中位数'] = df[event].median()  # 区间内单日发生事件的中位数
    result.loc[event, '无事件天数'] = (df[event] == 0).sum()  # 计算无事件天数
    result['无事件占比'] = result['无事件天数'] / df.shape[0]  # 无事件占比 = 无事件天数/交易日期

    # 计算最大连续有事件天数 & 最大连续无事件天数
    result.loc[event, '最大连续有事件天数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(df[event] > 0, 1, np.nan))])  # 最大有事件最大连续天数
    result.loc[event, '最大连续无事件天数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(df[event] == 0, 1, np.nan))])  # 最大无事件最大连续天数
    draw_equity_curve(df, date_col=date_col, data_dict={'event': event}, font_size=20, y_label='事件频率')
    return result


def evaluate_investment_for_event_driven(pos_data, day_event_df, rule_type='A', date_col='交易日期'):
    """
    计算资金曲线的各项评价指标，以及每年（月、季）的超额收益
    :param pos_data:资金曲线
    :param day_event_df:事件曲线
    :param rule_type:A 年度，Q季度，M月度
    :param date_col:交易日期
    :return:
    """
    t = pos_data.copy()
    # ===新建一个dataframe保存回测指标
    results = pd.DataFrame()

    # 将数字转为百分数
    def num_to_pct(value):
        return '%.2f%%' % (value * 100)

    # ===计算累积净值
    results.loc[0, '累积净值'] = round(t['净值'].iloc[-1], 2)

    # ===计算年化收益
    annual_return = (t['净值'].iloc[-1]) ** (
        '1 days 00:00:00' / (t[date_col].iloc[-1] - t[date_col].iloc[0]) * 365) - 1
    results.loc[0, '年化收益'] = num_to_pct(annual_return)

    # ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
    # 计算当日之前的资金曲线的最高点
    t['max2here'] = t['净值'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    t['dd2here'] = t['净值'] / t['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(t.sort_values(by=['dd2here']).iloc[0][[date_col, 'dd2here']])
    # 计算最大回撤开始时间
    start_date = t[t[date_col] <= end_date].sort_values(by='净值', ascending=False).iloc[0][
        date_col]
    # 将无关的变量删除
    t.drop(['max2here', 'dd2here'], axis=1, inplace=True)
    results.loc[0, '最大回撤'] = num_to_pct(max_draw_down)
    results.loc[0, '最大回撤开始时间'] = str(start_date)
    results.loc[0, '最大回撤结束时间'] = str(end_date)

    # ===年化收益/回撤比：我个人比较关注的一个指标
    results.loc[0, '年化收益/回撤比'] = round(annual_return / abs(max_draw_down), 2)

    # ===开始处理每笔交易的数据
    # 由于不是每天发生事件都会被买入的，所以需要筛选掉没有买入日期的事件
    buy_date = t[t['投出资金'] > 0][date_col].to_list()
    real_buy = day_event_df[day_event_df.index.isin(buy_date)].copy()

    real_buy['每笔涨跌幅'] = real_buy['持仓每日净值'].apply(lambda x: x[-1] - 1)
    # 盈利次数
    results.loc[0, '盈利次数'] = real_buy[real_buy['每笔涨跌幅'] > 0].shape[0]
    # 亏损次数
    results.loc[0, '亏损次数'] = real_buy[real_buy['每笔涨跌幅'] <= 0].shape[0]
    # 每笔交易平均盈亏
    results.loc[0, '每笔交易平均盈亏'] = num_to_pct(real_buy['每笔涨跌幅'].mean())
    # 单笔最大盈利
    results.loc[0, '单笔最大盈利'] = num_to_pct(real_buy['每笔涨跌幅'].max()) if real_buy['每笔涨跌幅'].max() >= 0 else None
    # 单笔最大亏损
    results.loc[0, '单笔最大亏损'] = num_to_pct(real_buy['每笔涨跌幅'].min()) if real_buy['每笔涨跌幅'].min() < 0 else None
    # 计算买入胜率与盈亏比
    results.loc[0, '胜率'] = num_to_pct(results.loc[0, '盈利次数'] / real_buy.shape[0])

    results.loc[0, '盈亏比'] = round(
        real_buy[real_buy['每笔涨跌幅'] > 0]['每笔涨跌幅'].mean() / real_buy[real_buy['每笔涨跌幅'] <= 0]['每笔涨跌幅'].mean() * -1, 2)

    # ===开始计算资金使用率
    describe_df = t['资金使用率'].describe()
    for i in ['mean', '25%', '50%', '75%']:
        results.loc[0, '资金使用率_' + i] = num_to_pct(describe_df[i])

    results.loc[0, '年化收益/资金占用'] = num_to_pct(annual_return / describe_df['mean'])

    # ===开始计算年度和月度的超额收益
    t = t.resample(rule=rule_type, on=date_col, ).agg({
        '净值': 'last',
        '基准净值': 'last',
    })
    # 策略年（月）度收益率
    t['策略收益率'] = t['净值'].pct_change()
    t.loc[t['策略收益率'].isna(), '策略收益率'] = t['净值'] - 1
    # 基准年（月）度收益率
    t['基准收益率'] = t['基准净值'].pct_change()
    t.loc[t['基准收益率'].isna(), '基准收益率'] = t['基准净值'] - 1
    # 策略超额收益率
    t['超额收益率'] = t['策略收益率'] - t['基准收益率']
    # 转为百分比
    t['基准收益率'] = t['基准收益率'].apply(num_to_pct)
    t['超额收益率'] = t['超额收益率'].apply(num_to_pct)
    t['策略收益率'] = t['策略收益率'].apply(num_to_pct)
    t = t[['策略收益率', '基准收益率', '超额收益率']]

    return results.T, t


def _get_hold_net_value(zdf_list, hold_period):
    """
    输入涨跌幅数据，根据持有期输出净值数据。（这是个内部函数）
    :param zdf_list: 涨跌幅数据
    :param hold_period: 持有期
    :return:
    """
    zdf_count = len(zdf_list)
    if zdf_count < hold_period:
        zdf_list = zdf_list
    else:
        zdf_list = zdf_list[:hold_period]
    net_value = np.cumprod(np.array(list(zdf_list)) + 1)
    return net_value


def get_max_trade(all_data, bt_df, hold_period, view_count=5, date_col='交易日期'):
    """
    获取回测结果中盈利（亏损）最大的几次交易
    :param all_data: 所有数据的集合
    :param bt_df: 回测的结果（back_test）
    :param hold_period: 持有期数据
    :param view_count: 需要查看的最大盈利（亏损）的个数
    :param date_col: 时间列的名字
    :return:
    """
    df = all_data.copy()
    # 先从回测结果中取出真正下单的数据
    buy_date = bt_df[bt_df['投出资金'] > 0][date_col].to_list()
    # 从all_data中保留真正下单买入的数据
    real_buy = df[df[date_col].isin(buy_date)].copy()

    # 计算每笔交易的净值
    real_buy['持仓每日净值'] = real_buy['未来N日涨跌幅'].apply(_get_hold_net_value, hold_period=hold_period)
    real_buy['最终净值'] = real_buy['持仓每日净值'].apply(lambda x: x[-1])

    # 只保留必要的列
    real_buy = real_buy[[date_col, '股票代码', '股票名称', '持仓每日净值', '最终净值']].copy()

    # 获取盈利最多的数据
    profit_max = real_buy.sort_values(by='最终净值', ascending=False).head(view_count).reset_index(drop=True)
    # 获取亏算最大的数据
    loss_max = real_buy.sort_values(by='最终净值', ascending=True).head(view_count).reset_index(drop=True)

    return profit_max, loss_max
