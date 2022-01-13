import time
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
from Signals import *
from Config import *
from Utility import robust
import configparser
import logging
import itertools
import matplotlib.pyplot as plt


# config = configparser.ConfigParser()


# =====获取数据
# 获取单个币种的1小时数据
# @robust
def fetch_binance_swap_candle_data(exchange, symbol, run_time, limit=LIMIT):
    """
    通过ccxt的接口fapiPublic_get_klines，获取永续合约k线数据
    获取单个币种的1小时数据
    :param exchange:
    :param symbol:
    :param limit:
    :param run_time:
    :return:
    """
    # 获取数据
    kline = robust(exchange.fapiPublic_get_klines, {'symbol': symbol, 'interval': '1h', 'limit': limit})

    # 将数据转换为DataFrame
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(kline, columns=columns, dtype='float')

    # 整理数据
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
    df['symbol'] = symbol  # 添加symbol列
    df['avg_price'] = df['open']
    df['下个周期_avg_price'] = df['avg_price'].shift(-1)
    columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'trade_num', 'volume', 'quote_volume',
               'taker_buy_quote_asset_volume', 'avg_price', '下个周期_avg_price']
    df = df[columns]

    # 删除runtime那行的数据，如果有的话
    df = df[df['candle_begin_time'] != run_time]
    df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
    return symbol, df


# 并行获取所有币种永续合约数据的1小时K线数据
def fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time):
    """
    并行获取所有币种永续合约数据的1小时K线数据
    :param exchange:
    :param symbol_list:
    :param run_time:
    :return:
    """
    # 创建参数列表
    arg_list = [(exchange, symbol, run_time) for symbol in symbol_list]
    s_time = time.time()

    # 多进程获取数据
    with Pool(processes=20) as pl:
        # 利用starmap启用多进程信息
        result = pl.starmap(fetch_binance_swap_candle_data, arg_list)

    df = dict(result)
    print('获取所有币种K线数据完成，花费时间：', time.time() - s_time, '\n')
    return df


# =====策略相关函数
# 选币数据整理 & 选币
def cal_factor_and_select_coin(stratagy_list, symbol_candle_data, c_rate, start_time):
    """
    :param stratagy_list:
    :param symbol_candle_data:
    :param run_time:
    :return:
    """
    no_enough_data_symbol = []

    # ===逐个遍历每一个策略
    select_coin_list = []
    for strategy in stratagy_list:
        # 获取策略参数
        c_factor = strategy['c_factor']
        hold_period = strategy['hold_period']
        selected_coin_num = strategy['selected_coin_num']
        factors = strategy['factors']

        # ===逐个遍历每一个币种，计算其因子，并且转化周期
        period_df_list = []

        _symbol_list = symbol_candle_data.keys()

        symbol_list = [symbol for symbol in _symbol_list if 'USDT' in symbol]

        for symbol in symbol_list:
            if symbol in no_enough_data_symbol:
                continue

            # =获取相应币种1h的k线，深度拷贝
            df = symbol_candle_data[symbol].copy()
            # =空数据
            if df.empty:
                print('no data', symbol)
                if symbol not in no_enough_data_symbol:
                    no_enough_data_symbol.append(symbol)
                continue

            if len(df) < 240:
                print('no enough data', symbol)
                if symbol not in no_enough_data_symbol:
                    no_enough_data_symbol.append(symbol)
                continue

            df[c_factor] = 0

            for factor_dict in factors:
                factor = factor_dict['factor']
                para = factor_dict['para']
                diff = factor_dict['diff']
                if_reverse = factor_dict['if_reverse']
                df = eval(f'signal_{factor}')(df, int(para), float(diff))  # 计算信号

                # 初始化
                df[factor + '_因子'] = np.nan

                # =空计算
                if np.isnan(df.iloc[-1][factor]):
                    continue

                if if_reverse:
                    df[factor + '_因子'] = -1 * df[factor]
                else:
                    df[factor + '_因子'] = df[factor]

            # =将数据转化为需要的周期
            # 在数据最前面，增加一行数据，这是为了在对>24h的周期进行resample时，保持数据的一致性。
            df['s_time'] = df['candle_begin_time']
            df['e_time'] = df['candle_begin_time']
            df.set_index('candle_begin_time', inplace=True)

            agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'avg_price': 'first',
                        '下个周期_avg_price': 'last', c_factor: 'last'}

            for factor_dict in factors:
                factor = factor_dict['factor']
                agg_dict[factor + '_因子'] = 'last'

            # 转换生成每个策略所有offset的因子
            for offset in range(int(hold_period[:-1])):
                # 转换周期
                period_df = df.resample(hold_period, base=offset).agg(agg_dict)
                period_df['offset'] = offset
                period_df['avg_price'] = period_df['avg_price'].shift(-1)
                period_df['下个周期_avg_price'] = period_df['下个周期_avg_price'].shift(-1)

                # 保存策略信息到结果当中
                period_df['key'] = f'{c_factor}_{hold_period}_{offset}H'  # 创建主键值
                period_df = period_df[int(192 / int(hold_period[:-1])):]
                # 合并数据
                period_df_list.append(period_df)

        # ===将不同offset的数据，合并到一张表
        df = pd.concat(period_df_list)
        df = df.sort_values(['s_time', 'symbol'])
        df[c_factor] = 0

        for factor_dict in factors:
            factor = factor_dict['factor']
            weight = factor_dict['weight']
            df[factor + '_排名'] = df.groupby('s_time')[factor + '_因子'].rank()
            df[c_factor] += df[factor + '_排名'] * weight

        # ===选币数据整理完成，接下来开始选币
        # 多空双向rank
        df['rank'] = df.groupby('s_time')[c_factor].rank(method='first')
        df['rank-1'] = df.groupby('s_time')[c_factor].rank(method='first', ascending=False)
        # 删除不要的币
        df['方向'] = 0
        df.loc[(df['rank'] <= selected_coin_num), '方向'] = 1
        df.loc[(df['rank-1'] <= selected_coin_num), '方向'] = -1
        df = df[df['方向'] != 0]
        # ===将每个币种的数据保存到dict中
        # 删除不需要的列
        df.drop(['rank'], axis=1, inplace=True)
        df.reset_index(inplace=True)
        select_coin_list.append(df)

    select_coin_all = pd.concat(select_coin_list)
    select_coin_all = select_coin_all[
        ['candle_begin_time', 'symbol', 's_time', 'e_time', 'offset', 'avg_price', '下个周期_avg_price', '方向']]

    select_coin_all = select_coin_all[select_coin_all['candle_begin_time'] >= pd.to_datetime(start_time)]
    select_coin_all = select_coin_all[:-int(hold_period[:-1]) * 4 + 2]  # 此处通过查看表格截取的
    select_coin_all.reset_index(inplace=True)
    select_coin_all.to_csv('selrct_coin.csv')

    select_coin_all['本周期涨跌幅'] = -(1 * c_rate) + 1 * (
            1 + (select_coin_all['下个周期_avg_price'] / select_coin_all['avg_price'] - 1) * select_coin_all[
        '方向']) * (1 - c_rate) - 1
    select_coin_all.sort_values(by=['candle_begin_time', '方向'], inplace=True)

    rtn_list = []
    ratio_list = []  # 盈亏比　
    return_list = []  # 净值
    select_merge_list = []
    for offset in range(int(hold_period[:-1])):
        df = select_coin_all[select_coin_all['offset'] == offset]
        # 整理选中币种数据
        select_coin = pd.DataFrame()
        # df['symbol'] += ' '
        select_coin['做多币种'] = df[df['方向'] == 1].groupby('candle_begin_time')['symbol'].sum()
        select_coin['做空币种'] = df[df['方向'] == -1].groupby('candle_begin_time')['symbol'].sum()
        select_coin['本周期多空涨跌幅'] = df.groupby('candle_begin_time')['本周期涨跌幅'].mean()

        # 计算整体资金曲线
        select_coin.reset_index(inplace=True)
        select_coin['资金曲线'] = (select_coin['本周期多空涨跌幅'] + 1).cumprod()
        # print(offset)
        rtn, select_c = cal_ind(select_coin)
        select_merge_list.append(select_c)

        if rtn is not None:
            r1 = rtn['累积净值'].values[0]
            r2 = rtn['最大回撤'].values[0]
            r2 = abs(float(r2.replace('%', '').strip()) / 100.)
            _ind = r1 / r2

            rtn_list.append(rtn)
            ratio_list.append(_ind)
            return_list.append(r1)

    select_c = pd.concat(select_merge_list, ignore_index=True)
    select_c.sort_values(by=['candle_begin_time'], inplace=True)
    select_c.reset_index(inplace=True)
    select_c['本周期实现涨跌幅'] = select_c['本周期多空涨跌幅'] / int(hold_period[:-1])
    select_c['本周期实现涨跌幅'].fillna(0, inplace=True)
    select_c['合成资金曲线'] = (select_c['本周期实现涨跌幅'] + 1).cumprod()
    del select_c['本周期多空涨跌幅']
    del select_c['资金曲线']
    select_c.rename(columns={"本周期实现涨跌幅": "本周期多空涨跌幅", "合成资金曲线": "资金曲线"}, inplace=True)

    rtn, select_c = cal_ind(select_c)

    rtn = rtn.rename(index={0: '合成'})
    rtn_list.append(rtn)
    results = pd.concat(rtn_list, ignore_index=True)

    sharp_ratio = target(select_c)
    mdd_std = 0.2
    condition = (select_c['dd2here'] >= -mdd_std) & (select_c['dd2here'].shift(1) < -mdd_std)
    select_c[f'回撤上穿{mdd_std}次数'] = 0
    select_c.loc[condition, f'回撤上穿{mdd_std}次数'] = 1
    mdd_num = int(select_c[f'回撤上穿{mdd_std}次数'].sum())

    # 画图
    plot(select_c, mdd_std, mdd_num)
    print()
    print(
        '平均盈亏比:', round(np.array(ratio_list).mean(), 2),
        ' 年化盈亏比:', round(results['年化收益/回撤比'].mean(), 2),
        ' 平均净值:', round(np.array(return_list).mean(), 2),
        ' 夏普:', sharp_ratio
    )
    print(results.to_markdown())


# =====辅助功能函数
# ===下次运行时间，和课程里面讲的函数是一样的
def next_run_time(time_interval, ahead_seconds=5, cheat_seconds=100):
    """
    根据time_interval，计算下次运行的时间，下一个整点时刻。
    目前只支持分钟和小时。
    :param time_interval: 运行的周期，15m，1h
    :param ahead_seconds: 预留的目标时间和当前时间的间隙
    :return: 下次运行的时间
    案例：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00
    10m  当前时间为：12:38:51  返回时间为：12:40:00
    5m  当前时间为：12:33:51  返回时间为：12:35:00

    5m  当前时间为：12:34:51  返回时间为：12:40:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00

    30m  当前时间为：14:37:51  返回时间为：14:56:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00

    """
    if time_interval.endswith('m') or time_interval.endswith('h'):
        pass
    elif time_interval.endswith('T'):
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):
        time_interval = time_interval.replace('H', 'h')
    else:
        print('time_interval格式不符合规范。程序exit')
        exit()
    ti = pd.to_timedelta(time_interval)
    now_time = datetime.now()
    # now_time = datetime(2019, 5, 9, 23, 50, 30)  # 修改now_time，可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)

    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break
    if cheat_seconds > 0.1:
        target_time = target_time - timedelta(seconds=cheat_seconds)
    print('程序下次运行的时间：', target_time, '\n')
    return target_time


# ===依据时间间隔, 自动计算并休眠到指定时间
def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True, cheat_seconds=120):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param if_sleep:
    :param time_interval:
    :param ahead_time:
    :return:
    """
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time, cheat_seconds)
    # sleep
    if if_sleep:
        time.sleep(max(0, (run_time - datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if datetime.now() > run_time:
                break
    run_time += timedelta(seconds=cheat_seconds)
    return run_time


def cal_ind(_select_c):
    if _select_c.empty:
        return None

    select_coin = _select_c.copy()

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
    start_date = \
        select_coin[select_coin['candle_begin_time'] <= end_date].sort_values(by='资金曲线', ascending=False).iloc[0][
            'candle_begin_time']
    # 将无关的变量删除
    # select_coin.drop(['max2here', 'dd2here'], axis=1, inplace=True)

    results.loc[0, '最大回撤'] = format(max_draw_down, '.2%')
    results.loc[0, '最大回撤开始时间'] = str(start_date)
    results.loc[0, '最大回撤结束时间'] = str(end_date)

    # ===统计每个周期
    results.loc[0, '盈利周期数'] = len(select_coin.loc[select_coin['本周期多空涨跌幅'] > 0])  # 盈利笔数
    results.loc[0, '亏损周期数'] = len(select_coin.loc[select_coin['本周期多空涨跌幅'] <= 0])  # 亏损笔数
    results.loc[0, '胜率'] = format(results.loc[0, '盈利周期数'] / len(select_coin), '.2%')  # 胜率
    results.loc[0, '每周期平均收益'] = format(select_coin['本周期多空涨跌幅'].mean(), '.2%')  # 每笔交易平均盈亏
    results.loc[0, '盈亏收益比'] = round(select_coin.loc[select_coin['本周期多空涨跌幅'] > 0]['本周期多空涨跌幅'].mean() / \
                                    select_coin.loc[select_coin['本周期多空涨跌幅'] <= 0]['本周期多空涨跌幅'].mean() * (-1), 2)  # 盈亏比
    results.loc[0, '单周期最大盈利'] = format(select_coin['本周期多空涨跌幅'].max(), '.2%')  # 单笔最大盈利
    results.loc[0, '单周期大亏损'] = format(select_coin['本周期多空涨跌幅'].min(), '.2%')  # 单笔最大亏损

    # ===连续盈利亏损
    results.loc[0, '最大连续盈利周期数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(select_coin['本周期多空涨跌幅'] > 0, 1, np.nan))])  # 最大连续盈利次数
    results.loc[0, '最大连续亏损周期数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(select_coin['本周期多空涨跌幅'] <= 0, 1, np.nan))])  # 最大连续亏损次数

    # ===计算年化收益
    # annual_return = (select_coin['资金曲线'].iloc[-1] / select_coin['资金曲线'].iloc[0]) ** (
    #    '1 days 00:00:00' / (select_coin['candle_begin_time'].iloc[-1] - select_coin['candle_begin_time'].iloc[0]) * 365) - 1
    time_during = select_coin.iloc[-1]['candle_begin_time'] - select_coin.iloc[0]['candle_begin_time']
    total_seconds = time_during.days * 24 * 3600 + time_during.seconds
    if total_seconds == 0:
        annual_return = 0
    else:
        final_r = round(select_coin['资金曲线'].iloc[-1], 2)
        annual_return = pow(final_r, 24 * 3600 * 365 / total_seconds) - 1

    results.loc[0, '年化收益'] = str(round(annual_return, 2)) + ' 倍'
    results.loc[0, '年化收益/回撤比'] = round(abs(annual_return / max_draw_down), 2)

    return results, select_coin


def plot(select_c, mdd_std, mdd_num):
    ax = plt.subplot(1, 1, 1)
    plt.subplots_adjust(hspace=0.2)  # 调整子图间距
    plt.title(f'Back draw{mdd_std} Number: {mdd_num}', fontsize='large', fontweight='bold',
              color='blue', loc='center')
    ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])
    ax2 = ax.twinx()  # 设置y轴次轴
    ax2.plot(select_c["candle_begin_time"], -select_c['dd2here'], color='red', alpha=0.4)

    # ax = plt.subplot(2, 1, 2)
    # ax.plot(select_c['candle_begin_time'], np.log(select_c['资金曲线']))
    plt.show()


def target(select_c):
    if select_c.empty:
        return 0

    select_c = select_c.copy()
    select_c.set_index('candle_begin_time', inplace=True)
    dailly_return = select_c[['资金曲线']].resample(rule='D').apply(lambda x: (1 + x).prod() - 1)
    annual_sharpe_ratio = _sharpe_annual(dailly_return)

    return annual_sharpe_ratio


def _sharpe_annual(df, periods=365):
    periods = int(periods)

    if df is not None or not df.empty:
        df['pct_chg'] = df.pct_change(periods=1)
        df['pct_chg'] = df['pct_chg'].replace([np.inf, -np.inf], np.nan)
        df['pct_chg'].fillna(value=0, inplace=True)

        mean = np.nan_to_num(np.mean(df['pct_chg']))
        std = np.nan_to_num(np.std(df['pct_chg']))

        if std == 0:
            sharpe = 0
        else:
            sharpe = np.sqrt(periods) * (mean / std)
    else:
        sharpe = 0

    return sharpe
