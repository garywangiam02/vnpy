# -*- coding: utf-8 -*-
'''
@file: Signal_cta.py

@time: 2021/8/18 23:25

@author: weixx
'''

import pandas as pd
import numpy as np

# =====计算资金曲线
# okex交割合约（usdt本位）资金曲线
def equity_curve_for_OKEx_USDT_future_next_open(df, slippage=1 / 1000, c_rate=5 / 10000, leverage_rate=3, face_value=0.01,
                                                min_margin_ratio=1/100):
    """
    okex交割合约（usdt本位）资金曲线
    开仓价格是下根K线的开盘价，可以是其他的形式
    相比之前杠杆交易的资金曲线函数，逻辑简单很多：手续费的处理、爆仓的处理等。
    在策略中增加滑点的。滑点的处理和手续费是不同的。
    :param df:
    :param slippage:  滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
    :param c_rate:  手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
    :param leverage_rate:  杠杆倍数
    :param face_value:  一张合约的面值，0.01BTC
    :param min_margin_ratio: 最低保证金率，低于就会爆仓
    :return:
    """
    if len(df) == 0:
        print('df等于0')
    # =====下根k线开盘价
    df['next_open'] = df['open'].shift(-1)  # 下根K线的开盘价
    df['next_open'].fillna(value=df['close'], inplace=True)

    # =====找出开仓、平仓的k线
    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = condition1 & condition2

    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = condition1 & condition2

    # =====对每次交易进行分组
    df.loc[open_pos_condition, 'start_time'] = df['candle_begin_time']
    df['start_time'].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, 'start_time'] = pd.NaT

    # =====开始计算资金曲线
    initial_cash = 10000  # 初始资金，默认为10000元
    # ===在开仓时
    # 在open_pos_condition的K线，以开盘价计算买入合约的数量。（当资金量大的时候，可以用5分钟均价）
    df.loc[open_pos_condition, 'contract_num'] = initial_cash * leverage_rate / (face_value * df['open'])
    df['contract_num'] = np.floor(df['contract_num'])  # 对合约张数向下取整
    # 开仓价格：理论开盘价加上相应滑点
    df.loc[open_pos_condition, 'open_pos_price'] = df['open'] * (1 + slippage * df['pos'])
    # 开仓之后剩余的钱，扣除手续费
    df['cash'] = initial_cash - df['open_pos_price'] * face_value * df['contract_num'] * c_rate  # 即保证金

    # ===开仓之后每根K线结束时
    # 买入之后cash，contract_num，open_pos_price不再发生变动
    for _ in ['contract_num', 'open_pos_price', 'cash']:
        df[_].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, ['contract_num', 'open_pos_price', 'cash']] = None

    # ===在平仓时
    # 平仓价格
    df.loc[close_pos_condition, 'close_pos_price'] = df['next_open'] * (1 - slippage * df['pos'])
    # 平仓之后剩余的钱，扣除手续费
    df.loc[close_pos_condition, 'close_pos_fee'] = df['close_pos_price'] * face_value * df['contract_num'] * c_rate

    # ===计算利润
    # 开仓至今持仓盈亏
    df['profit'] = face_value * df['contract_num'] * (df['close'] - df['open_pos_price']) * df['pos']
    # 平仓时理论额外处理
    df.loc[close_pos_condition, 'profit'] = face_value * df['contract_num'] * (
            df['close_pos_price'] - df['open_pos_price']) * df['pos']
    # 账户净值
    df['net_value'] = df['cash'] + df['profit']

    # ===计算爆仓
    # 至今持仓盈亏最小值
    df.loc[df['pos'] == 1, 'price_min'] = df['low']
    df.loc[df['pos'] == -1, 'price_min'] = df['high']
    df['profit_min'] = face_value * df['contract_num'] * (df['price_min'] - df['open_pos_price']) * df['pos']
    # 账户净值最小值
    df['net_value_min'] = df['cash'] + df['profit_min']
    # 计算保证金率
    df['margin_ratio'] = df['net_value_min'] / (face_value * df['contract_num'] * df['price_min'])
    # 计算是否爆仓
    df.loc[df['margin_ratio'] <= (min_margin_ratio + c_rate), '是否爆仓'] = 1
    # df.loc[df['margin_ratio'] <= (min_margin_ratio + c_rate), '是否爆仓'] = 0

    # ===平仓时扣除手续费
    df.loc[close_pos_condition, 'net_value'] -= df['close_pos_fee']
    # 应对偶然情况：下一根K线开盘价格价格突变，在平仓的时候爆仓。此处处理有省略，不够精确。
    df.loc[close_pos_condition & (df['net_value'] < 0), '是否爆仓'] = 1
    # df.loc[close_pos_condition & (df['net_value'] < 0), '是否爆仓'] = 0

    # ===对爆仓进行处理
    df['是否爆仓'] = df.groupby('start_time')['是否爆仓'].fillna(method='ffill')
    df.loc[df['是否爆仓'] == 1, 'net_value'] = 0

    # =====计算资金曲线
    df['equity_change'] = df['net_value'].pct_change()
    df.loc[open_pos_condition, 'equity_change'] = df.loc[open_pos_condition, 'net_value'] / initial_cash - 1  # 开仓日的收益率
    df['equity_change'].fillna(value=0, inplace=True)
    df['equity_curve'] = (1 + df['equity_change']).cumprod()

    # =====删除不必要的数据，并存储
    df.drop(['next_open', 'contract_num', 'open_pos_price', 'cash', 'close_pos_price', 'close_pos_fee',
             'profit', 'net_value', 'price_min', 'profit_min', 'net_value_min', 'margin_ratio', '是否爆仓'],
            axis=1, inplace=True)

    return df


def return_drawdown_ratio(equity_curve):
    """
    :param equity_curve: 带资金曲线的df
    :param trade: transfer_equity_curve_to_trade的输出结果，每笔交易的df
    :return:
    """

    # ===计算年化收益
    annual_return = (equity_curve['equity_curve'].iloc[-1] / equity_curve['equity_curve'].iloc[0]) ** (
        '1 days 00:00:00' / (equity_curve['candle_begin_time'].iloc[-1] - equity_curve['candle_begin_time'].iloc[0]) * 365) - 1

    # ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
    # 计算当日之前的资金曲线的最高点
    equity_curve['max2here'] = equity_curve['equity_curve'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    equity_curve['dd2here'] = equity_curve['equity_curve'] / equity_curve['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(equity_curve.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])

    # ===年化收益/回撤比
    sharpe = annual_return / abs(max_draw_down)


    return annual_return, max_draw_down, sharpe

def signal_adaptboll_with_mtm_v3(df, back_hour=11, start_time='2021-01-01'):

    n1 = int(back_hour)
    n2 = 35 * n1
    df['median'] = df['close'].rolling(window=n2, min_periods=1).mean()
    df['std'] = df['close'].rolling(n2, min_periods=1).apply(np.std)  # ddof代表标准差自由度
    df['z_score'] = abs(df['close'] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n2, min_periods=1).mean()
    df['upper'] = df['median'] + df['std'] * df['m']
    df['lower'] = df['median'] - df['std'] * df['m']

    condition_long = df['close'] > df['upper']
    condition_short = df['close'] < df['lower']

    df['mtm'] = df['close'] / df['close'].shift(n1) - 1
    df['mtm_mean'] = df['mtm'].rolling(window=n1, min_periods=1).mean()

    # 基于价格atr，计算波动率因子wd_atr
    df['c1'] = df['high'] - df['low']
    df['c2'] = abs(df['high'] - df['close'].shift(1))
    df['c3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=n1, min_periods=1).mean()
    df['avg_price'] = df['close'].rolling(window=n1, min_periods=1).mean()
    df['wd_atr'] = df['atr'] / df['avg_price']

    # 参考ATR，对MTM指标，计算波动率因子
    df['mtm_l'] = df['low'] / df['low'].shift(n1) - 1
    df['mtm_h'] = df['high'] / df['high'].shift(n1) - 1
    df['mtm_c'] = df['close'] / df['close'].shift(n1) - 1
    df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
    df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
    df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
    df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
    df['mtm_atr'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

    # 参考ATR，对MTM mean指标，计算波动率因子
    df['mtm_l_mean'] = df['mtm_l'].rolling(window=n1, min_periods=1).mean()
    df['mtm_h_mean'] = df['mtm_h'].rolling(window=n1, min_periods=1).mean()
    df['mtm_c_mean'] = df['mtm_c'].rolling(window=n1, min_periods=1).mean()
    df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
    df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
    df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
    df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
    df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

    indicator = 'mtm_mean'

    # mtm_mean指标分别乘以三个波动率因子
    df[indicator] = df[indicator] * df['mtm_atr'] * 1e6
    df[indicator] = df[indicator] * df['mtm_atr_mean']
    df[indicator] = df[indicator] * df['wd_atr']

    # 对新策略因子计算自适应布林
    df['median'] = df[indicator].rolling(window=n1, min_periods=1).mean()
    df['std'] = df[indicator].rolling(n1, min_periods=1).apply(np.std)  # ddof代表标准差自由度
    df['z_score'] = abs(df[indicator] - df['median']) / df['std']
    # df['m'] = df['z_score'].rolling(window=n1).max().shift(1)
    # df['m'] = df['z_score'].rolling(window=n1).mean()
    df['m'] = df['z_score'].rolling(window=n1).min().shift(1)
    df['up'] = df['median'] + df['std'] * df['m']
    df['dn'] = df['median'] - df['std'] * df['m']

    # 突破上轨做多
    condition1 = df[indicator] > df['up']
    condition2 = df[indicator].shift(1) <= df['up'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_long'] = 1

    # 突破下轨做空
    condition1 = df[indicator] < df['dn']
    condition2 = df[indicator].shift(1) >= df['dn'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_short'] = -1

    # 均线平仓(多头持仓)
    condition1 = df[indicator] < df['median']
    condition2 = df[indicator].shift(1) >= df['median'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_long'] = 0

    # 均线平仓(空头持仓)
    condition1 = df[indicator] > df['median']
    condition2 = df[indicator].shift(1) <= df['median'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_short'] = 0

    df.loc[condition_long, 'signal_short'] = 0
    df.loc[condition_short, 'signal_long'] = 0

    # ===由signal计算出实际的每天持有仓位
    # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
    df['signal_short'].fillna(method='ffill', inplace=True)
    df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1)
    df['signal'].fillna(value=0, inplace=True)
    # df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1,
    #                                                        skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    df.drop(['signal_long', 'signal_short', 'atr', 'z_score'], axis=1,
            inplace=True)
    # 计算对应的目标函数
    # 在产生signal的k线结束的时候，进行买入
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(value=0, inplace=True)  # 将初始行数的pos补全为0
    # 时间过滤
    df = df[pd.to_datetime(df['candle_begin_time']) >= pd.to_datetime(start_time)]
    try:
        # 计算资金曲线
        df = equity_curve_for_OKEx_USDT_future_next_open(df, slippage=0.001, c_rate=0.0006,
                                                          leverage_rate=1,
                                                          face_value=1,
                                                          min_margin_ratio=1 / 100)

        annual_return, max_draw_down, sharpe = return_drawdown_ratio(df) # 计算年化收益回撤比
    except Exception as e:
        msg = f'K线不足 :{df.shape}' + str(e)
        print(msg)
        annual_return = max_draw_down = sharpe = 0

    return annual_return, max_draw_down, sharpe
