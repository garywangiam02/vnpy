"""
中性策略信号配置
"""
import pandas as pd
import numpy as np
import talib as ta
from Evaluate import sharpe_annual,equity_curve_with_long_and_short

def transfer_to_period_data(df, rule_type='15T'):
    """
    将数据转换为其他周期的数据
    :param df:
    :param rule_type:
    :return:
    """

    # =====转换为其他分钟数据
    period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
        {'open': 'first',
         'high': 'max',
         'low': 'min',
         'close': 'last',
         'volume': 'sum',
         })
    period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
    period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
    period_df.reset_index(inplace=True)
    df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]

    return df


# 振幅策略
def signal_zhenfu(df, n=9):
    """
    振幅策略：选择近期振幅低的股票
    """

    # high = df['high'].rolling(n, min_periods=1).max()
    # low = df['low'].rolling(n, min_periods=1).min()
    high = df['close'].rolling(n, min_periods=1).max()
    low = df['close'].rolling(n, min_periods=1).min()
    df['zhenfu'] = high / low - 1

    return df


# 反转策略
def signal_contrarian(df, n=1):
    """
    反转策略：选择最近一段时间涨幅小的币种
    """
    df['contrarian'] = df['close'].pct_change(n)
    return df

# ------------------ #

def signal_bias(df, n=1):
    ma = df['close'].rolling(n, min_periods=1).mean()
    df['bias'] = (df['close'] / ma - 1)
    return df


def signal_cci(df, n=1):
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
    df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
    df['cci'] = (df['tp'] - df['ma']) / df['md'] / 0.015
    return df


def signal_adapt_boll_v3_sharpe(df, para=0, rule_type = '60T'):
    """
    adapt_boll_v3 sharpe
    """
    n1 = int(para)
    n2 = int(37)*n1
    stop_loss_pct = 10

    df['median'] = df['close'].rolling(window=n2).mean()
    df['std'] = df['close'].rolling(n2, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['z_score'] = abs(df['close'] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n2).mean()
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
    df[indicator] = df[indicator] * df['mtm_atr']
    df[indicator] = df[indicator] * df['mtm_atr_mean']
    df[indicator] = df[indicator] * df['wd_atr']

    # 对新策略因子计算自适应布林
    df['median'] = df[indicator].rolling(window=n1).mean()
    df['std'] = df[indicator].rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
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


    # ===考察是否需要止盈止损
    info_dict = {'pre_signal': 0, 'stop_lose_price': None}  # 用于记录之前交易信号，以及止损价格

    # 逐行遍历df，考察每一行的交易信号
    for i in range(df.shape[0]):
        # 如果之前是空仓
        if info_dict['pre_signal'] == 0:
            # 当本周期有做多信号
            if df.at[i, 'signal_long'] == 1:
                df.at[i, 'signal'] = 1  # 将真实信号设置为1
                # 记录当前状态
                pre_signal = 1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格。也可以用下周期的开盘价df.at[i+1, 'open']，但是此时需要注意i等于最后一个i时，取i+1会报错
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}
            # 当本周期有做空信号
            elif df.at[i, 'signal_short'] == -1:
                df.at[i, 'signal'] = -1  # 将真实信号设置为-1
                # 记录相关信息
                pre_signal = -1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}
            # 无信号
            else:
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None}

        # 如果之前是多头仓位
        elif info_dict['pre_signal'] == 1:
            # 当本周期有平多仓信号，或者需要止损
            if (df.at[i, 'signal_long'] == 0) or (df.at[i, 'close'] < info_dict['stop_lose_price']):
                df.at[i, 'signal'] = 0  # 将真实信号设置为0
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None}

            # 当本周期有平多仓并且还要开空仓
            if df.at[i, 'signal_short'] == -1:
                df.at[i, 'signal'] = -1  # 将真实信号设置为-1
                # 记录相关信息
                pre_signal = -1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}

        # 如果之前是空头仓位
        elif info_dict['pre_signal'] == -1:
            # 当本周期有平空仓信号，或者需要止损
            if (df.at[i, 'signal_short'] == 0) or (df.at[i, 'close'] > info_dict['stop_lose_price']):
                df.at[i, 'signal'] = 0  # 将真实信号设置为0
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None}

            # 当本周期有平空仓并且还要开多仓
            if df.at[i, 'signal_long'] == 1:
                df.at[i, 'signal'] = 1  # 将真实信号设置为1
                # 记录相关信息
                pre_signal = 1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}

        # 其他情况
        else:
            raise ValueError('不可能出现其他的情况，如果出现，说明代码逻辑有误，报错')

    # 将无关的变量删除
    # df.drop(['median', 'std', 'upper', 'lower', 'signal_long', 'signal_short', 'wd'], axis=1, inplace=True)

    # ===由signal计算出实际的每天持有仓位
    # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(method='ffill', inplace=True)
    df['pos'].fillna(value=0, inplace=True)  # 将初始行数的position补全为0

    df.drop(['signal_long', 'signal_short', 'atr', 'z_score'], axis=1,
            inplace=True)
    df = df.iloc[n2 * 2:, :]   # 母布林在初始阶段没有值,会影响持仓信号,去掉
    df = equity_curve_with_long_and_short( df, leverage_rate=1.0, c_rate=1.0 / 1000)
    equity = df
    equity["sharpe"] = sharpe_annual(equity['equity_curve'] , periods=60/int(rule_type.split('T')[0]) *24*365)
    df['adapt_boll_v3_sharpe'] = equity["sharpe"]

    return df




def signal_adapt_boll_v3_equity(df, para=0, rule_type = '60T'):
    """
    adapt_boll_v3 收益
    """
    n1 = int(para)
    n2 = int(37)*n1
    stop_loss_pct = 10

    df['median'] = df['close'].rolling(window=n2).mean()
    df['std'] = df['close'].rolling(n2, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['z_score'] = abs(df['close'] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n2).mean()
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
    df[indicator] = df[indicator] * df['mtm_atr']
    df[indicator] = df[indicator] * df['mtm_atr_mean']
    df[indicator] = df[indicator] * df['wd_atr']

    # 对新策略因子计算自适应布林
    df['median'] = df[indicator].rolling(window=n1).mean()
    df['std'] = df[indicator].rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
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


    # ===考察是否需要止盈止损
    info_dict = {'pre_signal': 0, 'stop_lose_price': None}  # 用于记录之前交易信号，以及止损价格

    # 逐行遍历df，考察每一行的交易信号
    for i in range(df.shape[0]):
        # 如果之前是空仓
        if info_dict['pre_signal'] == 0:
            # 当本周期有做多信号
            if df.at[i, 'signal_long'] == 1:
                df.at[i, 'signal'] = 1  # 将真实信号设置为1
                # 记录当前状态
                pre_signal = 1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格。也可以用下周期的开盘价df.at[i+1, 'open']，但是此时需要注意i等于最后一个i时，取i+1会报错
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}
            # 当本周期有做空信号
            elif df.at[i, 'signal_short'] == -1:
                df.at[i, 'signal'] = -1  # 将真实信号设置为-1
                # 记录相关信息
                pre_signal = -1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}
            # 无信号
            else:
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None}

        # 如果之前是多头仓位
        elif info_dict['pre_signal'] == 1:
            # 当本周期有平多仓信号，或者需要止损
            if (df.at[i, 'signal_long'] == 0) or (df.at[i, 'close'] < info_dict['stop_lose_price']):
                df.at[i, 'signal'] = 0  # 将真实信号设置为0
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None}

            # 当本周期有平多仓并且还要开空仓
            if df.at[i, 'signal_short'] == -1:
                df.at[i, 'signal'] = -1  # 将真实信号设置为-1
                # 记录相关信息
                pre_signal = -1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 + stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}

        # 如果之前是空头仓位
        elif info_dict['pre_signal'] == -1:
            # 当本周期有平空仓信号，或者需要止损
            if (df.at[i, 'signal_short'] == 0) or (df.at[i, 'close'] > info_dict['stop_lose_price']):
                df.at[i, 'signal'] = 0  # 将真实信号设置为0
                # 记录相关信息
                info_dict = {'pre_signal': 0, 'stop_lose_price': None}

            # 当本周期有平空仓并且还要开多仓
            if df.at[i, 'signal_long'] == 1:
                df.at[i, 'signal'] = 1  # 将真实信号设置为1
                # 记录相关信息
                pre_signal = 1  # 信号
                stop_lose_price = df.at[i, 'close'] * (1 - stop_loss_pct / 100)  # 以本周期的收盘价乘以一定比例作为止损价格，也可以用下周期的开盘价df.at[i+1, 'open']
                info_dict = {'pre_signal': pre_signal, 'stop_lose_price': stop_lose_price}

        # 其他情况
        else:
            raise ValueError('不可能出现其他的情况，如果出现，说明代码逻辑有误，报错')

    # 将无关的变量删除
    # df.drop(['median', 'std', 'upper', 'lower', 'signal_long', 'signal_short', 'wd'], axis=1, inplace=True)

    # ===由signal计算出实际的每天持有仓位
    # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(method='ffill', inplace=True)
    df['pos'].fillna(value=0, inplace=True)  # 将初始行数的position补全为0

    df.drop(['signal_long', 'signal_short', 'atr', 'z_score'], axis=1,
            inplace=True)
    df = df.iloc[n2 * 2:, :]   # 母布林在初始阶段没有值,会影响持仓信号,去掉
    df = equity_curve_with_long_and_short( df, leverage_rate=1.0, c_rate=1.0 / 1000)
    df['adapt_boll_v3_equity'] =  df['equity_curve']

    return df