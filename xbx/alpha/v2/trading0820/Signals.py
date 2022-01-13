"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import pandas as pd
import numpy as np
import talib as ta
import talib as ta
from fracdiff import fdiff
from datetime import datetime, timedelta


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


diff_d = [0.3, 0.5]

def add_diff(_df, _d_list, _name, _agg_dict, _agg_type, _add=True):
    """ 为 数据列 添加 差分数据列
    :param _add:
    :param _df: 原数据 DataFrame
    :param _d_list: 差分阶数 [0.3, 0.5, 0.7]
    :param _name: 需要添加 差分值 的数据列 名称
    :param _agg_dict:
    :param _agg_type:
    :param _add:
    :return: """
    if _add:
        for _d_num in _d_list:
            if len(_df) >= 12:  # 数据行数大于等于12才进行差分操作
                _diff_ar = fdiff(_df[_name], n=_d_num, window=10, mode="valid")  # 列差分，不使用未来数据
                _paddings = len(_df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
                _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
                _df[_name + f'_diff_{_d_num}'] = _diff  # 将差分数据记录到 DataFrame
            else:
                _df[_name + f'_diff_{_d_num}'] = np.nan  # 数据行数不足12的填充为空数据

            _agg_dict[_name + f'_diff_{_d_num}'] = _agg_type


# J 神的 spot 生成脚本
# 2_选币数据整理_spot.py
# 与回测因子一个最大的区别是，实盘的因子无需进行shift(1)操作，其它一样
# resample 中因子的 first 要改成 last

# 以下函数日后挪到 Signals.py
def deal_file_name(symbol_candle_data, hold_hour, run_time, c_factor, symbol):
    # print(symbol)
    back_hour_list = [3, 4, 6, 8, 9, 12, 24, 36, 48, 72, 96]

    # =获取相应币种1h的k线，深度拷贝
    df = symbol_candle_data[symbol].copy()

    # J 神的 spot 生成脚本
    # 2_选币数据整理_spot.py
    # 与回测因子一个最大的区别是，实盘的因子无需进行shift(1)操作，其它一样
    # ===计算各项选币指标
    extra_agg_dict = dict()
    # =技术指标
    # =====技术指标
    # '''
    # --- KDJ ---
    for n in back_hour_list:
        # 正常K线数据 计算 KDJ
        low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
        high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
        df[f'K_bh_{n}'] = rsv.ewm(com=2).mean()  # K
        extra_agg_dict[f'K_bh_{n}'] = 'last'
        df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D
        extra_agg_dict[f'D_bh_{n}'] = 'last'
        df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}']  # J
        extra_agg_dict[f'J_bh_{n}'] = 'last'

    # --- RSI ---  在期货市场很有效
    close_dif = df['close'].diff()
    df['up'] = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)
    for n in back_hour_list:
        a = df['up'].rolling(n).sum()
        b = df['down'].rolling(n).sum()
        df[f'RSI_bh_{n}'] = (a / (a + b))  # RSI
        extra_agg_dict[f'RSI_bh_{n}'] = 'last'

    # ===常见变量
    # --- 涨跌幅 ---
    for n in back_hour_list:
        df[f'涨跌幅_bh_{n}'] = df['close'].pct_change(n)
        extra_agg_dict[f'涨跌幅_bh_{n}'] = 'last'

    # --- bias ---  涨跌幅更好的表达方式 bias 币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df[f'bias_bh_{n}'] = (df['close'] / ma - 1)
        extra_agg_dict[f'bias_bh_{n}'] = 'last'

    # --- 振幅 ---  最高价最低价
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df[f'振幅_bh_{n}'] = (high / low - 1)
        extra_agg_dict[f'振幅_bh_{n}'] = 'last'

    # --- 涨跌幅std ---  振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'涨跌幅std_bh_{n}'] = change.rolling(n).std()
        extra_agg_dict[f'涨跌幅std_bh_{n}'] = 'last'

    # --- 涨跌幅skew ---  在商品期货市场有效
    # skew偏度rolling最小周期为3才有数据
    for n in back_hour_list:
        df[f'涨跌幅skew_bh_{n}'] = change.rolling(n, min_periods=3).skew()
        extra_agg_dict[f'涨跌幅skew_bh_{n}'] = 'last'

    # --- 资金流入比例 --- 币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df[f'资金流入比例_bh_{n}'] = (buy_volume / volume)
        extra_agg_dict[f'资金流入比例_bh_{n}'] = 'last'

    # --- 量比 ---
    for n in back_hour_list:
        df[f'量比_bh_{n}'] = (df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean())
        extra_agg_dict[f'量比_bh_{n}'] = 'last'

    # --- 量价相关系数 ---  量价相关选股策略
    for n in back_hour_list:
        df[f'量价相关系数_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume'])
        extra_agg_dict[f'量价相关系数_bh_{n}'] = 'last'

    # --- gap ---  量价相关选股策略
    for n in back_hour_list:
        ma = df['close'].rolling(window=n).mean()
        wma = ta.WMA(df['close'], n)
        gap = wma - ma
        df[f'gap_bh_{n}'] = (gap / abs(gap).rolling(window=n).sum())
        extra_agg_dict[f'gap_bh_{n}'] = 'last'

    # --- cci ---  量价相关选股策略
    for n in back_hour_list:
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
        df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
        df[f'cci_bh_{n}'] = (df['tp'] - df['ma']) / df['md'] / 0.015
        df[f'cci_bh_{n}'] = df[f'cci_bh_{n}']
        extra_agg_dict[f'cci_bh_{n}'] = 'last'

    # --- psy ---  量价相关选股策略
    for n in back_hour_list:
        df['rtn'] = df['close'].diff()
        df['up'] = np.where(df['rtn'] > 0, 1, 0)
        df[f'psy_bh_{n}'] = df['up'].rolling(window=n).sum() / n
        df[f'psy_bh_{n}'] = df[f'psy_bh_{n}']
        extra_agg_dict[f'psy_bh_{n}'] = 'last'

    # --- cmo ---  量价相关选股策略
    for n in back_hour_list:
        df['momentum'] = df['close'] - df['close'].shift(1)
        df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
        df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
        df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
        df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
        df[f'cmo_bh_{n}'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
        df[f'cmo_bh_{n}'] = df[f'cmo_bh_{n}']
        extra_agg_dict[f'cmo_bh_{n}'] = 'last'

    # --- tr_trix ---  量价相关选股策略
    for n in back_hour_list:
        df['tr_trix'] = df['close'].ewm(span=n, adjust=False).mean()
        df[f'tr_trix_bh_{n}'] = df['tr_trix'].pct_change()
        df[f'tr_trix_bh_{n}'] = df[f'tr_trix_bh_{n}']
        extra_agg_dict[f'tr_trix_bh_{n}'] = 'last'

    for n in back_hour_list:
        df['reg_close'] = ta.LINEARREG(df['close'], timeperiod=n)  # 该部分为talib内置求线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1
        df[f'reg_bh_{n}'] = df['reg']
        extra_agg_dict[f'reg_bh_{n}'] = 'last'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name=f'reg_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='last')

    # ========以上是需要修改的代码

    # ===将数据转化为需要的周期
    df['s_time'] = df['candle_begin_time']
    df['e_time'] = df['candle_begin_time']
    df.set_index('candle_begin_time', inplace=True)

    agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'close': 'last'}
    agg_dict = dict(agg_dict, **extra_agg_dict)  # 需要保留的列
    # 不同的offset，进行resample
    period_df_list = []

    df = df[list(agg_dict.keys())]

    for offset in range(int(hold_hour[:-1])):
        # 转换周期
        period_df = df.resample(hold_hour, base=offset).agg(agg_dict)

        # >= 1.1.0
        # period_df = df.resample(hold_hour, offset=offset).agg(agg_dict)

        period_df['offset'] = offset
        # 保存策略信息到结果当中
        period_df['key'] = f'{c_factor}_{hold_hour}_{offset}H'  # 创建主键值

        # 截取指定周期的数据
        period_df = period_df[
            (period_df['s_time'] <= run_time - timedelta(hours=int(hold_hour[:-1]))) &
            (period_df['s_time'] > run_time - 2 * timedelta(hours=int(hold_hour[:-1])))
            ]

        # resample 中编写指标
        # period_df.reset_index(inplace=True)
        # 合并数据
        period_df_list.append(period_df)

    # 将不同offset的数据，合并到一张表
    period_df = pd.concat(period_df_list)
    return period_df