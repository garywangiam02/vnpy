import pandas as pd
import numpy as np
from fracdiff import fdiff
import talib

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数

import warnings

warnings.filterwarnings('ignore')


# =====函数  zscore归一化
def scale_zscore(_s, _n):
    _s = (pd.Series(_s) - pd.Series(_s).rolling(_n, min_periods=1).mean()
          ) / pd.Series(_s).rolling(_n, min_periods=1).std()
    return pd.Series(_s)


def add_diff(_df, _d_num, _name):
    """ 为 数据列 添加 差分数据列
    :param _df: 原数据 DataFrame
    :param _d_num: 差分阶数 0.3, 0.5
    :param _name: 需要添加 差分值 的数据列 名称
    :return: """
    _d_num = float(_d_num)
    if len(_df) >= 12:  # 数据行数大于等于12才进行差分操作
        _diff_ar = fdiff(_df[_name], n=_d_num, window=10, mode="valid")  # 列差分，不使用未来数据
        _paddings = len(_df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
        _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
        _df[_name + f'_diff_{_d_num}'] = _diff  # 将差分数据记录到 DataFrame
    else:
        _df[_name + f'_diff_{_d_num}'] = np.nan  # 数据行数不足12的填充为空数据


def signal_(df, n=9):
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
