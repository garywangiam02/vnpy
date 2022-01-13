import numpy as np
import pandas as pd
import talib as ta
from fracdiff import fdiff


def add_diff(_df, _d_num, _name):
    if len(_df) >= 12:  # 数据行数大于等于12才进行差分操作
        _diff_ar = fdiff(_df[_name], n=_d_num, window=10, mode="valid")  # 列差分，不使用未来数据
        _paddings = len(_df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
        _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
        _df[_name] = _diff  # 将差分数据记录到 DataFrame
    else:
        _df[_name] = np.nan  # 数据行数不足12的填充为空数据

    return _df


def signal_Bias(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = 'Bias'

    ma = df['close'].rolling(n, min_periods=1).mean()
    df[factor_name] = (df['close'] / ma - 1)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
