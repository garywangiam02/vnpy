import numpy as np
import pandas as pd
from fracdiff import fdiff
from sklearn.linear_model import LinearRegression

factors = ['BOUKENJYA_REG', ]


def adaptBollingV1(df, para=50):
    n1 = int(para)
    df = df.copy()

    # 计算动量因子
    mtm = df['close'] / df['close'].shift(n1) - 1
    mtm_mean = mtm.rolling(window=n1, min_periods=1).mean()

    # 基于价格atr，计算波动率因子wd_atr
    c1 = df['high'] - df['low']
    c2 = abs(df['high'] - df['close'].shift(1))
    c3 = abs(df['low'] - df['close'].shift(1))
    tr = np.max(np.array([c1, c2, c3]), axis=0)  # 三个数列取其大值
    atr = pd.Series(tr).rolling(window=n1, min_periods=1).mean()
    avg_price = df['close'].rolling(window=n1, min_periods=1).mean()
    wd_atr = atr / avg_price  # === 波动率因子

    # 参考ATR，对MTM指标，计算波动率因子
    mtm_l = df['low'] / df['low'].shift(n1) - 1
    mtm_h = df['high'] / df['high'].shift(n1) - 1
    mtm_c = df['close'] / df['close'].shift(n1) - 1
    mtm_c1 = mtm_h - mtm_l
    mtm_c2 = abs(mtm_h - mtm_c.shift(1))
    mtm_c3 = abs(mtm_l - mtm_c.shift(1))
    mtm_tr = np.max(np.array([mtm_c1, mtm_c2, mtm_c3]), axis=0)  # 三个数列取其大值
    mtm_atr = pd.Series(mtm_tr).rolling(window=n1, min_periods=1).mean()  # === mtm 波动率因子

    # 参考ATR，对MTM mean指标，计算波动率因子
    mtm_l_mean = mtm_l.rolling(window=n1, min_periods=1).mean()
    mtm_h_mean = mtm_h.rolling(window=n1, min_periods=1).mean()
    mtm_c_mean = mtm_c.rolling(window=n1, min_periods=1).mean()
    mtm_c1 = mtm_h_mean - mtm_l_mean
    mtm_c2 = abs(mtm_h_mean - mtm_c_mean.shift(1))
    mtm_c3 = abs(mtm_l_mean - mtm_c_mean.shift(1))
    mtm_tr = np.max(np.array([mtm_c1, mtm_c2, mtm_c3]), axis=0)  # 三个数列取其大值
    mtm_atr_mean = pd.Series(mtm_tr).rolling(window=n1, min_periods=1).mean()  # === mtm_mean 波动率因子

    indicator = mtm_mean
    # mtm_mean指标分别乘以三个波动率因子
    indicator *= mtm_atr * mtm_atr_mean * wd_atr
    indicator = pd.Series(indicator)

    # 对新策略因子计算自适应布林
    median = indicator.rolling(window=n1).mean()
    std = indicator.rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    z_score = abs(indicator - median) / std
    m = pd.Series(z_score).rolling(window=n1).max().shift(1)
    up = median + std * m
    dn = median - std * m

    df['AdaBollingV1Upper'] = 1e8 * (indicator - up)  # 上轨因子
    df['AdaBollingV1Lower'] = 1e8 * (indicator - dn)  # 下轨因子

    return df


def add_diff(_df, _d_num, _name):
    """ 为 数据列 添加 差分数据列
    :param _df: 原数据 DataFrame
    :param _d_num: 差分阶数 0.3, 0.5
    :param _name: 需要添加 差分值 的数据列 名称
    :return: """
    if len(_df) >= 12:  # 数据行数大于等于12才进行差分操作
        _diff_ar = fdiff(_df[_name], n=_d_num, window=10, mode="valid")  # 列差分，不使用未来数据
        _paddings = len(_df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
        _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
        _df[_name + f'_diff_{_d_num}'] = _diff  # 将差分数据记录到 DataFrame
    else:
        _df[_name + f'_diff_{_d_num}'] = np.nan  # 数据行数不足12的填充为空数据


def signal(df: pd.DataFrame, n: int):
    """ 3H_2
    (AdaBollingV1Upper_bh_9_diff_0.5 + PSY_bh_3_diff_0.3) * (
    WR_bh_6_diff_0.5 + POS_bh_12 + J_bh_60 + VR_bh_6) +

    (REG_bh_24_diff_0.5 + 涨跌幅std_bh_3_diff_0.5) * (
    HMA_bh_24 + ATRUpper_bh_3 + CR_bh_3 + 成交笔数_bh_12)

    :param df:
    :param n:
    :return:
    """
    _n = n
    _df = df.copy()

    # =====AdaBollingV1Upper
    _n = 9
    _df = adaptBollingV1(_df, _n)
    _df[f'AdaBollingV1Upper_bh_{_n}'] = _df['AdaBollingV1Upper']  # ===AdaBollingV1Upper_bh_9
    del _df['AdaBollingV1Upper'], _df['AdaBollingV1Lower']
    add_diff(_df=_df, _d_num=0.5, _name=f'AdaBollingV1Upper_bh_{_n}')  # ===AdaBollingV1Upper_bh_9_diff_0.5

    # =====PSY
    _n = 3
    psy = np.where(_df['close'] > _df['close'].shift(1), 1, 0)
    _df[f'PSY_bh_{_n}'] = pd.Series(100 * psy / _n)  # ===PSY_bh_3
    add_diff(_df=_df, _d_num=0.3, _name=f'PSY_bh_{_n}')  # ===PSY_bh_3_diff_0.3

    # =====WR
    _n = 6
    high = _df['high'].rolling(_n, min_periods=1).max()
    low = _df['low'].rolling(_n, min_periods=1).min()
    _df[f'WR_bh_{_n}'] = 100 * (high - _df['close']) / (1e-9 + high - low)  # WR_bh_6
    add_diff(_df=_df, _d_num=0.5, _name=f'WR_bh_{_n}')  # ===WR_bh_6_diff_0.5

    # =====POS
    _n = 12
    price = (_df['close'] - _df['close'].shift(_n)) / _df['close'].shift(_n)
    price_min = price.rolling(_n, min_periods=1).min()
    price_max = price.rolling(_n, min_periods=1).max()
    _df[f'POS_bh_{_n}'] = (price - price_min) / (1e-9 + price_max - price_min)  # ===POS_bh_12

    # =====J
    _n = 60
    low_list = _df['low'].rolling(_n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
    high_list = _df['high'].rolling(_n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
    rsv = (_df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
    _K = rsv.ewm(com=2).mean()  # K
    _D = _K.ewm(com=2).mean()  # D
    _df[f'J_bh_{_n}'] = 3 * _K - 2 * _D  # ===J_bh_60

    # =====VR
    _n = 6
    av = np.where(_df['close'] > _df['close'].shift(1), _df['volume'], 0)
    bv = np.where(_df['close'] < _df['close'].shift(1), _df['volume'], 0)
    cv = np.where(_df['close'] == _df['close'].shift(1), _df['volume'], 0)
    avs = pd.Series(av).rolling(_n, min_periods=1).sum()
    bvs = pd.Series(bv).rolling(_n, min_periods=1).sum()
    cvs = pd.Series(cv).rolling(_n, min_periods=1).sum()
    _df[f'VR_bh_{_n}'] = (avs + 0.5 * cvs) / (1e-9 + bvs + 0.5 * cvs)  # ===VR_bh_6

    # =====REG
    _n = 24

    def reg_ols(_y):
        _x = np.arange(_n) + 1
        lr = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 回归
        _reg_close = lr.coef_ * _x + lr.intercept_  # REG_CLOSE = aX+b
        return _reg_close[-1]

    reg_close = _df['close'].rolling(_n).apply(lambda y: reg_ols(y))
    _df[f'REG_bh_{_n}'] = (_df['close'] - reg_close) / reg_close  # ===REG_bh_24
    add_diff(_df=_df, _d_num=0.5, _name=f'REG_bh_{_n}')  # ===REG_bh_24_diff_0.5

    # =====涨跌幅std
    _n = 3
    _df[f'涨跌幅std_bh_{_n}'] = _df['close'].pct_change().rolling(_n).std()  # ===涨跌幅std_bh_3
    add_diff(_df=_df, _d_num=0.5, _name=f'涨跌幅std_bh_{_n}')  # ===涨跌幅std_bh_3_diff_0.5

    # =====HMA
    _n = 24
    _df[f'HMA_bh_{_n}'] = _df['high'].rolling(_n, min_periods=1).mean()  # ===HMA_bh_24

    # =====ATRUpper
    _n = 3
    tr = np.max(np.array([
        (_df['high'] - _df['low']).abs(),
        (_df['high'] - _df['close'].shift(1)).abs(),
        (_df['low'] - _df['close'].shift(1)).abs()
    ]), axis=0)  # 三个数列取其大值
    atr = pd.Series(tr).ewm(alpha=1 / _n, adjust=False).mean().shift(1)
    _df[f'ATRUpper_bh_{_n}'] = _df['low'].rolling(int(_n / 2), min_periods=1).min() + 3 * atr  # ===ATRUpper_bh_3

    # =====CR
    _n = 3
    _typ = (_df['high'] + _df['low'] + _df['close']) / 3
    _h = np.maximum(_df['high'] - pd.Series(_typ).shift(1), 0)  # 两个数列取大值
    _l = np.maximum(pd.Series(_typ).shift(1) - _df['low'], 0)
    _df[f'CR_bh_{_n}'] = 100 * pd.Series(_h).rolling(_n, min_periods=1).sum() / (
            1e-9 + pd.Series(_l).rolling(_n, min_periods=1).sum())  # ===CR_bh_3

    # =====成交笔数
    _n = 12
    _df[f'成交笔数_bh_{_n}'] = _df['trade_num'].rolling(_n, min_periods=1).sum()  # ===成交笔数_bh_12

    # =====signal
    df[f'BOUKENJYA_REG'] = \
        (_df['AdaBollingV1Upper_bh_9_diff_0.5'] + _df['PSY_bh_3_diff_0.3']) * (
                _df['WR_bh_6_diff_0.5'] + _df['POS_bh_12'] + _df['J_bh_60'] + _df['VR_bh_6']) + \
        (_df['REG_bh_24_diff_0.5'] + _df['涨跌幅std_bh_3_diff_0.5']) * (
                _df['HMA_bh_24'] + _df['ATRUpper_bh_3'] + _df['CR_bh_3'] + _df['成交笔数_bh_12'])
    print('### --- ###')
    print('### --- ###')
    print(_df['AdaBollingV1Upper_bh_9_diff_0.5'].tail(1))
    print('### --- ###')
    print(_df['PSY_bh_3_diff_0.3'].tail(1))
    print('### --- ###')
    print(_df['WR_bh_6_diff_0.5'].tail(1))
    print('### --- ###')
    print(_df['POS_bh_12'].tail(1))
    print('### --- ###')
    print(_df['J_bh_60'].tail(1))
    print('### --- ###')
    print(_df['VR_bh_6'].tail(1))
    print('### --- ###')
    print(_df['REG_bh_24_diff_0.5'].tail(1))
    print('### --- ###')
    print(_df['涨跌幅std_bh_3_diff_0.5'].tail(1))
    print('### --- ###')
    print(_df['HMA_bh_24'].tail(1))
    print('### --- ###')
    print(_df['ATRUpper_bh_3'].tail(1))
    print('### --- ###')
    print(_df['CR_bh_3'].tail(1))
    print('### --- ###')
    print(_df['成交笔数_bh_12'].tail(1))
    print('### --- ###')
    print('### --- ###')

    # ===替换异常值
    # df.replace(to_replace=[np.inf, -np.inf], value=np.nan, inplace=True)  # 替换异常值

    print('### --- ###')
    print(df[f'BOUKENJYA_REG'].tail(5))
    print('### --- ###')

    # ===删除过程数据
    # ---AdaBollingV1Upper
    del _df['AdaBollingV1Upper_bh_9'], _df['AdaBollingV1Upper_bh_9_diff_0.5']

    # ---PSY
    del _df['PSY_bh_3'], _df['PSY_bh_3_diff_0.3']

    # ---WR
    del _df['WR_bh_6'], _df['WR_bh_6_diff_0.5']

    # ---POS
    del _df['POS_bh_12']

    # ---J
    del _df['J_bh_60']

    # ---VR
    del _df['VR_bh_6']

    # ---REG
    del _df['REG_bh_24'], _df['REG_bh_24_diff_0.5']

    # ---涨跌幅std
    del _df['涨跌幅std_bh_3'], _df['涨跌幅std_bh_3_diff_0.5']

    # ---HMA
    del _df['HMA_bh_24']

    # ---ATR
    del _df['ATRUpper_bh_3']

    # ---CR
    del _df['CR_bh_3']

    # ---成交笔数
    del _df['成交笔数_bh_12']

    return df
