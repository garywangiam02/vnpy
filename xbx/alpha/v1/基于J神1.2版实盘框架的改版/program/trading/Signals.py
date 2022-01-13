"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import numpy as np
from singals.BOUKENJYA_REG import signal as _signal_BOUKENJYA_REG
import talib


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


def signal_cmo(df, n=1):
    df['momentum'] = df['close'] - df['close'].shift(1)
    df['up'] = np.where(df['momentum'] > 0, df['momentum'], 0)
    df['dn'] = np.where(df['momentum'] < 0, abs(df['momentum']), 0)
    df['up_sum'] = df['up'].rolling(window=n, min_periods=1).sum()
    df['dn_sum'] = df['dn'].rolling(window=n, min_periods=1).sum()
    df['cmo'] = (df['up_sum'] - df['dn_sum']) / (df['up_sum'] + df['dn_sum'])
    return df


def signal_ADX(df, n=1):
    df['max_high'] = np.where(df['high'] > df['high'].shift(1), df['high'] - df['high'].shift(1), 0)
    df['max_low'] = np.where(df['low'].shift(1) > df['low'], df['low'].shift(1) - df['low'], 0)
    df['XPDM'] = np.where(df['max_high'] > df['max_low'], df['high'] - df['high'].shift(1), 0)
    df['PDM'] = df['XPDM'].rolling(n).sum()
    df['XNDM'] = np.where(df['max_low'] > df['max_high'], df['low'].shift(1) - df['low'], 0)
    df['NDM'] = df['XNDM'].rolling(n).sum()
    df['c1'] = abs(df['high'] - df['low'])
    df['c2'] = abs(df['high'] - df['close'])
    df['c3'] = abs(df['low'] - df['close'])
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['TR_sum'] = df['TR'].rolling(n).sum()

    df['ADX'] = (df['PDM'] + df['NDM']) / df['TR']

    del df['max_high']
    del df['max_low']
    del df['XPDM']
    del df['PDM']
    del df['XNDM']
    del df['NDM']
    del df['c1']
    del df['c2']
    del df['c3']
    del df['TR']
    del df['TR_sum']

    return df


# ------------------ #

def signal_bias(df, n=1):
    ma = df['close'].rolling(n, min_periods=1).mean()
    df['bias'] = (df['close'] / ma - 1)
    return df


def signal_RSI(df, n=1):
    close_dif = df['close'].diff()
    df['up'] = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)

    a = df['up'].rolling(n).sum()
    b = df['down'].rolling(n).sum()
    df[f'RSI'] = (a / (a + b))  # RSI

    del df['up']
    del df['down']

    return df


def signal_cci(df, n=1):
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
    df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
    df['cci'] = (df['tp'] - df['ma']) / df['md'] / 0.015
    return df


def signal_VIDYA(df, n=1):
    df['abs_diff_close'] = abs(df['close'] - df['close'].shift(n))
    df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n).sum()
    VI = df['abs_diff_close'] / df['abs_diff_close_sum']
    VIDYA = VI * df['close'] + (1 - VI) * df['close'].shift(1)
    # 进行无量纲处理
    df[f'VIDYA'] = VIDYA / df['close']

    del df['abs_diff_close']
    del df['abs_diff_close_sum']

    return df


# REG 因子
def signal_REG(df, n=1):
    df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n)  # 该部分为talib内置求线性回归
    df['REG'] = df['close'] / df['reg_close'] - 1
    return df


def signal_force(df, n=1):
    df['force'] = df['quote_volume'] * (df['close'] - df['close'].shift(1))
    df['force'] = df['force'].rolling(n, min_periods=1).mean()

    return df


def signal_CLV(df, n=1):
    df['CLV'] = (2 * df['close'] - df['low'] - df['high']) / (df['high'] - df['low'])
    df['CLV'] = df['CLV'].rolling(n, min_periods=1).mean()
    return df


def signal_收高差值(df, n=1):
    high_mean = df['high'].rolling(n, min_periods=1).mean()
    # low = df['low'].rolling(n, min_periods=1).mean()
    close_mean = df['close']
    df['收高差值'] = (close_mean - high_mean) / high_mean
    return df


def signal_量比(df, n=1):
    df['量比'] = (df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean())
    return df


def signal_lcsd(df, n=1):
    df['_lcsd_maim'] = df['close'].rolling(n).mean()
    df['lcsd'] = (df['low'] - df['_lcsd_maim']) / df['low']
    # 删除中间过程数据
    del df['_lcsd_maim']

    return df


def signal_K(df, n=1):
    low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
    high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
    df[f'K'] = rsv.ewm(com=2).mean()  # K
    return df


def signal_D(df, n=1):
    low_list = df['low'].rolling(n, min_periods=1).min()  # 过去n(含当前行)行数据 最低价的最小值
    high_list = df['high'].rolling(n, min_periods=1).max()  # 过去n(含当前行)行数据 最高价的最大值
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100  # 未成熟随机指标值
    df[f'_K'] = rsv.ewm(com=2).mean()  # K
    df[f'D'] = df[f'_K'].ewm(com=2).mean()  # D
    del df['_K']

    return df


def signal_GAP(df, n=1):
    ma = df['close'].rolling(n, min_periods=1).mean()
    ema = df['close'].ewm(span=n, adjust=False).mean()
    gap = ema - ma
    df[f'GAP'] = gap / abs(gap).rolling(n).sum()

    return df


def signal_vwap_bias(df, n=1):
    df['vwap'] = df['volume'] / df['quote_volume']
    ma = df['vwap'].rolling(n, min_periods=1).mean()
    df[f'vwap_bias'] = df['vwap'] / ma - 1

    return df


def signal_BOUKENJYA_REG(df, n=1):
    df = _signal_BOUKENJYA_REG(df, n)

    return df


def signal_DPO(df, n=1):
    # 计算 DPO
    """
    N=20
    DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
    DPO 是当前价格与延迟的移动平均线的差值，通过去除前一段时间
    的移动平均价格来减少长期的趋势对短期价格波动的影响。DPO>0
    表示目前处于多头市场；DPO<0 表示当前处于空头市场。我们通过
    DPO 上穿/下穿 0 线来产生买入/卖出信号。

    """
    ma = df['close'].rolling(n, min_periods=1).mean()
    ref = ma.shift(int(n / 2 + 1))
    df[f'DPO'] = df['close'] / ref - 1

    return df
