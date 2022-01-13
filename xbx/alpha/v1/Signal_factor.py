#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/3 8:31
# @Author  : weixx
# @File    : Signal_factor.py
# 中性策略框架
#  指标基础因子库
import pandas as pd
import numpy as np
import glob
import os
import sys

sys.path.append(os.getcwd())
from program.backtest.Function import *
from multiprocessing import Pool, freeze_support, cpu_count
import platform
import talib # talib版本 0.4.18
from sklearn.linear_model import LinearRegression # 版本0.0
from functools import partial
from fracdiff import fdiff  # https://github.com/simaki/fracdiff  pip install fracdiff
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数


diff_d = [0.3, 0.5, 0.7]  # 差分阶数
eps = 1e-8

def factor_calculation(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # ========以下是需要修改的代码
    # ===计算各项选币指标
    for n in back_hour_list:
        df['price'] = df['quote_volume'].rolling(n).sum() / df['volume'].rolling(n).sum()
        
        df['前%dh均价' % n] = (df['price'] - df['price'].rolling(n, min_periods=1).min()) / (df['price'].rolling(n, min_periods=1).max() - df['price'].rolling(n, min_periods=1).min())

        df['前%dh均价' % n] = df['前%dh均价' % n].shift(1)
        extra_agg_dict['前%dh均价' % n] = 'first'
        del df['price']
    # 涨跌幅
    for n in back_hour_list:
        df['前%dh涨跌幅' % n] = df['close'].pct_change(n)
        df['前%dh涨跌幅' % n] = df['前%dh涨跌幅' % n].shift(1)
        extra_agg_dict['前%dh涨跌幅' % n] = 'first'
    # 涨跌幅更好的表达方式：bias，币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df['前%dhbias' % n] = df['close'] / ma - 1
        df['前%dhbias' % n] = df['前%dhbias' % n].shift(1)
        extra_agg_dict['前%dhbias' % n] = 'first'
    # 振幅：最高价最低价
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df['前%dh振幅' % n] = high / low - 1
        df['前%dh振幅' % n] = df['前%dh振幅' % n].shift(1)
        extra_agg_dict['前%dh振幅' % n] = 'first'
    # 振幅：收盘价、开盘价
    high = df[['close', 'open']].max(axis=1)
    low = df[['close', 'open']].min(axis=1)
    for n in back_hour_list:
        high = high.rolling(n, min_periods=1).max()
        low = low.rolling(n, min_periods=1).min()
        df['前%dh振幅2' % n] = high / low - 1
        df['前%dh振幅2' % n] = df['前%dh振幅2' % n].shift(1)
        extra_agg_dict['前%dh振幅2' % n] = 'first'
    # 涨跌幅std，振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df['前%dh涨跌幅std' % n] = change.rolling(n).std()
        df['前%dh涨跌幅std' % n] = df['前%dh涨跌幅std' % n].shift(1)
        extra_agg_dict['前%dh涨跌幅std' % n] = 'first'
    # 涨跌幅偏度：在商品期货市场有效
    for n in back_hour_list:
        df['前%dh涨跌幅skew' % n] = change.rolling(n).skew()
        df['前%dh涨跌幅skew' % n] = df['前%dh涨跌幅skew' % n].shift(1)
        extra_agg_dict['前%dh涨跌幅skew' % n] = 'first'
    # 成交额：对应小市值概念
    for n in back_hour_list:
        df['前%dh成交额' % n] = df['quote_volume'].rolling(n, min_periods=1).sum()
        df['前%dh成交额' % n] = df['前%dh成交额' % n].shift(1)
        extra_agg_dict['前%dh成交额' % n] = 'first'
    # 成交额std，191选股因子中最有效的因子
    for n in back_hour_list:
        df['前%dh成交额std' % n] = df['quote_volume'].rolling(n, min_periods=2).std()
        df['前%dh成交额std' % n] = df['前%dh成交额std' % n].shift(1)
        extra_agg_dict['前%dh成交额std' % n] = 'first'
    # 资金流入，币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df['前%dh资金流入比例' % n] = buy_volume / volume
        df['前%dh资金流入比例' % n] = df['前%dh资金流入比例' % n].shift(1)
        extra_agg_dict['前%dh资金流入比例' % n] = 'first'
    # 量比
    for n in back_hour_list:
        df['前%dh量比' % n] = df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()
        df['前%dh量比' % n] = df['前%dh量比' % n].shift(1)
        extra_agg_dict['前%dh量比' % n] = 'first'
    # 成交笔数
    for n in back_hour_list:
        df['前%dh成交笔数' % n] = df['trade_num'].rolling(n, min_periods=1).sum()
        df['前%dh成交笔数' % n] = df['前%dh成交笔数' % n].shift(1)
        extra_agg_dict['前%dh成交笔数' % n] = 'first'
    # 量价相关系数：量价相关选股策略
    for n in back_hour_list:
        df['前%dh量价相关系数' % n] = df['close'].rolling(n).corr(df['quote_volume'].rolling(n))
        df['前%dh量价相关系数' % n] = df['前%dh量价相关系数' % n].shift(1)
        extra_agg_dict['前%dh量价相关系数' % n] = 'first'
    
    # RSI 指标
    for n in back_hour_list:
        """
        CLOSEUP=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        CLOSEDOWN=IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0)
        CLOSEUP_MA=SMA(CLOSEUP,N,1)
        CLOSEDOWN_MA=SMA(CLOSEDOWN,N,1)
        RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)
        RSI 反映一段时间内平均收益与平均亏损的对比。通常认为当 RSI 大 于 70，市场处于强势上涨甚至达到超买的状态；当 RSI 小于 30，市
        场处于强势下跌甚至达到超卖的状态。当 RSI 跌到 30 以下又上穿 30
        时，通常认为股价要从超卖的状态反弹；当 RSI 超过 70 又下穿 70
        时，通常认为市场要从超买的状态回落了。实际应用中，不一定要使
        用 70/30 的阈值选取。这里我们用 60/40 作为信号产生的阈值。
        RSI 上穿 40 则产生买入信号；
        RSI 下穿 60 则产生卖出信号。
        """
        diff = df['close'].diff() # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0) # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff), 0) # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()# SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean() # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df['前%dhRSI' % n] = A / (A + B)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df['前%dhRSI' % n] = df['前%dhRSI' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhRSI' % n] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']

    # KDJ 指标
    for n in back_hour_list:
        """
        N=40
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        K=SMA(Stochastics,3,1)
        D=SMA(K,3,1) 
        先计算 N 日内的未成熟随机值 RSV，然后计算 K 值=（2*前日 K 值+
        当日 RSV）/3，D 值=（2*前日 D 值+当日 K 值）/3
        KDJ 指标用来衡量当前收盘价在过去 N 天的最低价与最高价之间的
        位置。值越高（低），则说明其越靠近过去 N 天的最高（低）价。当
        值过高或过低时，价格可能发生反转。通常认为 D 值小于 20 处于超
        卖状态，D 值大于 80 属于超买状态。
        如果 D 小于 20 且 K 上穿 D，则产生买入信号；
        如果 D 大于 80 且 K 下穿 D，则产生卖出信号。
        """
        low_list = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N) 求周期内low的最小值
        high_list = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N) 求周期内high 的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100 # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100 计算一个随机值
        # K D J的值在固定的范围内
        df['前%dhK' % n] = rsv.ewm(com=2).mean() # K=SMA(Stochastics,3,1) 计算k
        df['前%dhD' % n] = df['前%dhK' % n].ewm(com=2).mean()  # D=SMA(K,3,1)  计算D
        df['前%dhJ' % n] = 3 * df['前%dhK' % n] - 2 * df['前%dhD' % n] # 计算J
        df['前%dhK' % n] = df['前%dhK' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        df['前%dhD' % n] = df['前%dhD' % n].shift(1)
        df['前%dhJ' % n] = df['前%dhJ' % n].shift(1)
        extra_agg_dict['前%dhK' % n] = 'first'
        extra_agg_dict['前%dhD' % n] = 'first'
        extra_agg_dict['前%dhJ' % n] = 'first'

    # 计算魔改CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        df['oma'] = df['open'].ewm(span=n, adjust=False).mean() # 取 open 的ema
        df['hma'] = df['high'].ewm(span=n, adjust=False).mean() # 取 high 的ema
        df['lma'] = df['low'].ewm(span=n, adjust=False).mean() # 取 low的ema
        df['cma'] = df['close'].ewm(span=n, adjust=False).mean() # 取 close的ema
        df['tp'] = (df['oma'] + df['hma'] + df['lma'] + df['cma']) / 4 # 魔改CCI基础指标 将TP=(HIGH+LOW+CLOSE)/3  替换成以open/high/low/close的ema 的均值
        df['ma'] = df['tp'].ewm(span=n, adjust=False).mean() # MA(TP,N)  将移动平均改成 ema
        df['abs_diff_close'] = abs(df['tp'] - df['ma']) # ABS(TP-MA)
        df['md'] = df['abs_diff_close'].ewm(span=n, adjust=False).mean() # MD=MA(ABS(TP-MA),N)  将移动平均替换成ema

        df['前%dhCCI' % n] = (df['tp'] - df['ma']) / df['md'] # CCI=(TP-MA)/(0.015MD)  CCI在一定范围内
        df['前%dhCCI' % n] = df['前%dhCCI' % n].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhCCI' % n] = 'first'
        # # 删除中间数据
        del df['oma']
        del df['hma']
        del df['lma']
        del df['cma']
        del df['tp']
        del df['ma']
        del df['abs_diff_close']
        del df['md']

    # 计算CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        open_ma = df['open'].rolling(n, min_periods=1).mean()
        high_ma = df['high'].rolling(n, min_periods=1).mean()
        low_ma = df['low'].rolling(n, min_periods=1).mean()
        close_ma = df['close'].rolling(n, min_periods=1).mean()
        tp = (high_ma + low_ma + close_ma) / 3 # TP=(HIGH+LOW+CLOSE)/3
        ma = tp.rolling(n, min_periods=1).mean() # MA=MA(TP,N)
        md = abs(ma - close_ma).rolling(n, min_periods=1).mean() # MD=MA(ABS(TP-MA),N)
        df['前%dhmagic_CCI' % n] = (tp - ma) / md / 0.015 # CCI=(TP-MA)/(0.015MD)
        df['前%dhmagic_CCI' % n] = df['前%dhmagic_CCI' % n].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhmagic_CCI' % n] = 'first'

    # 计算macd指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=5
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        MACD_SIGNAL=EMA(MACD,N3)
        MACD_HISTOGRAM=MACD-MACD_SIGNAL
        
        MACD 指标衡量快速均线与慢速均线的差值。由于慢速均线反映的是
        之前较长时间的价格的走向，而快速均线反映的是较短时间的价格的
        走向，所以在上涨趋势中快速均线会比慢速均线涨的快，而在下跌趋
        势中快速均线会比慢速均线跌得快。所以 MACD 上穿/下穿 0 可以作
        为一种构造交易信号的方式。另外一种构造交易信号的方式是求
        MACD 与其移动平均（信号线）的差值得到 MACD 柱，利用 MACD
        柱上穿/下穿 0（即 MACD 上穿/下穿其信号线）来构造交易信号。这
        种方式在其他指标的使用中也可以借鉴。
        """
        short_windows = n
        long_windows = 3 * n
        macd_windows = int(1.618 * n)

        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean() # EMA(CLOSE,N1)
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean() # EMA(CLOSE,N2)
        df['dif'] = df['ema_short'] - df['ema_long'] #  MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['dea'] = df['dif'].ewm(span=macd_windows, adjust=False).mean() # MACD_SIGNAL=EMA(MACD,N3)
        df['macd'] = 2 * (df['dif'] - df['dea']) # MACD_HISTOGRAM=MACD-MACD_SIGNAL  一般看图指标计算对应实际乘以了2倍
        # 进行去量纲
        df['前%dhmacd' % n] = df['macd'] / df['macd'].rolling(macd_windows, min_periods=1).mean() - 1

        # df['前%dhdif' % n] = df['前%dhdif' % n].shift(1)
        # extra_agg_dict['前%dhdif' % n] = 'first'
        #
        # df['前%dhdea' % n] = df['前%dhdea' % n].shift(1)
        # extra_agg_dict['前%dhdea' % n] = 'first'

        df['前%dhmacd' % n] = df['前%dhmacd' % n].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhmacd' % n] = 'first'

        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['dif']
        del df['dea']

    # 计算ema的差值
    for n in back_hour_list:
        """
        与求MACD的dif线一样
        """
        short_windows = n
        long_windows = 3 * n
        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean() # 计算短周期ema
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean() # 计算长周期的ema
        df['diff_ema'] = df['ema_short'] - df['ema_long'] # 计算俩条线之间的差值

        df['diff_ema_mean'] = df['diff_ema'].ewm(span=n, adjust=False).mean()

        df['前%dhdiff_ema' % n] = df['diff_ema'] / df['diff_ema_mean'] - 1  # 去量纲
        df['前%dhdiff_ema' % n] = df['前%dhdiff_ema' % n].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhdiff_ema' % n] = 'first'
        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['diff_ema']
        del df['diff_ema_mean']

    # bias因子以均价表示
    for n in back_hour_list:
        """
        将bias 的close替换成vwap
        """
        df['vwap'] = df['volume'] / df['quote_volume']  # 在周期内成交额除以成交量等于成交均价
        ma = df['vwap'].rolling(n, min_periods=1).mean() # 求移动平均线
        df['前%dhvwap_bias' % n] = df['vwap'] / ma - 1  # 去量纲
        df['前%dhvwap_bias' % n] = df['前%dhvwap_bias' % n].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhvwap_bias' % n] = 'first'

    # 计算BBI 的bias
    for n in back_hour_list:
        """
        BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        BBI 是对不同时间长度的移动平均线取平均，能够综合不同移动平均
        线的平滑性和滞后性。如果收盘价上穿/下穿 BBI 则产生买入/卖出信
        号。
        """
        # 将BBI指标计算出来求bias
        ma1 = df['close'].rolling(n, min_periods=1).mean()
        ma2 = df['close'].rolling(2 * n, min_periods=1).mean()
        ma3 = df['close'].rolling(4 * n, min_periods=1).mean()
        ma4 = df['close'].rolling(8 * n, min_periods=1).mean()
        bbi = (ma1 + ma2 + ma3 + ma4) / 4 # BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        df['前%dhbbi_bias' % n] = df['close'] / bbi - 1
        df['前%dhbbi_bias' % n] = df['前%dhbbi_bias' % n].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhbbi_bias' % n] = 'first'

    # 计算 DPO
    for n in back_hour_list:
        """
        N=20
        DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        DPO 是当前价格与延迟的移动平均线的差值，通过去除前一段时间
        的移动平均价格来减少长期的趋势对短期价格波动的影响。DPO>0
        表示目前处于多头市场；DPO<0 表示当前处于空头市场。我们通过
        DPO 上穿/下穿 0 线来产生买入/卖出信号。

        """
        ma = df['close'].rolling(n, min_periods=1).mean()# 求close移动平均线
        ref = ma.shift(int(n / 2 + 1)) # REF(MA(CLOSE,N),N/2+1)
        df['DPO'] = df['close'] - ref # DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        df['DPO_ma'] = df['DPO'].rolling(n, min_periods=1).mean()  # 求均值
        df['前%dhDPO' % n] = df['DPO'] / df['DPO_ma'] - 1  # 去量纲
        df['前%dhDPO' % n] = df['前%dhDPO' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhDPO' % n] = 'first'
        # 删除中间数据
        del df['DPO']
        del df['DPO_ma']


    # 计算 ER
    for n in back_hour_list:
        """
        N=20
        BullPower=HIGH-EMA(CLOSE,N)
        BearPower=LOW-EMA(CLOSE,N)
        ER 为动量指标。用来衡量市场的多空力量对比。在多头市场，人们
        会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量
        越强；而在空头市场，人们可能因为恐惧而在接近低价的地方卖出。
        BearPower 越低则当前空头力量越强。当两者都大于 0 时，反映当前
        多头力量占据主导地位；两者都小于0则反映空头力量占据主导地位。
        如果 BearPower 上穿 0，则产生买入信号；
        如果 BullPower 下穿 0，则产生卖出信号。
        """
        ema = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        bull_power = df['high'] - ema  # 越高表示上涨 牛市 BullPower=HIGH-EMA(CLOSE,N)
        bear_power = df['low'] - ema  # 越低表示下降越厉害  熊市 BearPower=LOW-EMA(CLOSE,N)
        df['前%dhER_bull' % n] = bull_power / ema  # 去量纲
        df['前%dhER_bear' % n] = bear_power / ema  # 去量纲
        df['前%dhER_bull' % n] = df['前%dhER_bull' % n].shift(1)
        df['前%dhER_bear' % n] = df['前%dhER_bear' % n].shift(1)
        extra_agg_dict['前%dhER_bull' % n] = 'first'
        extra_agg_dict['前%dhER_bear' % n] = 'first'

    # PO指标
    for n in back_hour_list:
        """
        EMA_SHORT=EMA(CLOSE,9)
        EMA_LONG=EMA(CLOSE,26)
        PO=(EMA_SHORT-EMA_LONG)/EMA_LONG*100
        PO 指标求的是短期均线与长期均线之间的变化率。
        如果 PO 上穿 0，则产生买入信号；
        如果 PO 下穿 0，则产生卖出信号。
        """
        ema_short = df['close'].ewm(n, adjust=False).mean() # 短周期的ema
        ema_long = df['close'].ewm(n * 3, adjust=False).mean() # 长周期的ema   固定倍数关系 减少参数
        df['前%dhPO' % n] = (ema_short - ema_long) / ema_long * 100 # 去量纲
        df['前%dhPO' % n] = df['前%dhPO' % n].shift(1)
        extra_agg_dict['前%dhPO' % n] = 'first'

    # MADisplaced 指标
    for n in back_hour_list:
        """
        N=20
        M=10
        MA_CLOSE=MA(CLOSE,N)
        MADisplaced=REF(MA_CLOSE,M)
        MADisplaced 指标把简单移动平均线向前移动了 M 个交易日，用法
        与一般的移动平均线一样。如果收盘价上穿/下穿 MADisplaced 则产
        生买入/卖出信号。
        有点变种bias
        """
        ma = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,N) 固定俩个参数之间的关系  减少参数
        ref = ma.shift(n)  # MADisplaced=REF(MA_CLOSE,M)

        df['前%dhMADisplaced' % n] = df['close'] / ref - 1 # 去量纲
        df['前%dhMADisplaced' % n] = df['前%dhMADisplaced' % n].shift(1)
        extra_agg_dict['前%dhMADisplaced' % n] = 'first'

    # T3 指标
    for n in back_hour_list:
        """
        N=20
        VA=0.5
        T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        当 VA 是 0 时，T3 就是三重指数平均线，此时具有严重的滞后性；当
        VA 是 0 时，T3 就是三重双重指数平均线（DEMA），此时可以快速
        反应价格的变化。VA 值是 T3 指标的一个关键参数，可以用来调节
        T3 指标的滞后性。如果收盘价上穿/下穿 T3，则产生买入/卖出信号。
        """
        va = 0.5
        ema = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N),N)
        T1 = ema * (1 + va) - ema_ema * va # T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T1_ema = T1.ewm(n, adjust=False).mean() # EMA(T1,N)
        T1_ema_ema = T1_ema.ewm(n, adjust=False).mean()  # EMA(EMA(T1,N),N)
        T2 = T1_ema * (1 + va) - T1_ema_ema * va # T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T2_ema = T2.ewm(n, adjust=False).mean() # EMA(T2,N)
        T2_ema_ema = T2_ema.ewm(n, adjust=False).mean() # EMA(EMA(T2,N),N)
        T3 = T2_ema * (1 + va) - T2_ema_ema * va # T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        df['前%dhT3' % n] = df['close'] / T3 - 1  # 去量纲
        df['前%dhT3' % n] = df['前%dhT3' % n].shift(1)
        extra_agg_dict['前%dhT3' % n] = 'first'

    # POS指标
    for n in back_hour_list:
        """
        N=100
        PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        POS 指标衡量当前的 N 天收益率在过去 N 天的 N 天收益率最大值和
        最小值之间的位置。当 POS 上穿 80 时产生买入信号；当 POS 下穿
        20 时产生卖出信号。
        """
        ref = df['close'].shift(n) # REF(CLOSE,N)
        price = (df['close'] - ref) / ref # PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        min_price = price.rolling(n).min() # MIN(PRICE,N)
        max_price = price.rolling(n).max() # MAX(PRICE,N)
        pos = (price - min_price) / (max_price - min_price) # POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        df['前%dhPOS' % n] = pos.shift(1)
        extra_agg_dict['前%dhPOS' % n] = 'first'

    # PAC 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        UPPER=SMA(HIGH,N1,1)
        LOWER=SMA(LOW,N2,1)
        用最高价和最低价的移动平均来构造价格变化的通道，如果价格突破
        上轨则做多，突破下轨则做空。
        """
        # upper = df['high'].rolling(n, min_periods=1).mean()
        df['upper'] = df['high'].ewm(span=n).mean() # UPPER=SMA(HIGH,N1,1)
        # lower = df['low'].rolling(n, min_periods=1).mean()
        df['lower'] = df['low'].ewm(span=n).mean() # LOWER=SMA(LOW,N2,1)
        df['width'] = df['upper'] - df['lower'] # 添加指标求宽度进行去量纲
        df['width_ma'] = df['width'].rolling(n, min_periods=1).mean()

        df['前%dhPAC' % n] = df['width'] / df['width_ma'] - 1
        df['前%dhPAC' % n] = df['前%dhPAC' % n].shift(1)
        extra_agg_dict['前%dhPAC' % n] = 'first'

        # 删除中间数据
        del df['upper']
        del df['lower']
        del df['width']
        del df['width_ma']



    # ADM 指标
    for n in back_hour_list:
        """
        N=20
        DTM=IF(OPEN>REF(OPEN,1),MAX(HIGH-OPEN,OPEN-REF(OP
        EN,1)),0)
        DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-O
        PEN),0)
        STM=SUM(DTM,N)
        SBM=SUM(DBM,N)
        ADTM=(STM-SBM)/MAX(STM,SBM)
        ADTM 通过比较开盘价往上涨的幅度和往下跌的幅度来衡量市场的
        人气。ADTM 的值在-1 到 1 之间。当 ADTM 上穿 0.5 时，说明市场
        人气较旺；当 ADTM 下穿-0.5 时，说明市场人气较低迷。我们据此构
        造交易信号。
        当 ADTM 上穿 0.5 时产生买入信号；
        当 ADTM 下穿-0.5 时产生卖出信号。

        """
        df['h_o'] = df['high'] - df['open'] # HIGH-OPEN
        df['diff_open'] = df['open'] - df['open'].shift(1) # OPEN-REF(OPEN,1)
        max_value1 = df[['h_o', 'diff_open']].max(axis=1) # MAX(HIGH-OPEN,OPEN-REF(OPEN,1))
        # df.loc[df['open'] > df['open'].shift(1), 'DTM'] = max_value1
        # df['DTM'].fillna(value=0, inplace=True)
        df['DTM'] = np.where(df['open'] > df['open'].shift(1), max_value1, 0) #DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['o_l'] = df['open'] - df['low'] # OPEN-LOW
        max_value2 = df[['o_l', 'diff_open']].max(axis=1) # MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['DBM'] = np.where(df['open'] < df['open'].shift(1), max_value2, 0) #DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        # df.loc[df['open'] < df['open'].shift(1), 'DBM'] = max_value2
        # df['DBM'].fillna(value=0, inplace=True)

        df['STM'] = df['DTM'].rolling(n).sum() # STM=SUM(DTM,N)
        df['SBM'] = df['DBM'].rolling(n).sum() # SBM=SUM(DBM,N)
        max_value3 = df[['STM', 'SBM']].max(axis=1) # MAX(STM,SBM)
        ADTM = (df['STM'] - df['SBM']) / max_value3 # ADTM=(STM-SBM)/MAX(STM,SBM)
        df['前%dhADTM' % n] = ADTM.shift(1)
        extra_agg_dict['前%dhADTM' % n] = 'first'

        # 删除中间数据
        del df['h_o']
        del df['diff_open']
        del df['o_l']
        del df['STM']
        del df['SBM']
        del df['DBM']
        del df['DTM']

    # ZLMACD 指标
    for n in back_hour_list:
        """
        N1=20
        N2=100
        ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EM
        A(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        ZLMACD 指标是对 MACD 指标的改进，它在计算中使用 DEMA 而不
        是 EMA，可以克服 MACD 指标的滞后性问题。如果 ZLMACD 上穿/
        下穿 0，则产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N1)
        ema_ema_1 = ema1.ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N1),N1)
        n2 = 5 * n # 固定俩参数的倍数关系减少参数
        ema2 = df['close'].ewm(n2, adjust=False).mean() # EMA(CLOSE,N2)
        ema_ema_2 = ema2.ewm(n2, adjust=False).mean() # EMA(EMA(CLOSE,N2),N2)
        ZLMACD = (2 * ema1 - ema_ema_1) - (2 * ema2 - ema_ema_2) # ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EMA(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        df['前%dhZLMACD' % n] = df['close'] / ZLMACD - 1
        df['前%dhZLMACD' % n] = df['前%dhZLMACD' % n].shift(1)
        extra_agg_dict['前%dhZLMACD' % n] = 'first'

    # TMA 指标
    for n in back_hour_list:
        """
        N=20
        CLOSE_MA=MA(CLOSE,N)
        TMA=MA(CLOSE_MA,N)
        TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
        越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
        靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
        卖出信号。
        """
        ma = df['close'].rolling(n, min_periods=1).mean() # CLOSE_MA=MA(CLOSE,N)
        tma = ma.rolling(n, min_periods=1).mean() # TMA=MA(CLOSE_MA,N)
        df['前%dhtma_bias' % n] = df['close'] / tma - 1
        df['前%dhtma_bias' % n] = df['前%dhtma_bias' % n].shift(1)
        extra_agg_dict['前%dhtma_bias' % n] = 'first'

    # TYP 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1=EMA(TYP,N1)
        TYPMA2=EMA(TYP,N2)
        在技术分析中，典型价格（最高价+最低价+收盘价）/3 经常被用来代
        替收盘价。比如我们在利用均线交叉产生交易信号时，就可以用典型
        价格的均线。
        TYPMA1 上穿/下穿 TYPMA2 时产生买入/卖出信号。
        """
        TYP = (df['close'] + df['high'] + df['low']) / 3 # TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1 = TYP.ewm(n, adjust=False).mean() # TYPMA1=EMA(TYP,N1)
        TYPMA2 = TYP.ewm(n * 3, adjust=False).mean() # TYPMA2=EMA(TYP,N2) 并且固定俩参数倍数关系
        diff_TYP = TYPMA1 - TYPMA2 # 俩ema相差
        diff_TYP_mean = diff_TYP.rolling(n, min_periods=1).mean()
        # diff_TYP_min = diff_TYP.rolling(n, min_periods=1).std()
        # 无量纲
        df['前%dhTYP' % n] = diff_TYP / diff_TYP_mean -1
        df['前%dhTYP' % n] = df['前%dhTYP' % n].shift(1)
        extra_agg_dict['前%dhTYP' % n] = 'first'

    # KDJD 指标
    for n in back_hour_list:
        """
        N=20
        M=60
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K=SMA(Stochastics_DOUBLE,3,1)
        D=SMA(K,3,1)
        KDJD 可以看作 KDJ 的变形。KDJ 计算过程中的变量 Stochastics 用
        来衡量收盘价位于最近 N 天最高价和最低价之间的位置。而 KDJD 计
        算过程中的 Stochastics_DOUBLE 可以用来衡量 Stochastics 在最近
        N 天的 Stochastics 最大值与最小值之间的位置。我们这里将其用作
        动量指标。当 D 上穿 70/下穿 30 时，产生买入/卖出信号。
        """
        min_low = df['low'].rolling(n).min()  # LOW_N=MIN(LOW,N)
        max_high = df['high'].rolling(n).max() # HIGH_N=MAX(HIGH,N)
        Stochastics = (df['close'] - min_low) / (max_high - min_low) * 100 # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        # 固定俩参数的倍数关系
        Stochastics_LOW = Stochastics.rolling(n * 3).min() # Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH = Stochastics.rolling(n * 3).max() # Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE = (Stochastics - Stochastics_LOW) / (Stochastics_HIGH - Stochastics_LOW) # Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K = Stochastics_DOUBLE.ewm(com=2).mean() #  K=SMA(Stochastics_DOUBLE,3,1)
        D = K.ewm(com=2).mean() # D=SMA(K,3,1)
        df['前%dhKDJD_K' % n] = K.shift(1)
        df['前%dhKDJD_D' % n] = D.shift(1)
        extra_agg_dict['前%dhKDJD_K' % n] = 'first'
        extra_agg_dict['前%dhKDJD_D' % n] = 'first'

    # VMA 指标
    for n in back_hour_list:
        """
        N=20
        PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        VMA=MA(PRICE,N)
        VMA 就是简单移动平均把收盘价替换为最高价、最低价、开盘价和
        收盘价的平均值。当 PRICE 上穿/下穿 VMA 时产生买入/卖出信号。
        """
        price = (df['high'] + df['low'] + df['open'] + df['close']) / 4 # PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        vma = price.rolling(n, min_periods=1).mean() # VMA=MA(PRICE,N)
        df['前%dhvma_bias' % n] = price / vma - 1 # 去量纲
        df['前%dhvma_bias' % n] = df['前%dhvma_bias' % n].shift(1)
        extra_agg_dict['前%dhvma_bias' % n] = 'first'

    # DDI 指标
    for n in back_hour_list:
        """
        n = 40
        HL=HIGH+LOW
        HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        LOW_ABS=ABS(LOW-REF(LOW,1))
        DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DDI=DIZ-DIF
        DDI 指标用来比较向上波动和向下波动的比例。如果 DDI 上穿/下穿 0
        则产生买入/卖出信号。
        """
        df['hl'] = df['high'] + df['low'] # HL=HIGH+LOW
        df['abs_high'] = abs(df['high'] - df['high'].shift(1)) # HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        df['abs_low'] = abs(df['low'] - df['low'].shift(1)) # LOW_ABS=ABS(LOW-REF(LOW,1))
        max_value1 = df[['abs_high', 'abs_low']].max(axis=1)  # MAX(HIGH_ABS,LOW_ABS)
        # df.loc[df['hl'] > df['hl'].shift(1), 'DMZ'] = max_value1
        # df['DMZ'].fillna(value=0, inplace=True)
        df['DMZ'] = np.where((df['hl'] > df['hl'].shift(1)), max_value1, 0) # DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        # df.loc[df['hl'] < df['hl'].shift(1), 'DMF'] = max_value1
        # df['DMF'].fillna(value=0, inplace=True)
        df['DMF'] = np.where((df['hl'] < df['hl'].shift(1)), max_value1, 0) # DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)

        DMZ_SUM = df['DMZ'].rolling(n).sum() # SUM(DMZ,N)
        DMF_SUM = df['DMF'].rolling(n).sum() # SUM(DMF,N)
        DIZ = DMZ_SUM / (DMZ_SUM + DMF_SUM) # DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF = DMF_SUM / (DMZ_SUM + DMF_SUM) # DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        df['前%dhDDI' % n] = DIZ - DIF
        df['前%dhDDI' % n] = df['前%dhDDI' % n].shift(1)
        extra_agg_dict['前%dhDDI' % n] = 'first'
        # 删除中间数据
        del df['hl']
        del df['abs_high']
        del df['abs_low']
        del df['DMZ']
        del df['DMF']


    # HMA 指标
    for n in back_hour_list:
        """
        N=20
        HMA=MA(HIGH,N)
        HMA 指标为简单移动平均线把收盘价替换为最高价。当最高价上穿/
        下穿 HMA 时产生买入/卖出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean() # HMA=MA(HIGH,N)
        df['前%dhHMA' % n] = df['high'] / hma - 1 # 去量纲
        df['前%dhHMA' % n] = df['前%dhHMA' % n].shift(1)
        extra_agg_dict['前%dhHMA' % n] = 'first'

    # SROC 指标
    for n in back_hour_list:
        """
        N=13
        M=21
        EMAP=EMA(CLOSE,N)
        SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROC 与 ROC 类似，但是会对收盘价进行平滑处理后再求变化率。
        """
        ema = df['close'].ewm(n, adjust=False).mean() # EMAP=EMA(CLOSE,N)
        ref = ema.shift(2 * n) # 固定俩参数之间的倍数 REF(EMAP,M)
        df['前%dhSROC' % n] = (ema - ref) / ref # SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df['前%dhSROC' % n] = df['前%dhSROC' % n].shift(1)
        extra_agg_dict['前%dhSROC' % n] = 'first'


    # DC 指标
    for n in back_hour_list:
        """
        N=20
        UPPER=MAX(HIGH,N)
        LOWER=MIN(LOW,N)
        MIDDLE=(UPPER+LOWER)/2
        DC 指标用 N 天最高价和 N 天最低价来构造价格变化的上轨和下轨，
        再取其均值作为中轨。当收盘价上穿/下穿中轨时产生买入/卖出信号。
        """
        upper = df['high'].rolling(n, min_periods=1).max() #UPPER=MAX(HIGH,N)
        lower = df['low'].rolling(n, min_periods=1).min() # LOWER=MIN(LOW,N)
        middle = (upper + lower) / 2 # MIDDLE=(UPPER+LOWER)/2
        ma_middle = middle.rolling(n, min_periods=1).mean() # 求中轨的均线
        # 进行无量纲处理
        df['前%dhDC' % n] = middle / ma_middle - 1
        df['前%dhDC' % n] = df['前%dhDC' % n].shift(1)
        extra_agg_dict['前%dhDC' % n] = 'first'

    # VIDYA
    for n in back_hour_list:
        """
        N=10
        VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER
        （EfficiencyRatio）指标。在当前趋势较强时，ER 值较大，VIDYA
        会赋予当前价格更大的权重，使得 VIDYA 紧随价格变动，减小其滞
        后性；在当前趋势较弱（比如振荡市中）,ER 值较小，VIDYA 会赋予
        当前价格较小的权重，增大 VIDYA 的滞后性，使其更加平滑，避免
        产生过多的交易信号。
        当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
        """
        df['abs_diff_close'] = abs(df['close'] - df['close'].shift(n)) # ABS(CLOSE-REF(CLOSE,N))
        df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n).sum() # SUM(ABS(CLOSE-REF(CLOSE,1))
        VI = df['abs_diff_close'] / df['abs_diff_close_sum'] # VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA = VI * df['close'] + (1 - VI) * df['close'].shift(1) # VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        # 进行无量纲处理
        df['前%dhVIDYA' % n] = VIDYA / df['close'] - 1
        df['前%dhVIDYA' % n] = df['前%dhVIDYA' % n].shift(1)
        extra_agg_dict['前%dhVIDYA' % n] = 'first'
        # 删除中间数据
        del df['abs_diff_close']
        del df['abs_diff_close_sum']

    # Qstick 指标
    for n in back_hour_list:
        """
        N=20
        Qstick=MA(CLOSE-OPEN,N)
        Qstick 通过比较收盘价与开盘价来反映股价趋势的方向和强度。如果
        Qstick 上穿/下穿 0 则产生买入/卖出信号。
        """
        cl = df['close'] - df['open'] # CLOSE-OPEN
        Qstick = cl.rolling(n, min_periods=1).mean() # Qstick=MA(CLOSE-OPEN,N)
        # 进行无量纲处理
        df['前%dhQstick' % n] = cl / Qstick - 1
        df['前%dhQstick' % n] = df['前%dhQstick' % n].shift(1)
        extra_agg_dict['前%dhQstick' % n] = 'first'

    # FB 指标
    # for n in back_hour_list:
    #     """
    #     N=20
    #     TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(
    #     CLOSE,1)))
    #     ATR=MA(TR,N)
    #     MIDDLE=MA(CLOSE,N)
    #     UPPER1=MIDDLE+1.618*ATR
    #     UPPER2=MIDDLE+2.618*ATR
    #     UPPER3=MIDDLE+4.236*ATR
    #     LOWER1=MIDDLE-1.618*ATR
    #     LOWER2=MIDDLE-2.618*ATR
    #     LOWER3=MIDDLE-4.236*ATR
    #     FB 指标类似于布林带，都以价格的移动平均线为中轨，在中线上下
    #     浮动一定数值构造上下轨。不同的是，Fibonacci Bands 有三条上轨
    #     和三条下轨，且分别为中轨加减 ATR 乘 Fibonacci 因子所得。当收盘
    #     价突破较高的两个上轨的其中之一时，产生买入信号；收盘价突破较
    #     低的两个下轨的其中之一时，产生卖出信号。
    #     """
    #     df['c1'] = df['high'] - df['low']
    #     df['c2'] = abs(df['high'] - df['close'].shift(1))
    #     df['c3'] = abs(df['low'] - df['close'].shift(1))
    #     df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    #     df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()
    #     df['middle'] = df['close'].rolling(n, min_periods=1).mean()
    #     #  添加通道部分需要后续进行过滤
    #     upper1 = df['middle'] + 1.618 * df['ATR']
    #     upper2 = df['middle'] + 2.618 * df['ATR']
    #     upper3 = df['middle'] + 4.236 * df['ATR']
    #
    #     lower1 = df['middle'] - 1.618 * df['ATR']
    #     lower2 = df['middle'] - 2.618 * df['ATR']
    #     lower3 = df['middle'] - 4.236 * df['ATR']

    # ATR 因子
    for n in back_hour_list:
        """
        N=20
        TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        ATR=MA(TR,N)
        MIDDLE=MA(CLOSE,N)
        """
        df['c1'] = df['high'] - df['low'] # HIGH-LOW
        df['c2'] = abs(df['high'] - df['close'].shift(1)) # ABS(HIGH-REF(CLOSE,1)
        df['c3'] = abs(df['low'] - df['close'].shift(1)) # ABS(LOW-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean() # ATR=MA(TR,N)
        df['middle'] = df['close'].rolling(n, min_periods=1).mean() # MIDDLE=MA(CLOSE,N)

        # ATR指标去量纲
        df['前%dhATR' % n] = df['ATR'] / df['middle']
        df['前%dhATR' % n] = df['前%dhATR' % n].shift(1)
        extra_agg_dict['前%dhATR' % n] = 'first'
        # 删除中间数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['middle']



    # DEMA 指标
    for n in back_hour_list:
        """
        N=60
        EMA=EMA(CLOSE,N)
        DEMA=2*EMA-EMA(EMA,N)
        DEMA 结合了单重 EMA 和双重 EMA，在保证平滑性的同时减少滞后
        性。
        """
        ema = df['close'].ewm(n, adjust=False).mean() # EMA=EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean() # EMA(EMA,N)
        dema = 2 * ema - ema_ema # DEMA=2*EMA-EMA(EMA,N)
        # dema 去量纲
        df['前%dhDEMA' % n] = dema / ema - 1
        df['前%dhDEMA' % n] = df['前%dhDEMA' % n].shift(1)
        extra_agg_dict['前%dhDEMA' % n] = 'first'

    # APZ 指标
    for n in back_hour_list:
        """
        N=10
        M=20
        PARAM=2
        VOL=EMA(EMA(HIGH-LOW,N),N)
        UPPER=EMA(EMA(CLOSE,M),M)+PARAM*VOL
        LOWER= EMA(EMA(CLOSE,M),M)-PARAM*VOL
        APZ（Adaptive Price Zone 自适应性价格区间）与布林线 Bollinger 
        Band 和肯通纳通道 Keltner Channel 很相似，都是根据价格波动性围
        绕均线而制成的价格通道。只是在这三个指标中计算价格波动性的方
        法不同。在布林线中用了收盘价的标准差，在肯通纳通道中用了真波
        幅 ATR，而在 APZ 中运用了最高价与最低价差值的 N 日双重指数平
        均来反映价格的波动幅度。
        """
        df['hl'] = df['high'] - df['low'] # HIGH-LOW,
        df['ema_hl'] = df['hl'].ewm(n, adjust=False).mean() # EMA(HIGH-LOW,N)
        df['vol'] = df['ema_hl'].ewm(n, adjust=False).mean() # VOL=EMA(EMA(HIGH-LOW,N),N)

        # 计算通道 可以作为CTA策略 作为因子的时候进行改造
        df['ema_close'] = df['close'].ewm(2 * n, adjust=False).mean() # EMA(CLOSE,M)
        df['ema_ema_close'] = df['ema_close'].ewm(2 * n, adjust=False).mean() # EMA(EMA(CLOSE,M),M)
        # EMA去量纲
        df['前%dhAPZ' % n] = df['vol'] / df['ema_ema_close']
        df['前%dhAPZ' % n] = df['前%dhAPZ' % n].shift(1)
        extra_agg_dict['前%dhAPZ' % n] = 'first'
        # 删除中间数据
        del df['hl']
        del df['ema_hl']
        del df['vol']
        del df['ema_close']
        del df['ema_ema_close']

    # ASI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        M=20
        ASI=CUMSUM(SI)
        ASIMA=MA(ASI,M)
        由于 SI 的波动性比较大，所以我们一般对 SI 累计求和得到 ASI 并捕
        捉 ASI 的变化趋势。一般我们不会直接看 ASI 的数值（对 SI 累计求
        和的求和起点不同会导致求出 ASI 的值不同），而是会观察 ASI 的变
        化方向。我们利用 ASI 与其均线的交叉来产生交易信号,上穿/下穿均
        线时买入/卖出。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1)) # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1)) # B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1)) # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1)) # D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1) # K=MAX(A,B)

        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D'] # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D'] # R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D'] # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'], df['R2']) # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] > df['A']) & (df['C'] >= df['B']), df['R3'], df['R4']) # R=IF((C>=A) & (C>=B),R3,R4)
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / df['R'] * df['K'] / n # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M

        df['ASI'] = df['SI'].cumsum() # ASI=CUMSUM(SI)
        df['ASI_MA'] = df['ASI'].rolling(n, min_periods=1).mean() # ASIMA=MA(ASI,M)

        df['前%dhASI' % n] = df['ASI'] / df['ASI_MA'] - 1
        df['前%dhASI' % n] = df['前%dhASI' % n].shift(1)
        extra_agg_dict['前%dhASI' % n] = 'first'
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']
        del df['ASI']
        del df['ASI_MA']

    # CR 指标
    for n in back_hour_list:
        """
        N=20
        TYP=(HIGH+LOW+CLOSE)/3
        H=MAX(HIGH-REF(TYP,1),0)
        L=MAX(REF(TYP,1)-LOW,0)
        CR=SUM(H,N)/SUM(L,N)*100
        CR 与 AR、BR 类似。CR 通过比较最高价、最低价和典型价格来衡
        量市场人气，其衡量昨日典型价格在今日最高价、最低价之间的位置。
        CR 超过 200 时，表示股价上升强势；CR 低于 50 时，表示股价下跌
        强势。如果 CR 上穿 200/下穿 50 则产生买入/卖出信号。
        """
        df['TYP'] = (df['high'] + df['low'] + df['close']) / 3 # TYP=(HIGH+LOW+CLOSE)/3
        df['H_TYP'] = df['high'] - df['TYP'].shift(1) # HIGH-REF(TYP,1)
        df['H'] = np.where(df['high'] > df['TYP'].shift(1), df['H_TYP'], 0) # H=MAX(HIGH-REF(TYP,1),0)
        df['L_TYP'] = df['TYP'].shift(1) - df['low'] # REF(TYP,1)-LOW
        df['L'] = np.where(df['TYP'].shift(1) > df['low'], df['L_TYP'], 0) # L=MAX(REF(TYP,1)-LOW,0)
        df['CR'] = df['H'].rolling(n).sum() / df['L'].rolling(n).sum() * 100 # CR=SUM(H,N)/SUM(L,N)*100
        df['前%dhCR' % n] = df['CR'].shift(1)
        extra_agg_dict['前%dhCR' % n] = 'first'
        # 删除中间数据
        del df['TYP']
        del df['H_TYP']
        del df['H']
        del df['L_TYP']
        del df['L']

    # BOP 指标
    for n in back_hour_list:
        """
        N=20
        BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)
        BOP 的变化范围为-1 到 1，用来衡量收盘价与开盘价的距离（正、负
        距离）占最高价与最低价的距离的比例，反映了市场的多空力量对比。
        如果 BOP>0，则多头更占优势；BOP<0 则说明空头更占优势。BOP
        越大，则说明价格被往最高价的方向推动得越多；BOP 越小，则说
        明价格被往最低价的方向推动得越多。我们可以用 BOP 上穿/下穿 0
        线来产生买入/卖出信号。
        """
        df['co'] = df['close'] - df['open'] #  CLOSE-OPEN
        df['hl'] = df['high'] - df['low'] # HIGH-LOW
        df['BOP'] = (df['co'] / df['hl']).rolling(n, min_periods=1).mean() # BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)

        df['前%dhBOP' % n] = df['BOP'].shift(1)
        extra_agg_dict['前%dhBOP' % n] = 'first'
        # 删除中间过程数据
        del df['co']
        del df['hl']
        del df['BOP']

    # HULLMA 指标
    for n in back_hour_list:
        """
        N=20,80
        X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        HULLMA=EMA(X,[√𝑁])
        HULLMA 也是均线的一种，相比于普通均线有着更低的延迟性。我们
        用短期均线上/下穿长期均线来产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,[N/2])
        ema2 = df['close'].ewm(n * 2, adjust=False).mean() # EMA(CLOSE,N)
        df['X'] = 2 * ema1 - ema2 # X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        df['HULLMA'] = df['X'].ewm(int(np.sqrt(2 * n)), adjust=False).mean() # HULLMA=EMA(X,[√𝑁])
        # 去量纲
        df['前%dhHULLMA' % n] = df['HULLMA'].shift(1) - 1
        extra_agg_dict['前%dhHULLMA' % n] = 'first'
        # 删除过程数据
        del df['X']
        del df['HULLMA']

    # COPP 指标
    for n in back_hour_list:
        """
        RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        COPP=WMA(RC,M)
        COPP 指标用不同时间长度的价格变化率的加权移动平均值来衡量
        动量。如果 COPP 上穿/下穿 0 则产生买入/卖出信号。
        """
        df['RC'] = 100 * ((df['close'] - df['close'].shift(n)) / df['close'].shift(n) + (
                df['close'] - df['close'].shift(2 * n)) / df['close'].shift(2 * n)) # RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        df['COPP'] = df['RC'].rolling(n, min_periods=1).mean() # COPP=WMA(RC,M)  使用ma代替wma
        df['前%dhCOPP' % n] = df['COPP'].shift(1)
        extra_agg_dict['前%dhCOPP' % n] = 'first'
        # 删除中间过程数据
        del df['RC']
        del df['COPP']

    # RSIH
    for n in back_hour_list:
        """
        N1=40
        N2=120
        CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
        OSE,1),0)
        RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLO
        SE,1)),N1,1)*100
        RSI_SIGNAL=EMA(RSI,N2)
        RSIH=RSI-RSI_SIGNAL
        RSI 指标的一个缺点波动性太大，为了使其更平滑我们可以对其作移
        动平均处理。类似于由 MACD 产生 MACD_SIGNAL 并取其差得到
        MACD_HISTOGRAM，我们对 RSI 作移动平均得到 RSI_SIGNAL，
        取两者的差得到 RSI HISTOGRAM。当 RSI HISTORGRAM 上穿 0
        时产生买入信号；当 RSI HISTORGRAM 下穿 0 产生卖出信号。
        """
        # CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
        # sma_diff_pos = df['close_diff_pos'].rolling(n, min_periods=1).mean()
        sma_diff_pos = df['close_diff_pos'].ewm(span=n).mean() # SMA(CLOSE_DIFF_POS,N1,1)
        # abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).rolling(n, min_periods=1).mean()
        # SMA(ABS(CLOSE-REF(CLOSE,1)),N1,1
        abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).ewm(span=n).mean()
        # RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N1,1)*100
        df['RSI'] = sma_diff_pos / abs_sma_diff_pos * 100
        # RSI_SIGNAL=EMA(RSI,N2)
        df['RSI_ema'] = df['RSI'].ewm(4 * n, adjust=False).mean()
        # RSIH=RSI-RSI_SIGNAL
        df['RSIH'] = df['RSI'] - df['RSI_ema']

        df['前%dhRSIH' % n] = df['RSIH'].shift(1)
        extra_agg_dict['前%dhRSIH' % n] = 'first'
        # 删除中间过程数据
        del df['close_diff_pos']
        del df['RSI']
        del df['RSI_ema']
        del df['RSIH']

    # HLMA 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        HMA=MA(HIGH,N1)
        LMA=MA(LOW,N2)
        HLMA 指标是把普通的移动平均中的收盘价换为最高价和最低价分
        别得到 HMA 和 LMA。当收盘价上穿 HMA/下穿 LMA 时产生买入/卖
        出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean() # HMA=MA(HIGH,N1)
        lma = df['low'].rolling(n, min_periods=1).mean() # LMA=MA(LOW,N2)
        df['HLMA'] = hma - lma # 可自行改造
        df['HLMA_mean'] = df['HLMA'].rolling(n, min_periods=1).mean()

        # 去量纲
        df['前%dhHLMA' % n] = df['HLMA'] / df['HLMA_mean'] - 1
        df['前%dhHLMA' % n] = df['前%dhHLMA' % n].shift(1)
        extra_agg_dict['前%dhHLMA' % n] = 'first'
        # 删除中间过程数据
        del df['HLMA']
        del df['HLMA_mean']

    # TRIX 指标
    for n in back_hour_list:
        """
        TRIPLE_EMA=EMA(EMA(EMA(CLOSE,N),N),N)
        TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
        TRIX 求价格的三重指数移动平均的变化率。当 TRIX>0 时，当前可
        能处于上涨趋势；当 TRIX<0 时，当前可能处于下跌趋势。TRIX 相
        比于普通移动平均的优点在于它通过三重移动平均去除了一些小的
        趋势和市场的噪音。我们可以通过 TRIX 上穿/下穿 0 线产生买入/卖
        出信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean() # EMA(EMA(EMA(CLOSE,N),N),N)
        # TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
        df['TRIX'] = (df['ema_ema_ema'] - df['ema_ema_ema'].shift(1)) / df['ema_ema_ema'].shift(1)

        df['前%dhTRIX' % n] = df['TRIX'].shift(1)
        extra_agg_dict['前%dhTRIX' % n] = 'first'
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TRIX']

    # WC 指标
    for n in back_hour_list:
        """
        WC=(HIGH+LOW+2*CLOSE)/4
        N1=20
        N2=40
        EMA1=EMA(WC,N1)
        EMA2=EMA(WC,N2)
        WC 也可以用来代替收盘价构造一些技术指标（不过相对比较少用
        到）。我们这里用 WC 的短期均线和长期均线的交叉来产生交易信号。
        """
        WC = (df['high'] + df['low'] + 2 * df['close']) / 4  # WC=(HIGH+LOW+2*CLOSE)/4
        df['ema1'] = WC.ewm(n, adjust=False).mean()  # EMA1=EMA(WC,N1)
        df['ema2'] = WC.ewm(2 * n, adjust=False).mean() # EMA2=EMA(WC,N2)
        # 去量纲
        df['前%dhWC' % n] = df['ema1'] / df['ema2'] - 1
        df['前%dhWC' % n] = df['前%dhWC' % n].shift(1)
        extra_agg_dict['前%dhWC' % n] = 'first'
        # 删除中间过程数据
        del df['ema1']
        del df['ema2']

    # ADX 指标
    for n in back_hour_list:
        """
        N1=14
        MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
        MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
        XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
        PDM=SUM(XPDM,N1)
        XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
        NDM=SUM(XNDM,N1)
        TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
        TR=SUM(TR,N1)
        DI+=PDM/TR
        DI-=NDM/TR
        ADX 指标计算过程中的 DI+与 DI-指标用相邻两天的最高价之差与最
        低价之差来反映价格的变化趋势。当 DI+上穿 DI-时，产生买入信号；
        当 DI+下穿 DI-时，产生卖出信号。
        """
        # MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
        df['max_high'] = np.where(df['high'] > df['high'].shift(1), df['high'] - df['high'].shift(1), 0)
        # MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
        df['max_low'] = np.where(df['low'].shift(1) > df['low'], df['low'].shift(1) - df['low'], 0)
        # XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
        df['XPDM'] = np.where(df['max_high'] > df['max_low'], df['high'] - df['high'].shift(1), 0)
        # PDM=SUM(XPDM,N1)
        df['PDM'] = df['XPDM'].rolling(n).sum()
        # XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
        df['XNDM'] = np.where(df['max_low'] > df['max_high'], df['low'].shift(1) - df['low'], 0)
        # NDM=SUM(XNDM,N1)
        df['NDM'] = df['XNDM'].rolling(n).sum()
        # ABS(HIGH-LOW)
        df['c1'] = abs(df['high'] - df['low'])
        # ABS(HIGH-CLOSE)
        df['c2'] = abs(df['high'] - df['close'])
        # ABS(LOW-CLOSE)
        df['c3'] = abs(df['low'] - df['close'])
        # TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
        # TR=SUM(TR,N1)
        df['TR_sum'] = df['TR'].rolling(n).sum()
        # DI+=PDM/TR
        df['DI+'] = df['PDM'] / df['TR']
        # DI-=NDM/TR
        df['DI-'] = df['NDM'] / df['TR']

        df['前%dhADX_DI+' % n] = df['DI+'].shift(1)
        df['前%dhADX_DI-' % n] = df['DI-'].shift(1)
        # 去量纲
        df['ADX'] = (df['PDM'] + df['NDM']) / df['TR']

        df['前%dhADX' % n] = df['ADX'].shift(1)
        extra_agg_dict['前%dhADX' % n] = 'first'
        extra_agg_dict['前%dhADX_DI+' % n] = 'first'
        extra_agg_dict['前%dhADX_DI-' % n] = 'first'
        # 删除中间过程数据
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
        del df['DI+']
        del df['DI-']
        del df['ADX']

    # FISHER指标
    for n in back_hour_list:
        """
        N=20
        PARAM=0.3
        PRICE=(HIGH+LOW)/2
        PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-
        0.5)
        PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99 
        PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        PRICE_CHANGE=PARAM*PRICE_CH+(1-PARAM)*REF(PRICE_CHANGE,1)
        FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        PRICE_CH 用来衡量当前价位于过去 N 天的最高价和最低价之间的
        位置。Fisher Transformation 是一个可以把股价数据变为类似于正态
        分布的方法。Fisher 指标的优点是减少了普通技术指标的滞后性。
        """
        PARAM = 1 / n
        df['price'] = (df['high'] + df['low']) / 2 # PRICE=(HIGH+LOW)/2
        df['min_low'] = df['low'].rolling(n).min() # MIN(LOW,N)
        df['max_high'] = df['high'].rolling(n).max() # MAX(HIGH,N)
        df['price_ch'] = 2 * (df['price'] - df['min_low']) / (df['max_high'] - df['low']) - 0.5 #         PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-0.5)
        df['price_change'] = PARAM * df['price_ch'] + (1 - PARAM) * df['price_ch'].shift(1)
        df['price_change'] = np.where(df['price_change'] > 0.99, 0.999, df['price_change']) # PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99
        df['price_change'] = np.where(df['price_change'] < -0.99, -0.999, df['price_change']) # PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        # 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        df['FISHER'] = 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        # FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        df['FISHER'] = 0.5 * df['FISHER'].shift(1) + 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))

        df['前%dhFISHER' % n] = df['FISHER'].shift(1)
        extra_agg_dict['前%dhFISHER' % n] = 'first'
        # 删除中间数据
        del df['price']
        del df['min_low']
        del df['max_high']
        del df['price_ch']
        del df['price_change']
        del df['FISHER']

    # Demakder 指标
    for n in back_hour_list:
        """
        N=20
        Demax=HIGH-REF(HIGH,1)
        Demax=IF(Demax>0,Demax,0)
        Demin=REF(LOW,1)-LOW
        Demin=IF(Demin>0,Demin,0)
        Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        当 Demaker>0.7 时上升趋势强烈，当 Demaker<0.3 时下跌趋势强烈。
        当 Demaker 上穿 0.7/下穿 0.3 时产生买入/卖出信号。
        """
        df['Demax'] = df['high'] - df['high'].shift(1) # Demax=HIGH-REF(HIGH,1)
        df['Demax'] = np.where(df['Demax'] > 0, df['Demax'], 0) # Demax=IF(Demax>0,Demax,0)
        df['Demin'] = df['low'].shift(1) - df['low'] # Demin=REF(LOW,1)-LOW
        df['Demin'] = np.where(df['Demin'] > 0, df['Demin'], 0) # Demin=IF(Demin>0,Demin,0)
        df['Demax_ma'] = df['Demax'].rolling(n, min_periods=1).mean() # MA(Demax,N)
        df['Demin_ma'] = df['Demin'].rolling(n, min_periods=1).mean() # MA(Demin,N)
        df['Demaker'] = df['Demax_ma'] / (df['Demax_ma'] + df['Demin_ma']) # Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        df['前%dhDemaker' % n] = df['Demaker'].shift(1)
        extra_agg_dict['前%dhDemaker' % n] = 'first'
        # 删除中间过程数据
        del df['Demax']
        del df['Demin']
        del df['Demax_ma']
        del df['Demin_ma']
        del df['Demaker']

    # IC 指标
    for n in back_hour_list:
        """
        N1=9
        N2=26
        N3=52
        TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        SPAN_A=(TS+KS)/2
        SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2
        在 IC 指标中，SPAN_A 与 SPAN_B 之间的部分称为云。如果价格在
        云上，则说明是上涨趋势（如果 SPAN_A>SPAN_B，则上涨趋势强
        烈；否则上涨趋势较弱）；如果价格在云下，则为下跌趋势（如果
        SPAN_A<SPAN_B，则下跌趋势强烈；否则下跌趋势较弱）。该指
        标的使用方式与移动平均线有许多相似之处，比如较快的线（TS）突
        破较慢的线（KS），价格突破 KS,价格突破云，SPAN_A 突破 SPAN_B
        等。我们产生信号的方式是：如果价格在云上方 SPAN_A>SPAN_B，
        则当价格上穿 KS 时买入；如果价格在云下方且 SPAN_A<SPAN_B，
        则当价格下穿 KS 时卖出。
        """
        n2 = 3 * n
        n3 = 2 * n2
        df['max_high_1'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N1)
        df['min_low_1'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N1)
        df['TS'] = (df['max_high_1'] + df['min_low_1']) / 2 # TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['max_high_2'] = df['high'].rolling(n2, min_periods=1).max() # MAX(HIGH,N2)
        df['min_low_2'] = df['low'].rolling(n2, min_periods=1).min() # MIN(LOW,N2)
        df['KS'] = (df['max_high_2'] + df['min_low_2']) / 2 # KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        df['span_A'] = (df['TS'] + df['KS']) / 2 # SPAN_A=(TS+KS)/2
        df['max_high_3'] = df['high'].rolling(n3, min_periods=1).max() # MAX(HIGH,N3)
        df['min_low_3'] = df['low'].rolling(n3, min_periods=1).min() # MIN(LOW,N3)
        df['span_B'] = (df['max_high_3'] + df['min_low_3']) / 2 # SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2

        # 去量纲
        df['前%dhIC' % n] = df['span_A'] / df['span_B']
        df['前%dhIC' % n] = df['前%dhIC' % n].shift(1)
        extra_agg_dict['前%dhIC' % n] = 'first'
        # 删除中间过程数据
        del df['max_high_1']
        del df['max_high_2']
        del df['max_high_3']
        del df['min_low_1']
        del df['min_low_2']
        del df['min_low_3']
        del df['TS']
        del df['KS']
        del df['span_A']
        del df['span_B']

    # TSI 指标
    for n in back_hour_list:
        """
        N1=25
        N2=13
        TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(
        CLOSE-REF(CLOSE,1)),N1),N2)*100
        TSI 是一种双重移动平均指标。与常用的移动平均指标对收盘价取移
        动平均不同，TSI 对两天收盘价的差值取移动平均。如果 TSI 上穿 10/
        下穿-10 则产生买入/卖出指标。
        """
        n1 = 2 * n
        df['diff_close'] = df['close'] - df['close'].shift(1) #  CLOSE-REF(CLOSE,1)
        df['ema'] = df['diff_close'].ewm(n1, adjust=False).mean() # EMA(CLOSE-REF(CLOSE,1),N1)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean() # EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)

        df['abs_diff_close'] = abs(df['diff_close']) # ABS(CLOSE-REF(CLOSE,1))
        df['abs_ema'] = df['abs_diff_close'].ewm(n1, adjust=False).mean() # EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        df['abs_ema_ema'] = df['abs_ema'].ewm(n, adjust=False).mean() # EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        # TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1),N2)*100
        df['TSI'] = df['ema_ema'] / df['abs_ema_ema'] * 100

        df['前%dhTSI' % n] = df['TSI'].shift(1)
        extra_agg_dict['前%dhTSI' % n] = 'first'
        # 删除中间过程数据
        del df['diff_close']
        del df['ema']
        del df['ema_ema']
        del df['abs_diff_close']
        del df['abs_ema']
        del df['abs_ema_ema']
        del df['TSI']

    # LMA 指标
    for n in back_hour_list:
        """
        N=20
        LMA=MA(LOW,N)
        LMA 为简单移动平均把收盘价替换为最低价。如果最低价上穿/下穿
        LMA 则产生买入/卖出信号。
        """
        df['low_ma'] = df['low'].rolling(n, min_periods=1).mean() # LMA=MA(LOW,N)
        # 进行去量纲
        df['前%dhLMA' % n] = df['low'] / df['low_ma'] - 1
        df['前%dhLMA' % n] = df['前%dhLMA' % n].shift(1)
        extra_agg_dict['前%dhLMA' % n] = 'first'
        # 删除中间过程数据
        del df['low_ma']

    # IMI 指标
    for n in back_hour_list:
        """
        N=14
        INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        IMI=INC/(INC+DEC)
        IMI 的计算方法与 RSI 很相似。其区别在于，在 IMI 计算过程中使用
        的是收盘价和开盘价，而 RSI 使用的是收盘价和前一天的收盘价。所
        以，RSI 做的是前后两天的比较，而 IMI 做的是同一个交易日内的比
        较。如果 IMI 上穿 80，则产生买入信号；如果 IMI 下穿 20，则产生
        卖出信号。
        """
        df['INC'] = np.where(df['close'] > df['open'], df['close'] - df['open'], 0) # IF(CLOSE>OPEN,CLOSE-OPEN,0)
        df['INC_sum'] = df['INC'].rolling(n).sum() # INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        df['DEC'] = np.where(df['open'] > df['close'], df['open'] - df['close'], 0) # IF(OPEN>CLOSE,OPEN-CLOSE,0)
        df['DEC_sum'] = df['DEC'].rolling(n).sum() # DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        df['IMI'] = df['INC_sum'] / (df['INC_sum'] + df['DEC_sum']) # IMI=INC/(INC+DEC)

        df['前%dhIMI' % n] = df['IMI'].shift(1)
        extra_agg_dict['前%dhIMI' % n] = 'first'
        # 删除中间过程数据
        del df['INC']
        del df['INC_sum']
        del df['DEC']
        del df['DEC_sum']
        del df['IMI']

    # VI 指标
    for n in back_hour_list:
        """
        TR=MAX([ABS(HIGH-LOW),ABS(LOW-REF(CLOSE,1)),ABS(HIG
        H-REF(CLOSE,1))])
        VMPOS=ABS(HIGH-REF(LOW,1))
        VMNEG=ABS(LOW-REF(HIGH,1))
        N=40
        SUMPOS=SUM(VMPOS,N)
        SUMNEG=SUM(VMNEG,N)
        TRSUM=SUM(TR,N)
        VI+=SUMPOS/TRSUM
        VI-=SUMNEG/TRSUM
        VI 指标可看成 ADX 指标的变形。VI 指标中的 VI+与 VI-与 ADX 中的
        DI+与 DI-类似。不同的是 ADX 中用当前高价与前一天高价的差和当
        前低价与前一天低价的差来衡量价格变化，而 VI 指标用当前当前高
        价与前一天低价和当前低价与前一天高价的差来衡量价格变化。当
        VI+上穿/下穿 VI-时，多/空的信号更强，产生买入/卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low']) # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1)) # ABS(LOW-REF(CLOSE,1)
        df['c3'] = abs(df['high'] - df['close'].shift(1))# ABS(HIGH-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX([ABS(HIGH-LOW),ABS(LOW-REF(CLOSE,1)),ABS(HIGH-REF(CLOSE,1))])

        df['VMPOS'] = abs(df['high'] - df['low'].shift(1)) # VMPOS=ABS(HIGH-REF(LOW,1))
        df['VMNEG'] = abs(df['low'] - df['high'].shift(1)) # VMNEG=ABS(LOW-REF(HIGH,1))
        df['sum_pos'] = df['VMPOS'].rolling(n).sum()  # SUMPOS=SUM(VMPOS,N)
        df['sum_neg'] = df['VMNEG'].rolling(n).sum() # SUMNEG=SUM(VMNEG,N)

        df['sum_tr'] = df['TR'].rolling(n).sum() # TRSUM=SUM(TR,N)
        df['VI+'] = df['sum_pos'] / df['sum_tr'] # VI+=SUMPOS/TRSUM
        df['VI-'] = df['sum_neg'] / df['sum_tr'] # VI-=SUMNEG/TRSUM
        df['前%dhVI+' % n] = df['VI+'].shift(1)
        df['前%dhVI-' % n] = df['VI-'].shift(1)
        extra_agg_dict['前%dhVI+' % n] = 'first'
        extra_agg_dict['前%dhVI-' % n] = 'first'
        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['VMPOS']
        del df['VMNEG']
        del df['sum_pos']
        del df['sum_neg']
        del df['sum_tr']
        del df['VI+']
        del df['VI-']

    # RWI 指标
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(
        CLOSE,1)-LOW))
        ATR=MA(TR,N)
        RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        RWI（随机漫步指标）对一段时间股票的随机漫步区间与真实运动区
        间进行比较以判断股票价格的走势。
        如果 RWIH>1，说明股价长期是上涨趋势，则产生买入信号；
        如果 RWIL>1，说明股价长期是下跌趋势，则产生卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low']) # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1)) # ABS(HIGH-REF(CLOSE,1))
        df['c3'] = abs(df['high'] - df['close'].shift(1)) # ABS(REF(CLOSE,1)-LOW)
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-LOW))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean() # ATR=MA(TR,N)
        df['RWIH'] = (df['high'] - df['low'].shift(1)) / (df['ATR'] * np.sqrt(n)) # RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        df['RWIL'] = (df['high'].shift(1) - df['low']) / (df['ATR'] * np.sqrt(n)) # RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        df['前%dhRWIH' % n] = df['RWIH'].shift(1)
        df['前%dhRWIL' % n] = df['RWIL'].shift(1)
        extra_agg_dict['前%dhRWIH' % n] = 'first'
        extra_agg_dict['前%dhRWIL' % n] = 'first'
        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['RWIH']
        del df['RWIL']

    # CMO 指标
    for n in back_hour_list:
        """
        N=20
        SU=SUM(MAX(CLOSE-REF(CLOSE,1),0),N)
        SD=SUM(MAX(REF(CLOSE,1)-CLOSE,0),N)
        CMO=(SU-SD)/(SU+SD)*100
        CMO指标用过去N天的价格上涨量和价格下跌量得到，可以看作RSI
        指标的变形。CMO>(<)0 表示当前处于上涨（下跌）趋势，CMO 越
        大（小）则当前上涨（下跌）趋势越强。我们用 CMO 上穿 30/下穿-30
        来产生买入/卖出信号。
        """
        # MAX(CLOSE-REF(CLOSE,1), 0
        df['max_su'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)

        df['sum_su'] = df['max_su'].rolling(n).sum() # SU=SUM(MAX(CLOSE-REF(CLOSE,1),0),N)
        # MAX(REF(CLOSE,1)-CLOSE,0)
        df['max_sd'] = np.where(df['close'].shift(1) > df['close'], df['close'].shift(1) - df['close'], 0)
        # SD=SUM(MAX(REF(CLOSE,1)-CLOSE,0),N)
        df['sum_sd'] = df['max_su'].rolling(n).sum()
        # CMO=(SU-SD)/(SU+SD)*100
        df['CMO'] = (df['sum_su'] - df['sum_sd']) / (df['sum_su'] + df['sum_sd']) * 100

        df['前%dhCMO' % n] = df['CMO'].shift(1)
        extra_agg_dict['前%dhCMO' % n] = 'first'
        # 删除中间过程数据
        del df['max_su']
        del df['sum_su']
        del df['max_sd']
        del df['sum_sd']
        del df['CMO']

    # OSC 指标
    for n in back_hour_list:
        """
        N=40
        M=20
        OSC=CLOSE-MA(CLOSE,N)
        OSCMA=MA(OSC,M)
        OSC 反映收盘价与收盘价移动平均相差的程度。如果 OSC 上穿/下 穿 OSCMA 则产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(2 * n, min_periods=1).mean() #MA(CLOSE,N)
        df['OSC'] = df['close'] - df['ma'] # OSC=CLOSE-MA(CLOSE,N)
        df['OSCMA'] = df['OSC'].rolling(n, min_periods=1).mean() # OSCMA=MA(OSC,M)
        df['前%dhOSC' % n] = df['OSCMA'].shift(1)
        extra_agg_dict['前%dhOSC' % n] = 'first'
        # 删除中间过程数据
        del df['ma']
        del df['OSC']
        del df['OSCMA']

    # CLV 指标
    for n in back_hour_list:
        """
        N=60
        CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        CLVMA=MA(CLV,N)
        CLV 用来衡量收盘价在最低价和最高价之间的位置。当
        CLOSE=HIGH 时，CLV=1;当 CLOSE=LOW 时，CLV=-1;当 CLOSE
        位于 HIGH 和 LOW 的中点时，CLV=0。CLV>0（<0），说明收盘价
        离最高（低）价更近。我们用 CLVMA 上穿/下穿 0 来产生买入/卖出
        信号。
        """
        # CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        df['CLV'] = (2 * df['close'] - df['low'] - df['high']) / (df['high'] - df['low'])
        df['CLVMA'] = df['CLV'].rolling(n, min_periods=1).mean() # CLVMA=MA(CLV,N)
        df['前%dhCLV' % n] = df['CLVMA'].shift(1)
        extra_agg_dict['前%dhCLV' % n] = 'first'
        # 删除中间过程数据
        del df['CLV']
        del df['CLVMA']

    #  WAD 指标
    for n in back_hour_list:
        """
        TRH=MAX(HIGH,REF(CLOSE,1))
        TRL=MIN(LOW,REF(CLOSE,1))
        AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH) 
        AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))  # 该指标怀疑有误
        WAD=CUMSUM(AD)
        N=20
        WADMA=MA(WAD,N)
        我们用 WAD 上穿/下穿其均线来产生买入/卖出信号。
        """
        df['ref_close'] = df['close'].shift(1) # REF(CLOSE,1)
        df['TRH'] = df[['high', 'ref_close']].max(axis=1) # TRH=MAX(HIGH,REF(CLOSE,1))
        df['TRL'] = df[['low', 'ref_close']].min(axis=1) # TRL=MIN(LOW,REF(CLOSE,1))
        # AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH)
        df['AD'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['TRL'], df['close'] - df['TRH'])
        # AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))
        df['AD'] = np.where(df['close'] > df['close'].shift(1), 0, df['close'] - df['close'].shift(1))
        # WAD=CUMSUM(AD)
        df['WAD'] = df['AD'].cumsum()
        # WADMA=MA(WAD,N)
        df['WADMA'] = df['WAD'].rolling(n, min_periods=1).mean()
        # 去量纲
        df['前%dhWAD' % n] = df['WAD'] / df['WADMA'] - 1
        df['前%dhWAD' % n] = df['前%dhWAD' % n].shift(1)
        extra_agg_dict['前%dhWAD' % n] = 'first'
        # 删除中间过程数据
        del df['ref_close']
        del df['TRH']
        del df['AD']
        del df['WAD']
        del df['WADMA']

    # BIAS36
    for n in back_hour_list:
        """
        N=6
        BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        MABIAS36=MA(BIAS36,N)
        类似于乖离用来衡量当前价格与移动平均价的差距，三六乖离用来衡
        量不同的移动平均价间的差距。当三六乖离上穿/下穿其均线时，产生
        买入/卖出信号。
        """
        df['ma3'] = df['close'].rolling(n, min_periods=1).mean() # MA(CLOSE,3)
        df['ma6'] = df['close'].rolling(2 * n, min_periods=1).mean() # MA(CLOSE,6)
        df['BIAS36'] = df['ma3'] - df['ma6'] # BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        df['MABIAS36'] = df['BIAS36'].rolling(2 * n, min_periods=1).mean() # MABIAS36=MA(BIAS36,N)
        # 去量纲
        df['前%dhBIAS36' % n] = df['BIAS36'] / df['MABIAS36']
        df['前%dhBIAS36' % n] = df['前%dhBIAS36' % n].shift(1)
        extra_agg_dict['前%dhBIAS36' % n] = 'first'
        # 删除中间过程数据
        del df['ma3']
        del df['ma6']
        del df['BIAS36']
        del df['MABIAS36']

    # TEMA 指标
    for n in back_hour_list:
        """
        N=20,40
        TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        TEMA 结合了单重、双重和三重的 EMA，相比于一般均线延迟性较
        低。我们用快、慢 TEMA 的交叉来产生交易信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean() # EMA(EMA(EMA(CLOSE,N),N),N)
        df['TEMA'] = 3 * df['ema'] - 3 * df['ema_ema'] + df['ema_ema_ema'] # TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        # 去量纲
        df['前%dhTEMA' % n] = df['ema'] / df['TEMA'] - 1
        df['前%dhTEMA' % n] = df['前%dhTEMA' % n].shift(1)
        extra_agg_dict['前%dhTEMA' % n] = 'first'
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TEMA']

    # REG 指标
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """

        # df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
        # df['reg'] = df['close'] / df['ref_close'] - 1

        # sklearn 线性回归
        def reg_ols(_y):
            _x = np.arange(n) + 1
            model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
            y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
            return y_pred[-1]

        df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y)) # 求数据拟合的线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1

        df['前%dhREG' % n] = df['reg'].shift(1)
        extra_agg_dict['前%dhREG' % n] = 'first'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']

    # PSY 指标
    for n in back_hour_list:
        """
        N=12
        PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        PSY 指标为过去 N 天股价上涨的天数的比例*100，用来衡量投资者
        心理和市场的人气。当 PSY 处于 40 和 60 之间时，多、空力量相对
        平衡，当 PSY 上穿 60 时，多头力量比较强，产生买入信号；当 PSY
        下穿 40 时，空头力量比较强，产生卖出信号。
        """
        df['P'] = np.where(df['close'] > df['close'].shift(1), 1, 0) #IF(CLOSE>REF(CLOSE,1),1,0)

        df['PSY'] = df['P'] / n * 100 # PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        df['前%dhPSY' % n] = df['PSY'].shift(1)
        extra_agg_dict['前%dhPSY' % n] = 'first'
        # 删除中间过程数据
        del df['P']
        del df['PSY']

    # DMA 指标
    for n in back_hour_list:
        """
        DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        AMA=MA(DMA,N1)
        DMA 衡量快速移动平均与慢速移动平均之差。用 DMA 上穿/下穿其
        均线产生买入/卖出信号。
        """
        df['ma1'] = df['close'].rolling(n, min_periods=1).mean() # MA(CLOSE,N1)
        df['ma2'] = df['close'].rolling(n * 3, min_periods=1).mean() # MA(CLOSE,N2)
        df['DMA'] = df['ma1'] - df['ma2'] # DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        df['AMA'] = df['DMA'].rolling(n, min_periods=1).mean() # AMA=MA(DMA,N1)
        # 去量纲
        df['前%dhDMA' % n] = df['DMA'] / df['AMA'] - 1
        df['前%dhDMA' % n] = df['前%dhDMA' % n].shift(1)
        extra_agg_dict['前%dhDMA' % n] = 'first'
        # 删除中间过程数据
        del df['ma1']
        del df['ma2']
        del df['DMA']
        del df['AMA']

    # KST 指标
    for n in back_hour_list:
        """
        ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        ROC_MA2=MA(CLOSE -REF(CLOSE,15),10)
        ROC_MA3=MA(CLOSE -REF(CLOSE,20),10)
        ROC_MA4=MA(CLOSE -REF(CLOSE,30),10)
        KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        KST=MA(KST_IND,9)
        KST 结合了不同时间长度的 ROC 指标。如果 KST 上穿/下穿 0 则产
        生买入/卖出信号。
        """
        df['ROC1'] = df['close'] - df['close'].shift(n) # CLOSE-REF(CLOSE,10)
        df['ROC_MA1'] = df['ROC1'].rolling(n, min_periods=1).mean() # ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        df['ROC2'] = df['close'] - df['close'].shift(int(n * 1.5))
        df['ROC_MA2'] = df['ROC2'].rolling(n, min_periods=1).mean()
        df['ROC3'] = df['close'] - df['close'].shift(int(n * 2))
        df['ROC_MA3'] = df['ROC3'].rolling(n, min_periods=1).mean()
        df['ROC4'] = df['close'] - df['close'].shift(int(n * 3))
        df['ROC_MA4'] = df['ROC4'].rolling(n, min_periods=1).mean()
        # KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        df['KST_IND'] = df['ROC_MA1'] + df['ROC_MA2'] * 2 + df['ROC_MA3'] * 3 + df['ROC_MA4'] * 4
        # KST=MA(KST_IND,9)
        df['KST'] = df['KST_IND'].rolling(n, min_periods=1).mean()
        # 去量纲
        df['前%dhKST' % n] = df['KST_IND'] / df['KST'] - 1
        df['前%dhKST' % n] = df['前%dhKST' % n].shift(1)
        extra_agg_dict['前%dhKST' % n] = 'first'
        # 删除中间过程数据
        del df['ROC1']
        del df['ROC2']
        del df['ROC3']
        del df['ROC4']
        del df['ROC_MA1']
        del df['ROC_MA2']
        del df['ROC_MA3']
        del df['ROC_MA4']
        del df['KST_IND']
        del df['KST']

    # MICD 指标
    for n in back_hour_list:
        """
        N=20
        N1=10
        N2=20
        M=10
        MI=CLOSE-REF(CLOSE,1)
        MTMMA=SMA(MI,N,1)
        DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        MICD=SMA(DIF,M,1)
        如果 MICD 上穿 0，则产生买入信号；
        如果 MICD 下穿 0，则产生卖出信号。
        """
        df['MI'] = df['close'] - df['close'].shift(1) # MI=CLOSE-REF(CLOSE,1)
        # df['MIMMA'] = df['MI'].rolling(n, min_periods=1).mean()
        df['MIMMA'] = df['MI'].ewm(span=n).mean() # MTMMA=SMA(MI,N,1)
        df['MIMMA_MA1'] = df['MIMMA'].shift(1).rolling(n, min_periods=1).mean() # MA(REF(MTMMA,1),N1)
        df['MIMMA_MA2'] = df['MIMMA'].shift(1).rolling(2 * n, min_periods=1).mean() # MA(REF(MTMMA,1),N2)
        df['DIF'] = df['MIMMA_MA1'] - df['MIMMA_MA2'] # DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        # df['MICD'] = df['DIF'].rolling(n, min_periods=1).mean()
        df['MICD'] = df['DIF'].ewm(span=n).mean()
        # 去量纲
        df['前%dhMICD' % n] = df['DIF'] / df['MICD']
        df['前%dhMICD' % n] = df['前%dhMICD' % n].shift(1)
        extra_agg_dict['前%dhMICD' % n] = 'first'
        # 删除中间过渡数据
        del df['MI']
        del df['MIMMA']
        del df['MIMMA_MA1']
        del df['MIMMA_MA2']
        del df['DIF']
        del df['MICD']

    # PMO 指标
    for n in back_hour_list:
        """
        N1=10
        N2=40
        N3=20
        ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        ROC_MA=DMA(ROC,2/N1)
        ROC_MA10=ROC_MA*10
        PMO=DMA(ROC_MA10,2/N2)
        PMO_SIGNAL=DMA(PMO,2/(N3+1))
        PMO 指标是 ROC 指标的双重平滑（移动平均）版本。与 SROC 不 同(SROC 是先对价格作平滑再求 ROC)，而 PMO 是先求 ROC 再对
        ROC 作平滑处理。PMO 越大（大于 0），则说明市场上涨趋势越强；
        PMO 越小（小于 0），则说明市场下跌趋势越强。如果 PMO 上穿/
        下穿其信号线，则产生买入/卖出指标。
        """
        df['ROC'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100 # ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        df['ROC_MA'] = df['ROC'].rolling(n, min_periods=1).mean() # ROC_MA=DMA(ROC,2/N1)
        df['ROC_MA10'] = df['ROC_MA'] * 10 # ROC_MA10=ROC_MA*10
        df['PMO'] = df['ROC_MA10'].rolling(4 * n, min_periods=1).mean() # PMO=DMA(ROC_MA10,2/N2)
        df['PMO_SIGNAL'] = df['PMO'].rolling(2 * n, min_periods=1).mean() # PMO_SIGNAL=DMA(PMO,2/(N3+1))

        df['前%dhPMO' % n] = df['PMO_SIGNAL'].shift(1)
        extra_agg_dict['前%dhPMO' % n] = 'first'
        # 删除中间过渡数据
        del df['ROC']
        del df['ROC_MA']
        del df['ROC_MA10']
        del df['PMO']
        del df['PMO_SIGNAL']

    # RCCD 指标
    for n in back_hour_list:
        """
        M=40
        N1=20
        N2=40
        RC=CLOSE/REF(CLOSE,M)
        ARC1=SMA(REF(RC,1),M,1)
        DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        RCCD=SMA(DIF,M,1)
        RC 指标为当前价格与昨日价格的比值。当 RC 指标>1 时，说明价格在上升；当 RC 指标增大时，说明价格上升速度在增快。当 RC 指标
        <1 时，说明价格在下降；当 RC 指标减小时，说明价格下降速度在增
        快。RCCD 指标先对 RC 指标进行平滑处理，再取不同时间长度的移
        动平均的差值，再取移动平均。如 RCCD 上穿/下穿 0 则产生买入/
        卖出信号。
        """
        df['RC'] = df['close'] / df['close'].shift(2 * n) # RC=CLOSE/REF(CLOSE,M)
        # df['ARC1'] = df['RC'].rolling(2 * n, min_periods=1).mean()
        df['ARC1'] = df['RC'].ewm(span=2 * n).mean() # ARC1=SMA(REF(RC,1),M,1)
        df['MA1'] = df['ARC1'].shift(1).rolling(n, min_periods=1).mean() # MA(REF(ARC1,1),N1)
        df['MA2'] = df['ARC1'].shift(1).rolling(2 * n, min_periods=1).mean() # MA(REF(ARC1,1),N2)
        df['DIF'] = df['MA1'] - df['MA2'] # DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        # df['RCCD'] = df['DIF'].rolling(2 * n, min_periods=1).mean()
        df['RCCD'] = df['DIF'].ewm(span=2 * n).mean() # RCCD=SMA(DIF,M,1)

        df['前%dhRCCD' % n] = df['RCCD'].shift(1)
        extra_agg_dict['前%dhRCCD' % n] = 'first'
        # 删除中间数据
        del df['RC']
        del df['ARC1']
        del df['MA1']
        del df['MA2']
        del df['DIF']
        del df['RCCD']

        # KAMA 指标
    for n in back_hour_list:
        """
        N=10
        N1=2
        N2=30
        DIRECTION=CLOSE-REF(CLOSE,N)
        VOLATILITY=SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        ER=DIRETION/VOLATILITY
        FAST=2/(N1+1)
        SLOW=2/(N2+1)
        SMOOTH=ER*(FAST-SLOW)+SLOW
        COF=SMOOTH*SMOOTH
        KAMA=COF*CLOSE+(1-COF)*REF(KAMA,1)
        KAMA 指标与 VIDYA 指标类似，都是把 ER(EfficiencyRatio)指标加
        入到移动平均的权重中，其用法与其他移动平均线类似。在当前趋势
        较强时，ER 值较大，KAMA 会赋予当前价格更大的权重，使得 KAMA
        紧随价格变动，减小其滞后性；在当前趋势较弱（比如振荡市中）,ER
        值较小，KAMA 会赋予当前价格较小的权重，增大 KAMA 的滞后性，
        使其更加平滑，避免产生过多的交易信号。与 VIDYA 指标不同的是，
        KAMA 指标可以设置权值的上界 FAST 和下界 SLOW。
        """
        N = 5 * n
        N2 = 15 * n

        df['DIRECTION'] = df['close'] - df['close'].shift(N) #  DIRECTION=CLOSE-REF(CLOSE,N)
        df['abs_ref'] =abs(df['close'] - df['close'].shift(1)) # ABS(CLOSE-REF(CLOSE,1))
        df['VOLATILITY'] = df['abs_ref'].rolling(N).sum() # VOLATILITY=SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        df['ER'] = df['DIRECTION'] / df['VOLATILITY']
        fast = 2 / (n + 1) # FAST=2/(N1+1)
        slow = 2/ (N2 + 1) # SLOW=2/(N2+1)
        df['SMOOTH'] = df['ER']  * (fast - slow) + slow # SMOOTH=ER*(FAST-SLOW)+SLOW
        df['COF'] = df['SMOOTH'] * df['SMOOTH'] # COF=SMOOTH*SMOOTH
        # KAMA=COF*CLOSE+(1-COF)*REF(KAMA,1)
        df['KAMA'] = df['COF'] * df['close'] + (1- df['COF'])
        df['KAMA'] = df['COF'] * df['close'] + (1- df['COF']) + df['KAMA'].shift(1)
        # 进行归一化
        df['KAMA_min'] = df['KAMA'].rolling(n, min_periods=1).min()
        df['KAMA_max'] = df['KAMA'].rolling(n, min_periods=1).max()
        df['KAMA_norm'] = (df['KAMA'] - df['KAMA_min']) / (df['KAMA_max'] - df['KAMA_min'])

        df['前%dhKAMA' % n] = df['KAMA_norm'].shift(1)
        extra_agg_dict['前%dhKAMA' % n] = 'first'
        # 删除中间过渡数据
        del df['DIRECTION']
        del df['abs_ref']
        del df['VOLATILITY']
        del df['ER']
        del df['SMOOTH']
        del df['COF']
        del df['KAMA']
        del df['KAMA_min']
        del df['KAMA_max']
        del df['KAMA_norm']


    # PPO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        N3=9
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        PPO_SIGNAL=EMA(PPO,N3)
        PPO 是 MACD 的变化率版本。
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)，而
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)。
        PPO 上穿/下穿 PPO_SIGNAL 产生买入/卖出信号。
        """
        #
        N3 = n
        N1 = int(n * 1.382) # 黄金分割线
        N2 = 3 * n
        df['ema_1'] = df['close'].ewm(N1, adjust=False).mean() # EMA(CLOSE,N1)
        df['ema_2'] = df['close'].ewm(N2, adjust=False).mean() # EMA(CLOSE,N2)
        df['PPO'] = (df['ema_1'] - df['ema_2']) / df['ema_2'] # PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        df['PPO_SIGNAL'] = df['PPO'].ewm(N3, adjust=False).mean() # PPO_SIGNAL=EMA(PPO,N3)

        df['前%dhPPO' % n] = df['PPO_SIGNAL'].shift(1)
        extra_agg_dict['前%dhPPO' % n] = 'first'
        # 删除中间数据
        del df['ema_1']
        del df['ema_2']
        del df['PPO']
        del df['PPO_SIGNAL']

    # SMI 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        N3=20
        M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        D=CLOSE-M
        DS=EMA(EMA(D,N2),N2)
        DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        SMI=100*DS/DHL
        SMIMA=MA(SMI,N3)
        SMI 指标可以看作 KDJ 指标的变形。不同的是，KD 指标衡量的是当
        天收盘价位于最近 N 天的最高价和最低价之间的位置，而 SMI 指标
        是衡量当天收盘价与最近 N 天的最高价与最低价均值之间的距离。我
        们用 SMI 指标上穿/下穿其均线产生买入/卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N1)
        df['min_low'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N1)
        df['M'] = (df['max_high'] + df['min_low']) / 2 # M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['D'] = df['close'] - df['M'] # D=CLOSE-M
        df['ema'] = df['D'].ewm(n, adjust=False).mean() # EMA(D,N2)
        df['DS'] = df['ema'].ewm(n, adjust=False).mean() # DS=EMA(EMA(D,N2),N2)
        df['HL'] = df['max_high'] - df['min_low'] # MAX(HIGH,N1) - MIN(LOW,N1)
        df['ema_hl'] = df['HL'].ewm(n, adjust=False).mean() # EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2)
        df['DHL'] = df['ema_hl'].ewm(n, adjust=False).mean() # DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        df['SMI'] = 100 * df['DS'] / df['DHL'] #  SMI=100*DS/DHL
        df['SMIMA'] = df['SMI'].rolling(n, min_periods=1).mean() # SMIMA=MA(SMI,N3)

        df['前%dhSMI' % n] = df['SMIMA'].shift(1)
        extra_agg_dict['前%dhSMI' % n] = 'first'
        # 删除中间数据
        del df['max_high']
        del df['min_low']
        del df['M']
        del df['D']
        del df['ema']
        del df['DS']
        del df['HL']
        del df['ema_hl']
        del df['DHL']
        del df['SMI']
        del df['SMIMA']

    # ARBR指标
    for n in back_hour_list:
        """
        AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100
        AR 衡量开盘价在最高价、最低价之间的位置；BR 衡量昨日收盘价在
        今日最高价、最低价之间的位置。AR 为人气指标，用来计算多空双
        方的力量对比。当 AR 值偏低（低于 50）时表示人气非常低迷，股价
        很低，若从 50 下方上穿 50，则说明股价未来可能要上升，低点买入。
        当 AR 值下穿 200 时卖出。
        """
        df['HO'] = df['high'] - df['open'] # (HIGH-OPEN)
        df['OL'] = df['open'] - df['low'] # (OPEN-LOW)
        df['AR'] = df['HO'].rolling(n).sum() / df['OL'].rolling(n).sum() * 100 # AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        df['HC'] = df['high'] - df['close'].shift(1) # (HIGH-REF(CLOSE,1))
        df['CL'] = df['close'].shift(1) - df['low'] # (REF(CLOSE,1)-LOW)
        df['BR'] = df['HC'].rolling(n).sum() / df['CL'].rolling(n).sum() * 100 # BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100

        df['前%dhARBR_AR' % n] = df['AR'].shift(1)
        df['前%dhARBR_BR' % n] = df['BR'].shift(1)
        extra_agg_dict['前%dhARBR_AR' % n] = 'first'
        extra_agg_dict['前%dhARBR_BR' % n] = 'first'
        # 删除中间数据
        del df['HO']
        del df['OL']
        del df['AR']
        del df['HC']
        del df['CL']
        del df['BR']

    # DO 指标
    for n in back_hour_list:
        """
        DO=EMA(EMA(RSI,N),M)
        DO 是平滑处理（双重移动平均）后的 RSI 指标。DO 大于 0 则说明
        市场处于上涨趋势，小于 0 说明市场处于下跌趋势。我们用 DO 上穿
        /下穿其移动平均线来产生买入/卖出信号。
        """
        # 计算RSI
        # 以下为基础策略分享会代码
        # diff = df['close'].diff()
        # df['up'] = np.where(diff > 0, diff, 0)
        # df['down'] = np.where(diff < 0, abs(diff), 0)
        # A = df['up'].rolling(n).sum()
        # B = df['down'].rolling(n).sum()
        # df['rsi'] = A / (A + B)
        diff = df['close'].diff() # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0) # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff), 0) # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()# SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean() # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df['rsi'] = A / (A + B)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df['ema_rsi'] = df['rsi'].ewm(n, adjust=False).mean() # EMA(RSI,N)
        df['DO'] = df['ema_rsi'].ewm(n, adjust=False).mean() # DO=EMA(EMA(RSI,N),M)
        df['前%dhDO' % n] = df['DO'].shift(1)
        extra_agg_dict['前%dhDO' % n] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']
        del df['rsi']
        del df['ema_rsi']
        del df['DO']

    # SI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+
        0.5*(CLOSE-OPEN))/R*K/M
        SI 用价格变化（即两天收盘价之差，昨日收盘与开盘价之差，今日收
        盘与开盘价之差）的加权平均来反映价格的变化。如果 SI 上穿/下穿
        0 则产生买入/卖出信号。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1)) # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1))# B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1)) # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1)) #  D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1) # K=MAX(A,B)
        df['M'] = (df['high'] - df['low']).rolling(n).max() # M=MAX(HIGH-LOW,N)
        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D'] # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D'] #  R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D'] # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'], df['R2']) # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] >= df['A']) & (df['C'] >= df['B']), df['R3'], df['R4']) # R=IF((C>=A) & (C>=B),R3,R4)
        # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / df['R'] * df['K'] / df['M']
        df['前%dhSI' % n] = df['SI'].shift(1)
        extra_agg_dict['前%dhSI' % n] = 'first'
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['M']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']

    # DBCD 指标
    for n in back_hour_list:
        """
        N=5
        M=16
        T=17
        BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        BIAS_DIF=BIAS-REF(BIAS,M)
        DBCD=SMA(BIAS_DIFF,T,1)
        DBCD（异同离差乖离率）为乖离率离差的移动平均。我们用 DBCD
        上穿 5%/下穿-5%来产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(n, min_periods=1).mean() # MA(CLOSE,N)

        df['BIAS'] = (df['close'] - df['ma']) / df['ma'] * 100 # BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        df['BIAS_DIF'] = df['BIAS'] - df['BIAS'].shift(3 * n) # BIAS_DIF=BIAS-REF(BIAS,M)
        # df['DBCD'] = df['BIAS_DIF'].rolling(3 * n + 2, min_periods=1).mean()
        df['DBCD'] = df['BIAS_DIF'].ewm(span=3 * n).mean() # DBCD=SMA(BIAS_DIFF,T,1)
        df['前%dhDBCD' % n] = df['DBCD'].shift(1)
        extra_agg_dict['前%dhDBCD' % n] = 'first'
        # 删除中间数据
        del df['ma']
        del df['BIAS']
        del df['BIAS_DIF']
        del df['DBCD']

    # CV 指标
    for n in back_hour_list:
        """
        N=10
        H_L_EMA=EMA(HIGH-LOW,N)
        CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
        CV 指标用来衡量股价的波动，反映一段时间内最高价与最低价之差
        （价格变化幅度）的变化率。如果 CV 的绝对值下穿 30，买入；
        如果 CV 的绝对值上穿 70，卖出。
        """
        df['H_L_ema'] = (df['high'] - df['low']).ewm(n, adjust=False).mean() # H_L_EMA=EMA(HIGH-LOW,N)
        df['CV'] = (df['H_L_ema'] - df['H_L_ema'].shift(n)) / df['H_L_ema'].shift(n) * 100 # CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
        df['前%dhCV' % n] = df['CV'].shift(1)
        extra_agg_dict['前%dhCV' % n] = 'first'
        # 删除中间数据
        del df['H_L_ema']
        del df['CV']

    # RMI 指标
    for n in back_hour_list:
        """
        N=7
        RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        RMI 与 RSI 的计算方式类似，将 RSI 中的动量与前一天价格之差
        CLOSE-REF(CLOSE,1)项改为了与前四天价格之差 CLOSEREF(CLOSE,4)
        """
        # MAX(CLOSE-REF(CLOSE,4),0)
        df['max_close'] = np.where(df['close'] > df['close'].shift(4), df['close'] - df['close'].shift(4), 0)
        # ABS(CLOSE-REF(CLOSE,1)
        df['abs_close'] = df['close'] - df['close'].shift(1)

        # df['sma_1'] = df['max_close'].rolling(n, min_periods=1).mean()
        df['sma_1'] = df['max_close'].ewm(span=n).mean() # SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)
        # df['sma_2'] = df['abs_close'].rolling(n, min_periods=1).mean()
        df['sma_2'] = df['abs_close'].ewm(span=n).mean() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['RMI'] = df['sma_1'] / df['sma_2'] * 100 #  RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        df['前%dhRMI' % n] = df['RMI'].shift(1)
        extra_agg_dict['前%dhRMI' % n] = 'first'
        # 删除中间数据
        del df['max_close']
        del df['abs_close']
        del df['sma_1']
        del df['sma_2']
        del df['RMI']

    # SKDJ 指标
    for n in back_hour_list:
        """
        N=60
        M=5
        RSV=(CLOSE-MIN(LOW,N))/(MAX(HIGH,N)-MIN(LOW,N))*100
        MARSV=SMA(RSV,3,1)
        K=SMA(MARSV,3,1)
        D=MA(K,3)
        SKDJ 为慢速随机波动（即慢速 KDJ）。SKDJ 中的 K 即 KDJ 中的 D，
        SKJ 中的 D 即 KDJ 中的 D 取移动平均。其用法与 KDJ 相同。
        当 D<40(处于超卖状态)且 K 上穿 D 时买入，当 D>60（处于超买状
        态）K 下穿 D 时卖出。
        """
        # RSV=(CLOSE-MIN(LOW,N))/(MAX(HIGH,N)-MIN(LOW,N))*100
        df['RSV'] = (df['close'] - df['low'].rolling(n, min_periods=1).min()) / (
                df['high'].rolling(n, min_periods=1).max() - df['low'].rolling(n, min_periods=1).min()) * 100
        # MARSV=SMA(RSV,3,1)
        df['MARSV'] = df['RSV'].ewm(com=2).mean()
        # K=SMA(MARSV,3,1)
        df['K'] = df['MARSV'].ewm(com=2).mean()
        # D=MA(K,3)
        df['D'] = df['K'].rolling(3, min_periods=1).mean()
        df['前%dhSKDJ' % n] = df['D'].shift(1)
        extra_agg_dict['前%dhSKDJ' % n] = 'first'
        # 删除中间过渡数据
        del df['RSV']
        del df['MARSV']
        del df['K']
        del df['D']

    # ROC 指标
    for n in back_hour_list:
        """
        ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
        ROC 衡量价格的涨跌幅。ROC 可以用来反映市场的超买超卖状态。
        当 ROC 过高时，市场处于超买状态；当 ROC 过低时，市场处于超
        卖状态。这些情况下，可能会发生反转。
        如果 ROC 上穿 5%，则产生买入信号；
        如果 ROC 下穿-5%，则产生卖出信号。
        """
        # ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
        df['ROC'] = df['close'] / df['close'].shift(n) - 1

        df['前%dhROC' % n] = df['ROC'].shift(1)
        extra_agg_dict['前%dhROC' % n] = 'first'
        del df['ROC']

    # WR 指标
    for n in back_hour_list:
        """
        HIGH(N)=MAX(HIGH,N)
        LOW(N)=MIN(LOW,N)
        WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
        WR 指标事实上就是 100-KDJ 指标计算过程中的 Stochastics。WR
        指标用来衡量市场的强弱和超买超卖状态。一般认为，当 WR 小于
        20 时，市场处于超买状态；当 WR 大于 80 时，市场处于超卖状态；
        当 WR 处于 20 到 80 之间时，多空较为平衡。
        如果 WR 上穿 80，则产生买入信号；
        如果 WR 下穿 20，则产生卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).max() # HIGH(N)=MAX(HIGH,N)
        df['min_low'] = df['low'].rolling(n, min_periods=1).min() # LOW(N)=MIN(LOW,N)
        # WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
        df['WR'] = (df['max_high'] - df['close']) / (df['max_high'] - df['min_low']) * 100
        df['前%dhWR' % n] = df['WR'].shift(1)
        extra_agg_dict['前%dhWR' % n] = 'first'
        # 删除中间过渡数据
        del df['max_high']
        del df['min_low']
        del df['WR']

    # STC 指标
    for n in back_hour_list:
        """
        N1=23
        N2=50
        N=40
        MACDX=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        V1=MIN(MACDX,N)
        V2=MAX(MACDX,N)-V1
        FK=IF(V2>0,(MACDX-V1)/V2*100,REF(FK,1))
        FD=SMA(FK,N,1)
        V3=MIN(FD,N)
        V4=MAX(FD,N)-V3
        SK=IF(V4>0,(FD-V3)/V4*100,REF(SK,1))
        STC=SD=SMA(SK,N,1) 
        STC 指标结合了 MACD 指标和 KDJ 指标的算法。首先用短期均线与
        长期均线之差算出 MACD，再求 MACD 的随机快速随机指标 FK 和
        FD，最后求 MACD 的慢速随机指标 SK 和 SD。其中慢速随机指标就
        是 STC 指标。STC 指标可以用来反映市场的超买超卖状态。一般认
        为 STC 指标超过 75 为超买，STC 指标低于 25 为超卖。
        如果 STC 上穿 25，则产生买入信号；
        如果 STC 下穿 75，则产生卖出信号。
        """
        N1 = n
        N2 = int(N1 * 1.5)  # 大约值
        N = 2 * n
        df['ema1'] = df['close'].ewm(N1, adjust=False).mean() # EMA(CLOSE,N1)
        df['ema2'] = df['close'].ewm(N, adjust=False).mean() # EMA(CLOSE,N2)
        df['MACDX'] = df['ema1'] - df['ema2'] # MACDX=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['V1'] = df['MACDX'].rolling(N2, min_periods=1).min() # V1=MIN(MACDX,N)
        df['V2'] = df['MACDX'].rolling(N2, min_periods=1).max() - df['V1'] # V2=MAX(MACDX,N)-V1
        # FK=IF(V2>0,(MACDX-V1)/V2*100,REF(FK,1))
        df['FK'] = (df['MACDX'] - df['V1']) / df['V2'] * 100
        df['FK'] = np.where(df['V2'] > 0, (df['MACDX'] - df['V1']) / df['V2'] * 100, df['FK'].shift(1))

        df['FD'] = df['FK'].rolling(N2, min_periods=1).mean()# FD=SMA(FK,N,1)  直接使用均线代替sma
        df['V3'] = df['FD'].rolling(N2, min_periods=1).min() # V3=MIN(FD,N)
        df['V4'] = df['FD'].rolling(N2, min_periods=1).max() - df['V3'] # V4=MAX(FD,N)-V3
        # SK=IF(V4>0,(FD-V3)/V4*100,REF(SK,1))
        df['SK'] = (df['FD'] - df['V3']) / df['V4'] * 100
        df['SK'] = np.where(df['V4'] > 0, (df['FD'] - df['V3']) / df['V4'] * 100, df['SK'].shift(1))
        # STC = SD = SMA(SK, N, 1)
        df['STC'] = df['SK'].rolling(N1, min_periods=1).mean()
        df['前%dhSTC' % n] = df['STC'].shift(1)
        extra_agg_dict['前%dhSTC' % n] = 'first'
        # 删除中间过渡数据
        del df['ema1']
        del df['ema2']
        del df['MACDX']
        del df['V1']
        del df['V2']
        del df['V3']
        del df['V4']
        del df['FK']
        del df['FD']
        del df['SK']
        del df['STC']

    # RVI 指标
    for n in back_hour_list:
        """
        N1=10
        N2=20
        STD=STD(CLOSE,N)
        USTD=SUM(IF(CLOSE>REF(CLOSE,1),STD,0),N2)
        DSTD=SUM(IF(CLOSE<REF(CLOSE,1),STD,0),N2)
        RVI=100*USTD/(USTD+DSTD)
        RVI 的计算方式与 RSI 一样，不同的是将 RSI 计算中的收盘价变化值
        替换为收盘价在过去一段时间的标准差，用来反映一段时间内上升
        的波动率和下降的波动率的对比。我们也可以像计算 RSI 指标时一样
        先对公式中的 USTD 和 DSTD 作移动平均得到 USTD_MA 和
        DSTD_MA 再求出 RVI=100*USTD_MV/(USTD_MV+DSTD_MV)。
        RVI 的用法与 RSI 一样。通常认为当 RVI 大于 70，市场处于强势上
        涨甚至达到超买的状态；当 RVI 小于 30，市场处于强势下跌甚至达
        到超卖的状态。当 RVI 跌到 30 以下又上穿 30 时，通常认为股价要
        从超卖的状态反弹；当 RVI 超过 70 又下穿 70 时，通常认为市场要
        从超买的状态回落了。
        如果 RVI 上穿 30，则产生买入信号；
        如果 RVI 下穿 70，则产生卖出信号。
        """
        df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0) # STD=STD(CLOSE,N)
        df['ustd'] = np.where(df['close'] > df['close'].shift(1), df['std'], 0) # IF(CLOSE>REF(CLOSE,1),STD,0)
        df['sum_ustd'] = df['ustd'].rolling(2 * n).sum() #  USTD=SUM(IF(CLOSE>REF(CLOSE,1),STD,0),N2)

        df['dstd'] = np.where(df['close'] < df['close'].shift(1), df['std'], 0) # IF(CLOSE<REF(CLOSE,1),STD,0)
        df['sum_dstd'] = df['dstd'].rolling(2 * n).sum() # DSTD=SUM(IF(CLOSE<REF(CLOSE,1),STD,0),N2)

        df['RVI'] = df['sum_ustd'] / (df['sum_ustd'] + df['sum_dstd']) * 100 # RVI=100*USTD/(USTD+DSTD)
        df['前%dhRVI' % n] = df['RVI'].shift(1)
        extra_agg_dict['前%dhRVI' % n] = 'first'
        # 删除中间过渡数据
        del df['std']
        del df['ustd']
        del df['sum_ustd']
        del df['dstd']
        del df['sum_dstd']
        del df['RVI']

    # UOS 指标
    for n in back_hour_list:
        """
        M=7
        N=14
        O=28
        TH=MAX(HIGH,REF(CLOSE,1))
        TL=MIN(LOW,REF(CLOSE,1))
        TR=TH-TL
        XR=CLOSE-TL
        XRM=SUM(XR,M)/SUM(TR,M)
        XRN=SUM(XR,N)/SUM(TR,N)
        XRO=SUM(XR,O)/SUM(TR,O)
        UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
        UOS 的用法与 RSI 指标类似，可以用来反映市场的超买超卖状态。
        一般来说，UOS 低于 30 市场处于超卖状态；UOS 高于 30 市场处于
        超买状态。
        如果 UOS 上穿 30，则产生买入信号；
        如果 UOS 下穿 70，则产生卖出信号。
        """
        # 固定多参数比例倍数
        M = n
        N = 2 * n
        O = 4 * n
        df['ref_close'] = df['close'].shift(1) # REF(CLOSE,1)
        df['TH'] = df[['high', 'ref_close']].max(axis=1) #  TH=MAX(HIGH,REF(CLOSE,1))
        df['TL'] = df[['low', 'ref_close']].min(axis=1) # TL=MIN(LOW,REF(CLOSE,1))
        df['TR'] = df['TH'] - df['TL']  # TR=TH-TL
        df['XR'] = df['close'] - df['TL'] # XR=CLOSE-TL
        df['XRM'] = df['XR'].rolling(M).sum() / df['TR'].rolling(M).sum() # XRM=SUM(XR,M)/SUM(TR,M)
        df['XRN'] = df['XR'].rolling(N).sum() / df['TR'].rolling(N).sum() # XRN=SUM(XR,N)/SUM(TR,N)
        df['XRO'] = df['XR'].rolling(O).sum() / df['TR'].rolling(O).sum() # XRO=SUM(XR,O)/SUM(TR,O)
        # UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
        df['UOS'] = 100 * (df['XRM'] * N * O + df['XRN'] * M * O + df['XRO'] * M * N) / (M * N + M * O + N * O)
        df['前%dhUOS' % n] = df['UOS'].shift(1)
        extra_agg_dict['前%dhUOS' % n] = 'first'
        # 删除中间过渡数据
        del df['ref_close']
        del df['TH']
        del df['TL']
        del df['TR']
        del df['XR']
        del df['XRM']
        del df['XRN']
        del df['XRO']
        del df['UOS']

    # RSIS 指标
    for n in back_hour_list:
        """
        N=120
        M=20
        CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
        OSE,1),0)
        RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOS
        E,1)),N,1)*100
        RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
        RSISMA=EMA(RSIS,M)
        RSIS 反映当前 RSI 在最近 N 天的 RSI 最大值和最小值之间的位置，
        与 KDJ 指标的构造思想类似。由于 RSIS 波动性比较大，我们先取移
        动平均再用其产生信号。其用法与 RSI 指标的用法类似。
        RSISMA 上穿 40 则产生买入信号；
        RSISMA 下穿 60 则产生卖出信号。
        """
        N = 6 * n
        M = n
        # CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
        # df['sma_1'] = df['close_diff_pos'].rolling(N).sum() # SMA(CLOSE_DIFF_POS,N,1)
        df['sma_1'] = df['close_diff_pos'].ewm(span=N).mean() # SMA(CLOSE_DIFF_POS,N,1)
        # df['sma_2'] = abs(df['close'] - df['close'].shift(1)).rolling(N).sum() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['sma_2'] = abs(df['close'] - df['close'].shift(1)).ewm(span=N).mean() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['RSI'] = df['sma_1'] / df['sma_2'] * 100 # RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        # RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
        df['RSIS'] = (df['RSI'] - df['RSI'].rolling(N, min_periods=1).min()) / (
                df['RSI'].rolling(N, min_periods=1).max() - df['RSI'].rolling(N, min_periods=1).min()) * 100
        # RSISMA=EMA(RSIS,M)
        df['RSISMA'] = df['RSIS'].ewm(M, adjust=False).mean()

        df['前%dhRSISMA' % n] = df['RSISMA'].shift(1)
        extra_agg_dict['前%dhRSISMA' % n] = 'first'

        del df['close_diff_pos']
        del df['sma_1']
        del df['sma_2']
        del df['RSI']
        del df['RSIS']
        del df['RSISMA']

    # MAAMT 指标
    for n in back_hour_list:
        """
        N=40
        MAAMT=MA(AMOUNT,N)
        MAAMT 是成交额的移动平均线。当成交额上穿/下穿移动平均线时产
        生买入/卖出信号。
        """
        df['MAAMT'] = df['volume'].rolling(n, min_periods=1).mean() #MAAMT=MA(AMOUNT,N)
        df['前%dhMAAMT' % n] = df['MAAMT'].shift(1)
        extra_agg_dict['前%dhMAAMT' % n] = 'first'
        del df['MAAMT']

    # SROCVOL 指标
    for n in back_hour_list:
        """
        N=20
        M=10
        EMAP=EMA(VOLUME,N)
        SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROCVOL 与 ROCVOL 类似，但是会先对成交量进行移动平均平滑
        处理之后再取其变化率。（SROCVOL 是 SROC 的成交量版本。）
        SROCVOL 上穿 0 买入，下穿 0 卖出。
        """
        df['emap'] = df['volume'].ewm(2 * n, adjust=False).mean() # EMAP=EMA(VOLUME,N)
        # SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df['SROCVOL'] = (df['emap'] - df['emap'].shift(n)) / df['emap'].shift(n)
        df['前%dhSROCVOL' % n] = df['SROCVOL'].shift(1)
        extra_agg_dict['前%dhSROCVOL' % n] = 'first'
        del df['emap']
        del df['SROCVOL']

    # PVO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
        PVO 用成交量的指数移动平均来反应成交量的变化。PVO 上穿 0 线
        买入；PVO 下穿 0 线卖出。
        """
        df['emap_1'] = df['volume'].ewm(n, min_periods=1).mean() # EMA(VOLUME,N1)
        df['emap_2'] = df['volume'].ewm(n * 2, min_periods=1).mean() # EMA(VOLUME,N2)
        df['PVO'] = (df['emap_1'] - df['emap_2']) / df['emap_2'] # PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
        df['前%dhPVO' % n] = df['PVO'].shift(1)
        extra_agg_dict['前%dhPVO' % n] = 'first'
        # 删除中间过渡数据
        del df['emap_1']
        del df['emap_2']
        del df['PVO']

    # BIASVOL 指标
    for n in back_hour_list:
        """
        N=6，12，24
        BIASVOL(N)=(VOLUME-MA(VOLUME,N))/MA(VOLUME,N)
        BIASVOL 是乖离率 BIAS 指标的成交量版本。如果 BIASVOL6 大于
        5 且 BIASVOL12 大于 7 且 BIASVOL24 大于 11，则产生买入信号；
        如果 BIASVOL6 小于-5 且 BIASVOL12 小于-7 且 BIASVOL24 小于
        -11，则产生卖出信号。
        """
        df['ma_volume'] = df['volume'].rolling(n, min_periods=1).mean() # MA(VOLUME,N)
        df['BIASVOL'] = (df['volume'] - df['ma_volume']) / df['ma_volume'] # BIASVOL(N)=(VOLUME-MA(VOLUME,N))/MA(VOLUME,N)
        df['前%dhBIASVOL' % n] = df['BIASVOL'].shift(1)
        extra_agg_dict['前%dhBIASVOL' % n] = 'first'
        del df['ma_volume']
        del df['BIASVOL']

    # MACDVOL 指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=10
        MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
        SIGNAL=MA(MACDVOL,N3)
        MACDVOL 是 MACD 的成交量版本。如果 MACDVOL 上穿 SIGNAL，
        则买入；下穿 SIGNAL 则卖出。
        """
        N1 = 2 * n
        N2 = 4 * n
        N3 = n
        df['ema_volume_1'] = df['volume'].ewm(N1, adjust=False).mean() # EMA(VOLUME,N1)
        df['ema_volume_2'] = df['volume'].ewm(N2, adjust=False).mean() # EMA(VOLUME,N2)
        df['MACDV'] = df['ema_volume_1'] - df['ema_volume_2'] # MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
        df['SIGNAL'] = df['MACDV'].rolling(N3, min_periods=1).mean() # SIGNAL=MA(MACDVOL,N3)
        # 去量纲
        df['MACDVOL'] = df['MACDV'] / df['SIGNAL'] - 1
        df['前%dhMACDVOL' % n] = df['MACDVOL'].shift(1)
        extra_agg_dict['前%dhMACDVOL' % n] = 'first'
        # 删除中间过程数据
        del df['ema_volume_1']
        del df['ema_volume_2']
        del df['MACDV']
        del df['SIGNAL']
        del df['MACDVOL']

    # ROCVOL 指标
    for n in back_hour_list:
        """
        N = 80
        ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)
        ROCVOL 是 ROC 的成交量版本。如果 ROCVOL 上穿 0 则买入，下
        穿 0 则卖出。
        """
        df['ROCVOL'] = df['volume'] / df['volume'].shift(n) - 1 # ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)

        df['前%dhROCVOL' % n] = df['ROCVOL'].shift(1)
        extra_agg_dict['前%dhROCVOL' % n] = 'first'

    # FI 指标
    for n in back_hour_list:
        """
        N=13
        FI=(CLOSE-REF(CLOSE,1))*VOLUME
        FIMA=EMA(FI,N)
        FI 用价格的变化来衡量价格的趋势，用成交量大小来衡量趋势的强
        弱。我们先对 FI 取移动平均，当均线上穿 0 线时产生买入信号，下
        穿 0 线时产生卖出信号。
        """
        df['FI'] = (df['close'] - df['close'].shift(1)) * df['volume'] # FI=(CLOSE-REF(CLOSE,1))*VOLUME
        df['FIMA'] = df['FI'].ewm(n, adjust=False).mean() # FIMA=EMA(FI,N)
        # 去量纲
        df['前%dhFI' % n] = df['FI'] / df['FIMA'] - 1
        df['前%dhFI' % n] = df['前%dhFI' % n].shift(1)
        extra_agg_dict['前%dhFI' % n] = 'first'
        # 删除中间过程数据
        del df['FI']
        del df['FIMA']

    # PVT 指标
    for n in back_hour_list:
        """
        PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
        PVT_MA1=MA(PVT,N1)
        PVT_MA2=MA(PVT,N2)
        PVT 指标用价格的变化率作为权重求成交量的移动平均。PVT 指标
        与 OBV 指标的思想类似，但与 OBV 指标相比，PVT 考虑了价格不
        同涨跌幅的影响，而 OBV 只考虑了价格的变化方向。我们这里用 PVT
        短期和长期均线的交叉来产生交易信号。
        如果 PVT_MA1 上穿 PVT_MA2，则产生买入信号；
        如果 PVT_MA1 下穿 PVT_MA2，则产生卖出信号。
        """
        # PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
        df['PVT'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * df['volume']
        df['PVT_MA1'] = df['PVT'].rolling(n, min_periods=1).mean() # PVT_MA1=MA(PVT,N1)
        # df['PVT_MA2'] = df['PVT'].rolling(2 * n, min_periods=1).mean()

        # 去量纲  只引入一个ma做因子
        df['前%dhPVT' % n] = df['PVT'] / df['PVT_MA1'] - 1
        df['前%dhPVT' % n] = df['前%dhPVT' % n].shift(1)
        extra_agg_dict['前%dhPVT' % n] = 'first'
        # 删除中间过程数据
        del df['PVT']
        del df['PVT_MA1']

    # RSIV 指标
    for n in back_hour_list:
        """
        N=20
        VOLUP=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        VOLDOWN=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        SUMUP=SUM(VOLUP,N)
        SUMDOWN=SUM(VOLDOWN,N)
        RSIV=100*SUMUP/(SUMUP+SUMDOWN)
        RSIV 的计算方式与 RSI 相同，只是把其中的价格变化 CLOSEREF(CLOSE,1)替换成了成交量 VOLUME。用法与 RSI 类似。我们
        这里将其用作动量指标，上穿 60 买入，下穿 40 卖出。
        """
        df['VOLUP'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0) # VOLUP=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        df['VOLDOWN'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0) #  VOLDOWN=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['SUMUP'] = df['VOLUP'].rolling(n).sum() # SUMUP=SUM(VOLUP,N)
        df['SUMDOWN'] = df['VOLDOWN'].rolling(n).sum() # SUMDOWN=SUM(VOLDOWN,N)
        df['RSIV'] = df['SUMUP'] / (df['SUMUP'] + df['SUMDOWN']) * 100 # RSIV=100*SUMUP/(SUMUP+SUMDOWN)

        df['前%dhRSIV' % n] = df['RSIV'].shift(1)
        extra_agg_dict['前%dhRSIV' % n] = 'first'
        # 删除中间过渡数据
        del df['VOLUP']
        del df['VOLDOWN']
        del df['SUMUP']
        del df['SUMDOWN']
        del df['RSIV']

    # AMV 指标
    for n in back_hour_list:
        """
        N1=13
        N2=34
        AMOV=VOLUME*(OPEN+CLOSE)/2
        AMV1=SUM(AMOV,N1)/SUM(VOLUME,N1)
        AMV2=SUM(AMOV,N2)/SUM(VOLUME,N2)
        AMV 指标用成交量作为权重对开盘价和收盘价的均值进行加权移动
        平均。成交量越大的价格对移动平均结果的影响越大，AMV 指标减
        小了成交量小的价格波动的影响。当短期 AMV 线上穿/下穿长期 AMV
        线时，产生买入/卖出信号。
        """
        df['AMOV'] = df['volume'] * (df['open'] + df['close']) / 2 # AMOV=VOLUME*(OPEN+CLOSE)/2
        df['AMV1'] = df['AMOV'].rolling(n).sum() / df['volume'].rolling(n).sum() # AMV1=SUM(AMOV,N1)/SUM(VOLUME,N1)
        # df['AMV2'] = df['AMOV'].rolling(n * 3).sum() / df['volume'].rolling(n * 3).sum()
        # 去量纲
        df['AMV'] = (df['AMV1'] - df['AMV1'].rolling(n).min()) / (
                df['AMV1'].rolling(n).max() - df['AMV1'].rolling(n).min())  # 标准化
        df['前%dhAMV' % n] = df['AMV'].shift(1)
        extra_agg_dict['前%dhAMV' % n] = 'first'
        # 删除中间过程数据
        del df['AMOV']
        del df['AMV1']
        del df['AMV']

    # VRAMT 指标
    for n in back_hour_list:
        """
        N=40
        AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
        BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
        CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
        AVS=SUM(AV,N)
        BVS=SUM(BV,N)
        CVS=SUM(CV,N)
        VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
        VRAMT 的计算与 VR 指标（Volume Ratio）一样，只是把其中的成
        交量替换成了成交额。
        如果 VRAMT 上穿 180，则产生买入信号；
        如果 VRAMT 下穿 70，则产生卖出信号。
        """
        df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0) # AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
        df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0) # BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
        df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0) # CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
        df['AVS'] = df['AV'].rolling(n).sum() # AVS=SUM(AV,N)
        df['BVS'] = df['BV'].rolling(n).sum() # BVS=SUM(BV,N)
        df['CVS'] = df['CV'].rolling(n).sum() # CVS=SUM(CV,N)
        df['VRAMT'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2) # VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
        df['前%dhVRAMT' % n] = df['VRAMT'].shift(1)
        extra_agg_dict['前%dhVRAMT' % n] = 'first'
        # 删除中间过程数据
        del df['AV']
        del df['BV']
        del df['CV']
        del df['AVS']
        del df['BVS']
        del df['CVS']
        del df['VRAMT']

    # WVAD 指标
    for n in back_hour_list:
        """
        N=20
        WVAD=SUM(((CLOSE-OPEN)/(HIGH-LOW)*VOLUME),N)
        WVAD 是用价格信息对成交量加权的价量指标，用来比较开盘到收盘
        期间多空双方的力量。WVAD 的构造与 CMF 类似，但是 CMF 的权
        值用的是 CLV(反映收盘价在最高价、最低价之间的位置)，而 WVAD
        用的是收盘价与开盘价的距离（即蜡烛图的实体部分的长度）占最高
        价与最低价的距离的比例，且没有再除以成交量之和。
        WVAD 上穿 0 线，代表买方力量强；
        WVAD 下穿 0 线，代表卖方力量强。
        """
        # ((CLOSE-OPEN)/(HIGH-LOW)*VOLUME)
        df['VAD'] = (df['close'] - df['open']) / (df['high'] - df['low']) * df['volume']
        df['WVAD'] = df['VAD'].rolling(n).sum() # WVAD=SUM(((CLOSE-OPEN)/(HIGH-LOW)*VOLUME),N)

        # 标准化
        df['前%dhWVAD' % n] = (df['WVAD'] - df['WVAD'].rolling(n).min()) / (
                df['WVAD'].rolling(n).max() - df['WVAD'].rolling(n).min())
        df['前%dhWVAD' % n] = df['前%dhWVAD' % n].shift(1)
        extra_agg_dict['前%dhWVAD' % n] = 'first'
        del df['VAD']
        del df['WVAD']

    # OBV 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
        VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
        OBV=REF(OBV,1)+VOL
        OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
        OBV 指标把成交量分为正的成交量（价格上升时的成交量）和负的
        成交量（价格下降时）的成交量。OBV 就是分了正负之后的成交量
        的累计和。如果 OBV 和价格的均线一起上涨（下跌），则上涨（下
        跌）趋势被确认。如果 OBV 上升（下降）而价格的均线下降（上升），
        说明价格可能要反转，可能要开始新的下跌（上涨）行情。
        如果 OBV_HISTOGRAM 上穿 0 则买入，下穿 0 则卖出。
        """
        # VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
        df['VOL'] = np.where(df['close'] > df['close'].shift(1), df['volume'], -df['volume'])
        # VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
        df['VOL'] = np.where(df['close'] != df['close'].shift(1), df['VOL'], 0)
        # OBV=REF(OBV,1)+VOL
        df['OBV'] = df['VOL']
        df['OBV'] = df['VOL'] + df['OBV'].shift(1)
        # OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
        df['OBV_HISTOGRAM'] = df['OBV'].ewm(n, adjust=False).mean() - df['OBV'].ewm(3 * n, adjust=False).mean()
        df['前%dhOBV_HISTOGRAM' % n] = df['OBV_HISTOGRAM'].shift(1)
        extra_agg_dict['前%dhOBV_HISTOGRAM' % n] = 'first'
        # 删除中间过程数据
        del df['VOL']
        del df['OBV']
        del df['OBV_HISTOGRAM']

    # CMF 指标
    for n in back_hour_list:
        """
        N=60
        CMF=SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW),N)/SUM(VOLUME,N)
        CMF 用 CLV 对成交量进行加权，如果收盘价在高低价的中点之上，
        则为正的成交量（买方力量占优势）；若收盘价在高低价的中点之下，
        则为负的成交量（卖方力量占优势）。
        如果 CMF 上穿 0，则产生买入信号；
        如果 CMF 下穿 0，则产生卖出信号。
        """
        # ((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW)
        A = ((df['close'] - df['low']) - (df['high'] - df['close'])) * df['volume'] / (df['high'] - df['low'])
        # CMF=SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW),N)/SUM(VOLUME,N)
        df['CMF'] = A.rolling(n).sum() / df['volume'].rolling(n).sum()

        df['前%dhCMF' % n] = df['CMF'].shift(1)
        extra_agg_dict['前%dhCMF' % n] = 'first'
        del df['CMF']

    # PVI 指标
    for n in back_hour_list:
        """
        N=40
        PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/ CLOSE,0)
        PVI=CUM_SUM(PVI_INC)
        PVI_MA=MA(PVI,N)
        PVI 是成交量升高的交易日的价格变化百分比的累积。
        PVI 相关理论认为，如果当前价涨量增，则说明散户主导市场，PVI
        可以用来识别价涨量增的市场（散户主导的市场）。
        如果 PVI 上穿 PVI_MA，则产生买入信号；
        如果 PVI 下穿 PVI_MA，则产生卖出信号。
        """
        df['ref_close'] = (df['close'] - df['close'].shift(1)) / df['close'] # (CLOSE-REF(CLOSE))/ CLOSE
        df['PVI_INC'] = np.where(df['volume'] > df['volume'].shift(1), df['ref_close'], 0) # PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/ CLOSE,0)
        df['PVI'] = df['PVI_INC'].cumsum() #  PVI=CUM_SUM(PVI_INC)
        df['PVI_INC_MA'] = df['PVI'].rolling(n, min_periods=1).mean() # PVI_MA=MA(PVI,N)

        df['前%dhPVI' % n] = df['PVI_INC_MA'].shift(1)
        extra_agg_dict['前%dhPVI' % n] = 'first'
        # 删除中间数据
        del df['ref_close']
        del df['PVI_INC']
        del df['PVI']
        del df['PVI_INC_MA']

    # TMF 指标
    for n in back_hour_list:
        """
        N=80
        HIGH_TRUE=MAX(HIGH,REF(CLOSE,1))
        LOW_TRUE=MIN(LOW,REF(CLOSE,1))
        TMF=EMA(VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TR
        UE-LOW_TRUE),N)/EMA(VOL,N)
        TMF 指标和 CMF 指标类似，都是用价格对成交量加权。但是 CMF
        指标用 CLV 做权重，而 TMF 指标用的是真实最低价和真实最高价，
        且取的是移动平均而不是求和。如果 TMF 上穿 0，则产生买入信号；
        如果 TMF 下穿 0，则产生卖出信号。
        """
        df['ref'] = df['close'].shift(1) # REF(CLOSE,1)
        df['max_high'] = df[['high', 'ref']].max(axis=1) # HIGH_TRUE=MAX(HIGH,REF(CLOSE,1))
        df['min_low'] = df[['low', 'ref']].min(axis=1) # LOW_TRUE=MIN(LOW,REF(CLOSE,1))
        # VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TRUE-LOW_TRUE)
        T = df['volume'] * (2 * df['close'] - df['max_high'] - df['min_low']) / (df['max_high'] - df['min_low'])
        # TMF=EMA(VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TRUE-LOW_TRUE),N)/EMA(VOL,N)
        df['TMF'] = T.ewm(n, adjust=False).mean() / df['volume'].ewm(n, adjust=False).mean()
        df['前%dhTMF' % n] = df['TMF'].shift(1)
        extra_agg_dict['前%dhTMF' % n] = 'first'
        # 删除中间数据
        del df['ref']
        del df['max_high']
        del df['min_low']
        del df['TMF']

    # MFI 指标
    for n in back_hour_list:
        """
        N=14
        TYPICAL_PRICE=(HIGH+LOW+CLOSE)/3
        MF=TYPICAL_PRICE*VOLUME
        MF_POS=SUM(IF(TYPICAL_PRICE>=REF(TYPICAL_PRICE,1),M
        F,0),N)
        MF_NEG=SUM(IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),
        MF,0),N)
        MFI=100-100/(1+MF_POS/MF_NEG)
        MFI 指标的计算与 RSI 指标类似，不同的是，其在上升和下跌的条件
        判断用的是典型价格而不是收盘价，且其是对 MF 求和而不是收盘价
        的变化值。MFI 同样可以用来判断市场的超买超卖状态。
        如果 MFI 上穿 80，则产生买入信号；
        如果 MFI 下穿 20，则产生卖出信号。
        """
        df['price'] = (df['high'] + df['low'] + df['close']) / 3 # TYPICAL_PRICE=(HIGH+LOW+CLOSE)/3
        df['MF'] = df['price'] * df['volume'] # MF=TYPICAL_PRICE*VOLUME
        df['pos'] = np.where(df['price'] >= df['price'].shift(1), df['MF'], 0) # IF(TYPICAL_PRICE>=REF(TYPICAL_PRICE,1),MF,0)MF,0),N)
        df['MF_POS'] = df['pos'].rolling(n).sum()
        df['neg'] = np.where(df['price'] <= df['price'].shift(1), df['MF'], 0) # IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),MF,0)
        df['MF_NEG'] = df['neg'].rolling(n).sum() # MF_NEG=SUM(IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),MF,0),N)

        df['MFI'] = 100 - 100 / (1 + df['MF_POS'] / df['MF_NEG']) # MFI=100-100/(1+MF_POS/MF_NEG)

        df['前%dhMFI' % n] = df['MFI'].shift(1)
        extra_agg_dict['前%dhMFI' % n] = 'first'
        # 删除中间数据
        del df['price']
        del df['MF']
        del df['pos']
        del df['MF_POS']
        del df['neg']
        del df['MF_NEG']
        del df['MFI']

    # ADOSC 指标
    for n in back_hour_list:
        """
        AD=CUM_SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW))
        AD_EMA1=EMA(AD,N1)
        AD_EMA2=EMA(AD,N2) 
        ADOSC=AD_EMA1-AD_EMA2
        ADL（收集派发线）指标是成交量的加权累计求和，其中权重为 CLV
        指标。ADL 指标可以与 OBV 指标进行类比。不同的是 OBV 指标只
        根据价格的变化方向把成交量分为正、负成交量再累加，而 ADL 是 用 CLV 指标作为权重进行成交量的累加。我们知道，CLV 指标衡量
        收盘价在最低价和最高价之间的位置，CLV>0(<0),则收盘价更靠近最
        高（低）价。CLV 越靠近 1(-1)，则收盘价越靠近最高（低）价。如
        果当天的 CLV>0，则 ADL 会加上成交量*CLV（收集）；如果当天的
        CLV<0，则 ADL 会减去成交量*CLV（派发）。
        ADOSC 指标是 ADL（收集派发线）指标的短期移动平均与长期移动
        平均之差。如果 ADOSC 上穿 0，则产生买入信号；如果 ADOSC 下 穿 0，则产生卖出信号。
        """
        # ((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW)
        df['AD'] = ((df['close'] - df['low']) - (df['high'] - df['close'])) * df['volume'] / (
                df['high'] - df['low'])
        df['AD_sum'] = df['AD'].cumsum() # AD=CUM_SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW))
        df['AD_EMA1'] = df['AD_sum'].ewm(n, adjust=False).mean() # AD_EMA1=EMA(AD,N1)
        df['AD_EMA2'] = df['AD_sum'].ewm(n * 2, adjust=False).mean() # AD_EMA2=EMA(AD,N2)
        df['ADOSC'] = df['AD_EMA1'] - df['AD_EMA2'] # ADOSC=AD_EMA1-AD_EMA2

        # 标准化
        df['前%dhADOSC' % n] = (df['ADOSC'] - df['ADOSC'].rolling(n).min()) / (
                df['ADOSC'].rolling(n).max() - df['ADOSC'].rolling(n).min())
        df['前%dhADOSC' % n] = df['前%dhADOSC' % n].shift(1)
        extra_agg_dict['前%dhADOSC' % n] = 'first'
        # 删除中间数据
        del df['AD']
        del df['AD_sum']
        del df['AD_EMA2']
        del df['AD_EMA1']
        del df['ADOSC']

    # VR 指标
    for n in back_hour_list:
        """
        N=40
        AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        AVS=SUM(AV,N)
        BVS=SUM(BV,N)
        CVS=SUM(CV,N)
        VR=(AVS+CVS/2)/(BVS+CVS/2)

        VR 用过去 N 日股价上升日成交量与下降日成交量的比值来衡量多空
        力量对比。当 VR 小于 70 时，表示市场较为低迷；上穿 70 时表示市
        场可能有好转；上穿 250 时表示多方力量压倒空方力量。当 VR>300
        时，市场可能过热、买方力量过强，下穿 300 表明市场可能要反转。
        如果 VR 上穿 250，则产生买入信号；
        如果 VR 下穿 300，则产生卖出信号。
        """
        df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0) # AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0) # BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0) # BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['AVS'] = df['AV'].rolling(n).sum() # AVS=SUM(AV,N)
        df['BVS'] = df['BV'].rolling(n).sum() # BVS=SUM(BV,N)
        df['CVS'] = df['CV'].rolling(n).sum() # CVS=SUM(CV,N)
        df['VR'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2) # VR=(AVS+CVS/2)/(BVS+CVS/2)
        df['前%dhVR' % n] = df['VR'].shift(1)
        extra_agg_dict['前%dhVR' % n] = 'first'
        # 删除中间数据
        del df['AV']
        del df['BV']
        del df['CV']
        del df['AVS']
        del df['BVS']
        del df['CVS']
        del df['VR']

    # KO 指标
    for n in back_hour_list:
        """
        N1=34
        N2=55
        TYPICAL=(HIGH+LOW+CLOSE)/3
        VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
        VOLUME_EMA1=EMA(VOLUME,N1)
        VOLUME_EMA2=EMA(VOLUME,N2)
        KO=VOLUME_EMA1-VOLUME_EMA2
        这个技术指标的目的是为了观察短期和长期股票资金的流入和流出
        的情况。它的主要用途是确认股票价格趋势的方向和强度。KO 与
        OBV,VPT 等指标类似，都是用价格对成交量进行加权。KO 用的是典
        型价格的变化（只考虑变化方向，不考虑变化量），OBV 用的是收
        盘价的变化（只考虑变化方向，不考虑变化量），VPT 用的是价格的
        变化率（即考虑方向又考虑变化幅度）。
        如果 KO 上穿 0，则产生买入信号；
        如果 KO 下穿 0，则产生卖出信号。
        """
        df['price'] = (df['high'] + df['low'] + df['close']) / 3 # TYPICAL=(HIGH+LOW+CLOSE)/3
        df['V'] = np.where(df['price'] > df['price'].shift(1), df['volume'], -df['volume']) # VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
        df['V_ema1'] = df['V'].ewm(n, adjust=False).mean() # VOLUME_EMA1=EMA(VOLUME,N1)
        df['V_ema2'] = df['V'].ewm(int(n * 1.618), adjust=False).mean() # VOLUME_EMA2=EMA(VOLUME,N2)
        df['KO'] = df['V_ema1'] - df['V_ema2'] # KO=VOLUME_EMA1-VOLUME_EMA2
        # 标准化
        df['前%dhKO' % n] = (df['KO'] - df['KO'].rolling(n).min()) / (
                df['KO'].rolling(n).max() - df['KO'].rolling(n).min())
        df['前%dhKO' % n] = df['前%dhKO' % n].shift(1)
        extra_agg_dict['前%dhKO' % n] = 'first'
        # 删除中间数据
        del df['price']
        del df['V']
        del df['V_ema1']
        del df['V_ema2']
        del df['KO']

    return df, extra_agg_dict


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

def scale_zscore(_s, _n):
    # 标准化
    _s = (pd.Series(_s) - pd.Series(_s).rolling(_n, min_periods=1).mean()
          ) / pd.Series(_s).rolling(_n, min_periods=1).std(ddof=0)
    return pd.Series(_s)

def factor_calculation_diff(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # ========以下是需要修改的代码  指标添加差分
    # ===计算各项选币指标
    for n in back_hour_list:
        df['price'] = df['quote_volume'].rolling(n).sum() / df['volume'].rolling(n).sum()

        df['前%dh均价' % n] = (df['price'] - df['price'].rolling(n, min_periods=1).min()) / (
                    df['price'].rolling(n, min_periods=1).max() - df['price'].rolling(n, min_periods=1).min())

        df['前%dh均价' % n] = df['前%dh均价' % n].shift(1)
        extra_agg_dict['前%dh均价' % n] = 'first'
        del df['price']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh均价' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 涨跌幅
    for n in back_hour_list:
        df['前%dh涨跌幅' % n] = df['close'].pct_change(n)
        df['前%dh涨跌幅' % n] = df['前%dh涨跌幅' % n].shift(1)
        extra_agg_dict['前%dh涨跌幅' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh涨跌幅' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 涨跌幅更好的表达方式：bias，币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df['前%dhbias' % n] = df['close'] / ma - 1
        df['前%dhbias' % n] = df['前%dhbias' % n].shift(1)
        extra_agg_dict['前%dhbias' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhbias' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 振幅：最高价最低价
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df['前%dh振幅' % n] = high / low - 1
        df['前%dh振幅' % n] = df['前%dh振幅' % n].shift(1)
        extra_agg_dict['前%dh振幅' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh振幅' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 振幅：收盘价、开盘价
    high = df[['close', 'open']].max(axis=1)
    low = df[['close', 'open']].min(axis=1)
    for n in back_hour_list:
        high = high.rolling(n, min_periods=1).max()
        low = low.rolling(n, min_periods=1).min()
        df['前%dh振幅2' % n] = high / low - 1
        df['前%dh振幅2' % n] = df['前%dh振幅2' % n].shift(1)
        extra_agg_dict['前%dh振幅2' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh振幅2' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 涨跌幅std，振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df['前%dh涨跌幅std' % n] = change.rolling(n).std()
        df['前%dh涨跌幅std' % n] = df['前%dh涨跌幅std' % n].shift(1)
        extra_agg_dict['前%dh涨跌幅std' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh涨跌幅std' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 涨跌幅偏度：在商品期货市场有效
    for n in back_hour_list:
        df['前%dh涨跌幅skew' % n] = change.rolling(n).skew()
        df['前%dh涨跌幅skew' % n] = df['前%dh涨跌幅skew' % n].shift(1)
        extra_agg_dict['前%dh涨跌幅skew' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh涨跌幅skew' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 成交额：对应小市值概念
    for n in back_hour_list:
        df['前%dh成交额' % n] = df['quote_volume'].rolling(n, min_periods=1).sum()
        df['前%dh成交额' % n] = df['前%dh成交额' % n].shift(1)
        extra_agg_dict['前%dh成交额' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh成交额' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 成交额std，191选股因子中最有效的因子
    for n in back_hour_list:
        df['前%dh成交额std' % n] = df['quote_volume'].rolling(n, min_periods=2).std()
        df['前%dh成交额std' % n] = df['前%dh成交额std' % n].shift(1)
        extra_agg_dict['前%dh成交额std' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh成交额std' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 资金流入，币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df['前%dh资金流入比例' % n] = buy_volume / volume
        df['前%dh资金流入比例' % n] = df['前%dh资金流入比例' % n].shift(1)
        extra_agg_dict['前%dh资金流入比例' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh资金流入比例' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 量比
    for n in back_hour_list:
        df['前%dh量比' % n] = df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()
        df['前%dh量比' % n] = df['前%dh量比' % n].shift(1)
        extra_agg_dict['前%dh量比' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh量比' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 成交笔数
    for n in back_hour_list:
        df['前%dh成交笔数' % n] = df['trade_num'].rolling(n, min_periods=1).sum()
        df['前%dh成交笔数' % n] = df['前%dh成交笔数' % n].shift(1)
        extra_agg_dict['前%dh成交笔数' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh成交笔数' % n, _agg_dict=extra_agg_dict, _agg_type='first')
    # 量价相关系数：量价相关选股策略
    for n in back_hour_list:
        df['前%dh量价相关系数' % n] = df['close'].rolling(n).corr(df['quote_volume'].rolling(n))
        df['前%dh量价相关系数' % n] = df['前%dh量价相关系数' % n].shift(1)
        extra_agg_dict['前%dh量价相关系数' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dh量价相关系数' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RSI 指标
    for n in back_hour_list:
        """
        CLOSEUP=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        CLOSEDOWN=IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0)
        CLOSEUP_MA=SMA(CLOSEUP,N,1)
        CLOSEDOWN_MA=SMA(CLOSEDOWN,N,1)
        RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)
        RSI 反映一段时间内平均收益与平均亏损的对比。通常认为当 RSI 大 于 70，市场处于强势上涨甚至达到超买的状态；当 RSI 小于 30，市
        场处于强势下跌甚至达到超卖的状态。当 RSI 跌到 30 以下又上穿 30
        时，通常认为股价要从超卖的状态反弹；当 RSI 超过 70 又下穿 70
        时，通常认为市场要从超买的状态回落了。实际应用中，不一定要使
        用 70/30 的阈值选取。这里我们用 60/40 作为信号产生的阈值。
        RSI 上穿 40 则产生买入信号；
        RSI 下穿 60 则产生卖出信号。
        """
        diff = df['close'].diff()  # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0)  # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff),
                              0)  # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()  # SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean()  # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df['前%dhRSI' % n] = A / (A + B)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df['前%dhRSI' % n] = df['前%dhRSI' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhRSI' % n] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']

        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRSI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # KDJ 指标
    for n in back_hour_list:
        """
        N=40
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        K=SMA(Stochastics,3,1)
        D=SMA(K,3,1) 
        先计算 N 日内的未成熟随机值 RSV，然后计算 K 值=（2*前日 K 值+
        当日 RSV）/3，D 值=（2*前日 D 值+当日 K 值）/3
        KDJ 指标用来衡量当前收盘价在过去 N 天的最低价与最高价之间的
        位置。值越高（低），则说明其越靠近过去 N 天的最高（低）价。当
        值过高或过低时，价格可能发生反转。通常认为 D 值小于 20 处于超
        卖状态，D 值大于 80 属于超买状态。
        如果 D 小于 20 且 K 上穿 D，则产生买入信号；
        如果 D 大于 80 且 K 下穿 D，则产生卖出信号。
        """
        low_list = df['low'].rolling(n, min_periods=1).min()  # MIN(LOW,N) 求周期内low的最小值
        high_list = df['high'].rolling(n, min_periods=1).max()  # MAX(HIGH,N) 求周期内high 的最大值
        rsv = (df['close'] - low_list) / (
                    high_list - low_list) * 100  # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100 计算一个随机值
        # K D J的值在固定的范围内
        df['前%dhK' % n] = rsv.ewm(com=2).mean()  # K=SMA(Stochastics,3,1) 计算k
        df['前%dhD' % n] = df['前%dhK' % n].ewm(com=2).mean()  # D=SMA(K,3,1)  计算D
        df['前%dhJ' % n] = 3 * df['前%dhK' % n] - 2 * df['前%dhD' % n]  # 计算J
        df['前%dhK' % n] = df['前%dhK' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        df['前%dhD' % n] = df['前%dhD' % n].shift(1)
        df['前%dhJ' % n] = df['前%dhJ' % n].shift(1)
        extra_agg_dict['前%dhK' % n] = 'first'
        extra_agg_dict['前%dhD' % n] = 'first'
        extra_agg_dict['前%dhJ' % n] = 'first'

        for _ in ['前%dhK' % n, '前%dhD' % n, '前%dhJ' % n]:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算魔改CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        df['oma'] = df['open'].ewm(span=n, adjust=False).mean()  # 取 open 的ema
        df['hma'] = df['high'].ewm(span=n, adjust=False).mean()  # 取 high 的ema
        df['lma'] = df['low'].ewm(span=n, adjust=False).mean()  # 取 low的ema
        df['cma'] = df['close'].ewm(span=n, adjust=False).mean()  # 取 close的ema
        df['tp'] = (df['oma'] + df['hma'] + df['lma'] + df[
            'cma']) / 4  # 魔改CCI基础指标 将TP=(HIGH+LOW+CLOSE)/3  替换成以open/high/low/close的ema 的均值
        df['ma'] = df['tp'].ewm(span=n, adjust=False).mean()  # MA(TP,N)  将移动平均改成 ema
        df['abs_diff_close'] = abs(df['tp'] - df['ma'])  # ABS(TP-MA)
        df['md'] = df['abs_diff_close'].ewm(span=n, adjust=False).mean()  # MD=MA(ABS(TP-MA),N)  将移动平均替换成ema

        df['前%dhCCI' % n] = (df['tp'] - df['ma']) / df['md']  # CCI=(TP-MA)/(0.015MD)  CCI在一定范围内
        df['前%dhCCI' % n] = df['前%dhCCI' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhCCI' % n] = 'first'
        # # 删除中间数据
        del df['oma']
        del df['hma']
        del df['lma']
        del df['cma']
        del df['tp']
        del df['ma']
        del df['abs_diff_close']
        del df['md']

        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCCI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        open_ma = df['open'].rolling(n, min_periods=1).mean()
        high_ma = df['high'].rolling(n, min_periods=1).mean()
        low_ma = df['low'].rolling(n, min_periods=1).mean()
        close_ma = df['close'].rolling(n, min_periods=1).mean()
        tp = (high_ma + low_ma + close_ma) / 3  # TP=(HIGH+LOW+CLOSE)/3
        ma = tp.rolling(n, min_periods=1).mean()  # MA=MA(TP,N)
        md = abs(ma - close_ma).rolling(n, min_periods=1).mean()  # MD=MA(ABS(TP-MA),N)
        df['前%dhmagic_CCI' % n] = (tp - ma) / md / 0.015  # CCI=(TP-MA)/(0.015MD)
        df['前%dhmagic_CCI' % n] = df['前%dhmagic_CCI' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhmagic_CCI' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhmagic_CCI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算macd指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=5
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        MACD_SIGNAL=EMA(MACD,N3)
        MACD_HISTOGRAM=MACD-MACD_SIGNAL

        MACD 指标衡量快速均线与慢速均线的差值。由于慢速均线反映的是
        之前较长时间的价格的走向，而快速均线反映的是较短时间的价格的
        走向，所以在上涨趋势中快速均线会比慢速均线涨的快，而在下跌趋
        势中快速均线会比慢速均线跌得快。所以 MACD 上穿/下穿 0 可以作
        为一种构造交易信号的方式。另外一种构造交易信号的方式是求
        MACD 与其移动平均（信号线）的差值得到 MACD 柱，利用 MACD
        柱上穿/下穿 0（即 MACD 上穿/下穿其信号线）来构造交易信号。这
        种方式在其他指标的使用中也可以借鉴。
        """
        short_windows = n
        long_windows = 3 * n
        macd_windows = int(1.618 * n)

        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean()  # EMA(CLOSE,N2)
        df['dif'] = df['ema_short'] - df['ema_long']  # MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['dea'] = df['dif'].ewm(span=macd_windows, adjust=False).mean()  # MACD_SIGNAL=EMA(MACD,N3)
        df['macd'] = 2 * (df['dif'] - df['dea'])  # MACD_HISTOGRAM=MACD-MACD_SIGNAL  一般看图指标计算对应实际乘以了2倍
        # 进行去量纲
        df['前%dhmacd' % n] = df['macd'] / df['macd'].rolling(macd_windows, min_periods=1).mean() - 1

        # df['前%dhdif' % n] = df['前%dhdif' % n].shift(1)
        # extra_agg_dict['前%dhdif' % n] = 'first'
        #
        # df['前%dhdea' % n] = df['前%dhdea' % n].shift(1)
        # extra_agg_dict['前%dhdea' % n] = 'first'

        df['前%dhmacd' % n] = df['前%dhmacd' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhmacd' % n] = 'first'

        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['dif']
        del df['dea']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhmacd' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算ema的差值
    for n in back_hour_list:
        """
        与求MACD的dif线一样
        """
        short_windows = n
        long_windows = 3 * n
        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()  # 计算短周期ema
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean()  # 计算长周期的ema
        df['diff_ema'] = df['ema_short'] - df['ema_long']  # 计算俩条线之间的差值

        df['diff_ema_mean'] = df['diff_ema'].ewm(span=n, adjust=False).mean()

        df['前%dhdiff_ema' % n] = df['diff_ema'] / df['diff_ema_mean'] - 1  # 去量纲
        df['前%dhdiff_ema' % n] = df['前%dhdiff_ema' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhdiff_ema' % n] = 'first'
        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['diff_ema']
        del df['diff_ema_mean']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhdiff_ema' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # bias因子以均价表示
    for n in back_hour_list:
        """
        将bias 的close替换成vwap
        """
        df['vwap'] = df['volume'] / df['quote_volume']  # 在周期内成交额除以成交量等于成交均价
        ma = df['vwap'].rolling(n, min_periods=1).mean()  # 求移动平均线
        df['前%dhvwap_bias' % n] = df['vwap'] / ma - 1  # 去量纲
        df['前%dhvwap_bias' % n] = df['前%dhvwap_bias' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhvwap_bias' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhvwap_bias' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算BBI 的bias
    for n in back_hour_list:
        """
        BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        BBI 是对不同时间长度的移动平均线取平均，能够综合不同移动平均
        线的平滑性和滞后性。如果收盘价上穿/下穿 BBI 则产生买入/卖出信
        号。
        """
        # 将BBI指标计算出来求bias
        ma1 = df['close'].rolling(n, min_periods=1).mean()
        ma2 = df['close'].rolling(2 * n, min_periods=1).mean()
        ma3 = df['close'].rolling(4 * n, min_periods=1).mean()
        ma4 = df['close'].rolling(8 * n, min_periods=1).mean()
        bbi = (ma1 + ma2 + ma3 + ma4) / 4  # BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        df['前%dhbbi_bias' % n] = df['close'] / bbi - 1
        df['前%dhbbi_bias' % n] = df['前%dhbbi_bias' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhbbi_bias' % n] = 'first'

        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhbbi_bias' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算 DPO
    for n in back_hour_list:
        """
        N=20
        DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        DPO 是当前价格与延迟的移动平均线的差值，通过去除前一段时间
        的移动平均价格来减少长期的趋势对短期价格波动的影响。DPO>0
        表示目前处于多头市场；DPO<0 表示当前处于空头市场。我们通过
        DPO 上穿/下穿 0 线来产生买入/卖出信号。

        """
        ma = df['close'].rolling(n, min_periods=1).mean()  # 求close移动平均线
        ref = ma.shift(int(n / 2 + 1))  # REF(MA(CLOSE,N),N/2+1)
        df['DPO'] = df['close'] - ref  # DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        df['DPO_ma'] = df['DPO'].rolling(n, min_periods=1).mean()  # 求均值
        df['前%dhDPO' % n] = df['DPO'] / df['DPO_ma'] - 1  # 去量纲
        df['前%dhDPO' % n] = df['前%dhDPO' % n].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict['前%dhDPO' % n] = 'first'
        # 删除中间数据
        del df['DPO']
        del df['DPO_ma']

        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDPO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # 计算 ER
    for n in back_hour_list:
        """
        N=20
        BullPower=HIGH-EMA(CLOSE,N)
        BearPower=LOW-EMA(CLOSE,N)
        ER 为动量指标。用来衡量市场的多空力量对比。在多头市场，人们
        会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量
        越强；而在空头市场，人们可能因为恐惧而在接近低价的地方卖出。
        BearPower 越低则当前空头力量越强。当两者都大于 0 时，反映当前
        多头力量占据主导地位；两者都小于0则反映空头力量占据主导地位。
        如果 BearPower 上穿 0，则产生买入信号；
        如果 BullPower 下穿 0，则产生卖出信号。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        bull_power = df['high'] - ema  # 越高表示上涨 牛市 BullPower=HIGH-EMA(CLOSE,N)
        bear_power = df['low'] - ema  # 越低表示下降越厉害  熊市 BearPower=LOW-EMA(CLOSE,N)
        df['前%dhER_bull' % n] = bull_power / ema  # 去量纲
        df['前%dhER_bear' % n] = bear_power / ema  # 去量纲
        df['前%dhER_bull' % n] = df['前%dhER_bull' % n].shift(1)
        df['前%dhER_bear' % n] = df['前%dhER_bear' % n].shift(1)
        extra_agg_dict['前%dhER_bull' % n] = 'first'
        extra_agg_dict['前%dhER_bear' % n] = 'first'

        for _ in ['前%dhER_bull' % n, '前%dhER_bear' % n]:
        # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')

    # PO指标
    for n in back_hour_list:
        """
        EMA_SHORT=EMA(CLOSE,9)
        EMA_LONG=EMA(CLOSE,26)
        PO=(EMA_SHORT-EMA_LONG)/EMA_LONG*100
        PO 指标求的是短期均线与长期均线之间的变化率。
        如果 PO 上穿 0，则产生买入信号；
        如果 PO 下穿 0，则产生卖出信号。
        """
        ema_short = df['close'].ewm(n, adjust=False).mean()  # 短周期的ema
        ema_long = df['close'].ewm(n * 3, adjust=False).mean()  # 长周期的ema   固定倍数关系 减少参数
        df['前%dhPO' % n] = (ema_short - ema_long) / ema_long * 100  # 去量纲
        df['前%dhPO' % n] = df['前%dhPO' % n].shift(1)
        extra_agg_dict['前%dhPO' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # MADisplaced 指标
    for n in back_hour_list:
        """
        N=20
        M=10
        MA_CLOSE=MA(CLOSE,N)
        MADisplaced=REF(MA_CLOSE,M)
        MADisplaced 指标把简单移动平均线向前移动了 M 个交易日，用法
        与一般的移动平均线一样。如果收盘价上穿/下穿 MADisplaced 则产
        生买入/卖出信号。
        有点变种bias
        """
        ma = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,N) 固定俩个参数之间的关系  减少参数
        ref = ma.shift(n)  # MADisplaced=REF(MA_CLOSE,M)

        df['前%dhMADisplaced' % n] = df['close'] / ref - 1  # 去量纲
        df['前%dhMADisplaced' % n] = df['前%dhMADisplaced' % n].shift(1)
        extra_agg_dict['前%dhMADisplaced' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhMADisplaced' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # T3 指标
    for n in back_hour_list:
        """
        N=20
        VA=0.5
        T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        当 VA 是 0 时，T3 就是三重指数平均线，此时具有严重的滞后性；当
        VA 是 0 时，T3 就是三重双重指数平均线（DEMA），此时可以快速
        反应价格的变化。VA 值是 T3 指标的一个关键参数，可以用来调节
        T3 指标的滞后性。如果收盘价上穿/下穿 T3，则产生买入/卖出信号。
        """
        va = 0.5
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N),N)
        T1 = ema * (1 + va) - ema_ema * va  # T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T1_ema = T1.ewm(n, adjust=False).mean()  # EMA(T1,N)
        T1_ema_ema = T1_ema.ewm(n, adjust=False).mean()  # EMA(EMA(T1,N),N)
        T2 = T1_ema * (1 + va) - T1_ema_ema * va  # T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T2_ema = T2.ewm(n, adjust=False).mean()  # EMA(T2,N)
        T2_ema_ema = T2_ema.ewm(n, adjust=False).mean()  # EMA(EMA(T2,N),N)
        T3 = T2_ema * (1 + va) - T2_ema_ema * va  # T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        df['前%dhT3' % n] = df['close'] / T3 - 1  # 去量纲
        df['前%dhT3' % n] = df['前%dhT3' % n].shift(1)
        extra_agg_dict['前%dhT3' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhT3' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # POS指标
    for n in back_hour_list:
        """
        N=100
        PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        POS 指标衡量当前的 N 天收益率在过去 N 天的 N 天收益率最大值和
        最小值之间的位置。当 POS 上穿 80 时产生买入信号；当 POS 下穿
        20 时产生卖出信号。
        """
        ref = df['close'].shift(n)  # REF(CLOSE,N)
        price = (df['close'] - ref) / ref  # PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        min_price = price.rolling(n).min()  # MIN(PRICE,N)
        max_price = price.rolling(n).max()  # MAX(PRICE,N)
        pos = (price - min_price) / (max_price - min_price)  # POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        df['前%dhPOS' % n] = pos.shift(1)
        extra_agg_dict['前%dhPOS' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPOS' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PAC 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        UPPER=SMA(HIGH,N1,1)
        LOWER=SMA(LOW,N2,1)
        用最高价和最低价的移动平均来构造价格变化的通道，如果价格突破
        上轨则做多，突破下轨则做空。
        """
        # upper = df['high'].rolling(n, min_periods=1).mean()
        df['upper'] = df['high'].ewm(span=n).mean()  # UPPER=SMA(HIGH,N1,1)
        # lower = df['low'].rolling(n, min_periods=1).mean()
        df['lower'] = df['low'].ewm(span=n).mean()  # LOWER=SMA(LOW,N2,1)
        df['width'] = df['upper'] - df['lower']  # 添加指标求宽度进行去量纲
        df['width_ma'] = df['width'].rolling(n, min_periods=1).mean()

        df['前%dhPAC' % n] = df['width'] / df['width_ma'] - 1
        df['前%dhPAC' % n] = df['前%dhPAC' % n].shift(1)
        extra_agg_dict['前%dhPAC' % n] = 'first'

        # 删除中间数据
        del df['upper']
        del df['lower']
        del df['width']
        del df['width_ma']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPAC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ADM 指标
    for n in back_hour_list:
        """
        N=20
        DTM=IF(OPEN>REF(OPEN,1),MAX(HIGH-OPEN,OPEN-REF(OP
        EN,1)),0)
        DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-O
        PEN),0)
        STM=SUM(DTM,N)
        SBM=SUM(DBM,N)
        ADTM=(STM-SBM)/MAX(STM,SBM)
        ADTM 通过比较开盘价往上涨的幅度和往下跌的幅度来衡量市场的
        人气。ADTM 的值在-1 到 1 之间。当 ADTM 上穿 0.5 时，说明市场
        人气较旺；当 ADTM 下穿-0.5 时，说明市场人气较低迷。我们据此构
        造交易信号。
        当 ADTM 上穿 0.5 时产生买入信号；
        当 ADTM 下穿-0.5 时产生卖出信号。

        """
        df['h_o'] = df['high'] - df['open']  # HIGH-OPEN
        df['diff_open'] = df['open'] - df['open'].shift(1)  # OPEN-REF(OPEN,1)
        max_value1 = df[['h_o', 'diff_open']].max(axis=1)  # MAX(HIGH-OPEN,OPEN-REF(OPEN,1))
        # df.loc[df['open'] > df['open'].shift(1), 'DTM'] = max_value1
        # df['DTM'].fillna(value=0, inplace=True)
        df['DTM'] = np.where(df['open'] > df['open'].shift(1), max_value1,
                             0)  # DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['o_l'] = df['open'] - df['low']  # OPEN-LOW
        max_value2 = df[['o_l', 'diff_open']].max(axis=1)  # MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['DBM'] = np.where(df['open'] < df['open'].shift(1), max_value2,
                             0)  # DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        # df.loc[df['open'] < df['open'].shift(1), 'DBM'] = max_value2
        # df['DBM'].fillna(value=0, inplace=True)

        df['STM'] = df['DTM'].rolling(n).sum()  # STM=SUM(DTM,N)
        df['SBM'] = df['DBM'].rolling(n).sum()  # SBM=SUM(DBM,N)
        max_value3 = df[['STM', 'SBM']].max(axis=1)  # MAX(STM,SBM)
        ADTM = (df['STM'] - df['SBM']) / max_value3  # ADTM=(STM-SBM)/MAX(STM,SBM)
        df['前%dhADTM' % n] = ADTM.shift(1)
        extra_agg_dict['前%dhADTM' % n] = 'first'

        # 删除中间数据
        del df['h_o']
        del df['diff_open']
        del df['o_l']
        del df['STM']
        del df['SBM']
        del df['DBM']
        del df['DTM']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhADTM' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ZLMACD 指标
    for n in back_hour_list:
        """
        N1=20
        N2=100
        ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EM
        A(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        ZLMACD 指标是对 MACD 指标的改进，它在计算中使用 DEMA 而不
        是 EMA，可以克服 MACD 指标的滞后性问题。如果 ZLMACD 上穿/
        下穿 0，则产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N1)
        ema_ema_1 = ema1.ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N1),N1)
        n2 = 5 * n  # 固定俩参数的倍数关系减少参数
        ema2 = df['close'].ewm(n2, adjust=False).mean()  # EMA(CLOSE,N2)
        ema_ema_2 = ema2.ewm(n2, adjust=False).mean()  # EMA(EMA(CLOSE,N2),N2)
        ZLMACD = (2 * ema1 - ema_ema_1) - (
                    2 * ema2 - ema_ema_2)  # ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EMA(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        df['前%dhZLMACD' % n] = df['close'] / ZLMACD - 1
        df['前%dhZLMACD' % n] = df['前%dhZLMACD' % n].shift(1)
        extra_agg_dict['前%dhZLMACD' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhZLMACD' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # TMA 指标
    for n in back_hour_list:
        """
        N=20
        CLOSE_MA=MA(CLOSE,N)
        TMA=MA(CLOSE_MA,N)
        TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
        越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
        靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
        卖出信号。
        """
        ma = df['close'].rolling(n, min_periods=1).mean()  # CLOSE_MA=MA(CLOSE,N)
        tma = ma.rolling(n, min_periods=1).mean()  # TMA=MA(CLOSE_MA,N)
        df['前%dhtma_bias' % n] = df['close'] / tma - 1
        df['前%dhtma_bias' % n] = df['前%dhtma_bias' % n].shift(1)
        extra_agg_dict['前%dhtma_bias' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhtma_bias' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # TYP 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1=EMA(TYP,N1)
        TYPMA2=EMA(TYP,N2)
        在技术分析中，典型价格（最高价+最低价+收盘价）/3 经常被用来代
        替收盘价。比如我们在利用均线交叉产生交易信号时，就可以用典型
        价格的均线。
        TYPMA1 上穿/下穿 TYPMA2 时产生买入/卖出信号。
        """
        TYP = (df['close'] + df['high'] + df['low']) / 3  # TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1 = TYP.ewm(n, adjust=False).mean()  # TYPMA1=EMA(TYP,N1)
        TYPMA2 = TYP.ewm(n * 3, adjust=False).mean()  # TYPMA2=EMA(TYP,N2) 并且固定俩参数倍数关系
        diff_TYP = TYPMA1 - TYPMA2  # 俩ema相差
        diff_TYP_mean = diff_TYP.rolling(n, min_periods=1).mean()
        # diff_TYP_min = diff_TYP.rolling(n, min_periods=1).std()
        # 无量纲
        df['前%dhTYP' % n] = diff_TYP / diff_TYP_mean - 1
        df['前%dhTYP' % n] = df['前%dhTYP' % n].shift(1)
        extra_agg_dict['前%dhTYP' % n] = 'first'

    # KDJD 指标
    for n in back_hour_list:
        """
        N=20
        M=60
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K=SMA(Stochastics_DOUBLE,3,1)
        D=SMA(K,3,1)
        KDJD 可以看作 KDJ 的变形。KDJ 计算过程中的变量 Stochastics 用
        来衡量收盘价位于最近 N 天最高价和最低价之间的位置。而 KDJD 计
        算过程中的 Stochastics_DOUBLE 可以用来衡量 Stochastics 在最近
        N 天的 Stochastics 最大值与最小值之间的位置。我们这里将其用作
        动量指标。当 D 上穿 70/下穿 30 时，产生买入/卖出信号。
        """
        min_low = df['low'].rolling(n).min()  # LOW_N=MIN(LOW,N)
        max_high = df['high'].rolling(n).max()  # HIGH_N=MAX(HIGH,N)
        Stochastics = (df['close'] - min_low) / (
                    max_high - min_low) * 100  # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        # 固定俩参数的倍数关系
        Stochastics_LOW = Stochastics.rolling(n * 3).min()  # Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH = Stochastics.rolling(n * 3).max()  # Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE = (Stochastics - Stochastics_LOW) / (
                    Stochastics_HIGH - Stochastics_LOW)  # Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K = Stochastics_DOUBLE.ewm(com=2).mean()  # K=SMA(Stochastics_DOUBLE,3,1)
        D = K.ewm(com=2).mean()  # D=SMA(K,3,1)
        df['前%dhKDJD_K' % n] = K.shift(1)
        df['前%dhKDJD_D' % n] = D.shift(1)
        extra_agg_dict['前%dhKDJD_K' % n] = 'first'
        extra_agg_dict['前%dhKDJD_D' % n] = 'first'
        for _ in ['前%dhKDJD_K' % n, '前%dhKDJD_D' % n]:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')

    # VMA 指标
    for n in back_hour_list:
        """
        N=20
        PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        VMA=MA(PRICE,N)
        VMA 就是简单移动平均把收盘价替换为最高价、最低价、开盘价和
        收盘价的平均值。当 PRICE 上穿/下穿 VMA 时产生买入/卖出信号。
        """
        price = (df['high'] + df['low'] + df['open'] + df['close']) / 4  # PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        vma = price.rolling(n, min_periods=1).mean()  # VMA=MA(PRICE,N)
        df['前%dhvma_bias' % n] = price / vma - 1  # 去量纲
        df['前%dhvma_bias' % n] = df['前%dhvma_bias' % n].shift(1)
        extra_agg_dict['前%dhvma_bias' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhvma_bias' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # DDI 指标
    for n in back_hour_list:
        """
        n = 40
        HL=HIGH+LOW
        HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        LOW_ABS=ABS(LOW-REF(LOW,1))
        DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DDI=DIZ-DIF
        DDI 指标用来比较向上波动和向下波动的比例。如果 DDI 上穿/下穿 0
        则产生买入/卖出信号。
        """
        df['hl'] = df['high'] + df['low']  # HL=HIGH+LOW
        df['abs_high'] = abs(df['high'] - df['high'].shift(1))  # HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        df['abs_low'] = abs(df['low'] - df['low'].shift(1))  # LOW_ABS=ABS(LOW-REF(LOW,1))
        max_value1 = df[['abs_high', 'abs_low']].max(axis=1)  # MAX(HIGH_ABS,LOW_ABS)
        # df.loc[df['hl'] > df['hl'].shift(1), 'DMZ'] = max_value1
        # df['DMZ'].fillna(value=0, inplace=True)
        df['DMZ'] = np.where((df['hl'] > df['hl'].shift(1)), max_value1,
                             0)  # DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        # df.loc[df['hl'] < df['hl'].shift(1), 'DMF'] = max_value1
        # df['DMF'].fillna(value=0, inplace=True)
        df['DMF'] = np.where((df['hl'] < df['hl'].shift(1)), max_value1,
                             0)  # DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)

        DMZ_SUM = df['DMZ'].rolling(n).sum()  # SUM(DMZ,N)
        DMF_SUM = df['DMF'].rolling(n).sum()  # SUM(DMF,N)
        DIZ = DMZ_SUM / (DMZ_SUM + DMF_SUM)  # DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF = DMF_SUM / (DMZ_SUM + DMF_SUM)  # DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        df['前%dhDDI' % n] = DIZ - DIF
        df['前%dhDDI' % n] = df['前%dhDDI' % n].shift(1)
        extra_agg_dict['前%dhDDI' % n] = 'first'
        # 删除中间数据
        del df['hl']
        del df['abs_high']
        del df['abs_low']
        del df['DMZ']
        del df['DMF']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDDI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # HMA 指标
    for n in back_hour_list:
        """
        N=20
        HMA=MA(HIGH,N)
        HMA 指标为简单移动平均线把收盘价替换为最高价。当最高价上穿/
        下穿 HMA 时产生买入/卖出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean()  # HMA=MA(HIGH,N)
        df['前%dhHMA' % n] = df['high'] / hma - 1  # 去量纲
        df['前%dhHMA' % n] = df['前%dhHMA' % n].shift(1)
        extra_agg_dict['前%dhHMA' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhHMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # SROC 指标
    for n in back_hour_list:
        """
        N=13
        M=21
        EMAP=EMA(CLOSE,N)
        SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROC 与 ROC 类似，但是会对收盘价进行平滑处理后再求变化率。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMAP=EMA(CLOSE,N)
        ref = ema.shift(2 * n)  # 固定俩参数之间的倍数 REF(EMAP,M)
        df['前%dhSROC' % n] = (ema - ref) / ref  # SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df['前%dhSROC' % n] = df['前%dhSROC' % n].shift(1)
        extra_agg_dict['前%dhSROC' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhSROC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # DC 指标
    for n in back_hour_list:
        """
        N=20
        UPPER=MAX(HIGH,N)
        LOWER=MIN(LOW,N)
        MIDDLE=(UPPER+LOWER)/2
        DC 指标用 N 天最高价和 N 天最低价来构造价格变化的上轨和下轨，
        再取其均值作为中轨。当收盘价上穿/下穿中轨时产生买入/卖出信号。
        """
        upper = df['high'].rolling(n, min_periods=1).max()  # UPPER=MAX(HIGH,N)
        lower = df['low'].rolling(n, min_periods=1).min()  # LOWER=MIN(LOW,N)
        middle = (upper + lower) / 2  # MIDDLE=(UPPER+LOWER)/2
        ma_middle = middle.rolling(n, min_periods=1).mean()  # 求中轨的均线
        # 进行无量纲处理
        df['前%dhDC' % n] = middle / ma_middle - 1
        df['前%dhDC' % n] = df['前%dhDC' % n].shift(1)
        extra_agg_dict['前%dhDC' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # VIDYA
    for n in back_hour_list:
        """
        N=10
        VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER
        （EfficiencyRatio）指标。在当前趋势较强时，ER 值较大，VIDYA
        会赋予当前价格更大的权重，使得 VIDYA 紧随价格变动，减小其滞
        后性；在当前趋势较弱（比如振荡市中）,ER 值较小，VIDYA 会赋予
        当前价格较小的权重，增大 VIDYA 的滞后性，使其更加平滑，避免
        产生过多的交易信号。
        当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
        """
        df['abs_diff_close'] = abs(df['close'] - df['close'].shift(n))  # ABS(CLOSE-REF(CLOSE,N))
        df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n).sum()  # SUM(ABS(CLOSE-REF(CLOSE,1))
        VI = df['abs_diff_close'] / df[
            'abs_diff_close_sum']  # VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA = VI * df['close'] + (1 - VI) * df['close'].shift(1)  # VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        # 进行无量纲处理
        df['前%dhVIDYA' % n] = VIDYA / df['close'] - 1
        df['前%dhVIDYA' % n] = df['前%dhVIDYA' % n].shift(1)
        extra_agg_dict['前%dhVIDYA' % n] = 'first'
        # 删除中间数据
        del df['abs_diff_close']
        del df['abs_diff_close_sum']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhVIDYA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # Qstick 指标
    for n in back_hour_list:
        """
        N=20
        Qstick=MA(CLOSE-OPEN,N)
        Qstick 通过比较收盘价与开盘价来反映股价趋势的方向和强度。如果
        Qstick 上穿/下穿 0 则产生买入/卖出信号。
        """
        cl = df['close'] - df['open']  # CLOSE-OPEN
        Qstick = cl.rolling(n, min_periods=1).mean()  # Qstick=MA(CLOSE-OPEN,N)
        # 进行无量纲处理
        df['前%dhQstick' % n] = cl / Qstick - 1
        df['前%dhQstick' % n] = df['前%dhQstick' % n].shift(1)
        extra_agg_dict['前%dhQstick' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhQstick' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # FB 指标
    # for n in back_hour_list:
    #     """
    #     N=20
    #     TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(
    #     CLOSE,1)))
    #     ATR=MA(TR,N)
    #     MIDDLE=MA(CLOSE,N)
    #     UPPER1=MIDDLE+1.618*ATR
    #     UPPER2=MIDDLE+2.618*ATR
    #     UPPER3=MIDDLE+4.236*ATR
    #     LOWER1=MIDDLE-1.618*ATR
    #     LOWER2=MIDDLE-2.618*ATR
    #     LOWER3=MIDDLE-4.236*ATR
    #     FB 指标类似于布林带，都以价格的移动平均线为中轨，在中线上下
    #     浮动一定数值构造上下轨。不同的是，Fibonacci Bands 有三条上轨
    #     和三条下轨，且分别为中轨加减 ATR 乘 Fibonacci 因子所得。当收盘
    #     价突破较高的两个上轨的其中之一时，产生买入信号；收盘价突破较
    #     低的两个下轨的其中之一时，产生卖出信号。
    #     """
    #     df['c1'] = df['high'] - df['low']
    #     df['c2'] = abs(df['high'] - df['close'].shift(1))
    #     df['c3'] = abs(df['low'] - df['close'].shift(1))
    #     df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    #     df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()
    #     df['middle'] = df['close'].rolling(n, min_periods=1).mean()
    #     #  添加通道部分需要后续进行过滤
    #     upper1 = df['middle'] + 1.618 * df['ATR']
    #     upper2 = df['middle'] + 2.618 * df['ATR']
    #     upper3 = df['middle'] + 4.236 * df['ATR']
    #
    #     lower1 = df['middle'] - 1.618 * df['ATR']
    #     lower2 = df['middle'] - 2.618 * df['ATR']
    #     lower3 = df['middle'] - 4.236 * df['ATR']

    # ATR 因子
    for n in back_hour_list:
        """
        N=20
        TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        ATR=MA(TR,N)
        MIDDLE=MA(CLOSE,N)
        """
        df['c1'] = df['high'] - df['low']  # HIGH-LOW
        df['c2'] = abs(df['high'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1)
        df['c3'] = abs(df['low'] - df['close'].shift(1))  # ABS(LOW-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)  # TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()  # ATR=MA(TR,N)
        df['middle'] = df['close'].rolling(n, min_periods=1).mean()  # MIDDLE=MA(CLOSE,N)

        # ATR指标去量纲
        df['前%dhATR' % n] = df['ATR'] / df['middle']
        df['前%dhATR' % n] = df['前%dhATR' % n].shift(1)
        extra_agg_dict['前%dhATR' % n] = 'first'
        # 删除中间数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['middle']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhATR' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # DEMA 指标
    for n in back_hour_list:
        """
        N=60
        EMA=EMA(CLOSE,N)
        DEMA=2*EMA-EMA(EMA,N)
        DEMA 结合了单重 EMA 和双重 EMA，在保证平滑性的同时减少滞后
        性。
        """
        ema = df['close'].ewm(n, adjust=False).mean()  # EMA=EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean()  # EMA(EMA,N)
        dema = 2 * ema - ema_ema  # DEMA=2*EMA-EMA(EMA,N)
        # dema 去量纲
        df['前%dhDEMA' % n] = dema / ema - 1
        df['前%dhDEMA' % n] = df['前%dhDEMA' % n].shift(1)
        extra_agg_dict['前%dhDEMA' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDEMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # APZ 指标
    for n in back_hour_list:
        """
        N=10
        M=20
        PARAM=2
        VOL=EMA(EMA(HIGH-LOW,N),N)
        UPPER=EMA(EMA(CLOSE,M),M)+PARAM*VOL
        LOWER= EMA(EMA(CLOSE,M),M)-PARAM*VOL
        APZ（Adaptive Price Zone 自适应性价格区间）与布林线 Bollinger 
        Band 和肯通纳通道 Keltner Channel 很相似，都是根据价格波动性围
        绕均线而制成的价格通道。只是在这三个指标中计算价格波动性的方
        法不同。在布林线中用了收盘价的标准差，在肯通纳通道中用了真波
        幅 ATR，而在 APZ 中运用了最高价与最低价差值的 N 日双重指数平
        均来反映价格的波动幅度。
        """
        df['hl'] = df['high'] - df['low']  # HIGH-LOW,
        df['ema_hl'] = df['hl'].ewm(n, adjust=False).mean()  # EMA(HIGH-LOW,N)
        df['vol'] = df['ema_hl'].ewm(n, adjust=False).mean()  # VOL=EMA(EMA(HIGH-LOW,N),N)

        # 计算通道 可以作为CTA策略 作为因子的时候进行改造
        df['ema_close'] = df['close'].ewm(2 * n, adjust=False).mean()  # EMA(CLOSE,M)
        df['ema_ema_close'] = df['ema_close'].ewm(2 * n, adjust=False).mean()  # EMA(EMA(CLOSE,M),M)
        # EMA去量纲
        df['前%dhAPZ' % n] = df['vol'] / df['ema_ema_close']
        df['前%dhAPZ' % n] = df['前%dhAPZ' % n].shift(1)
        extra_agg_dict['前%dhAPZ' % n] = 'first'
        # 删除中间数据
        del df['hl']
        del df['ema_hl']
        del df['vol']
        del df['ema_close']
        del df['ema_ema_close']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhAPZ' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ASI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        M=20
        ASI=CUMSUM(SI)
        ASIMA=MA(ASI,M)
        由于 SI 的波动性比较大，所以我们一般对 SI 累计求和得到 ASI 并捕
        捉 ASI 的变化趋势。一般我们不会直接看 ASI 的数值（对 SI 累计求
        和的求和起点不同会导致求出 ASI 的值不同），而是会观察 ASI 的变
        化方向。我们利用 ASI 与其均线的交叉来产生交易信号,上穿/下穿均
        线时买入/卖出。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1))  # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1))  # B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1))  # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1))  # D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1)  # K=MAX(A,B)

        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D']  # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D']  # R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D']  # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'],
                            df['R2'])  # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] > df['A']) & (df['C'] >= df['B']), df['R3'],
                           df['R4'])  # R=IF((C>=A) & (C>=B),R3,R4)
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / df['R'] * df[
                       'K'] / n  # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M

        df['ASI'] = df['SI'].cumsum()  # ASI=CUMSUM(SI)
        df['ASI_MA'] = df['ASI'].rolling(n, min_periods=1).mean()  # ASIMA=MA(ASI,M)

        df['前%dhASI' % n] = df['ASI'] / df['ASI_MA'] - 1
        df['前%dhASI' % n] = df['前%dhASI' % n].shift(1)
        extra_agg_dict['前%dhASI' % n] = 'first'
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']
        del df['ASI']
        del df['ASI_MA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhASI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # CR 指标
    for n in back_hour_list:
        """
        N=20
        TYP=(HIGH+LOW+CLOSE)/3
        H=MAX(HIGH-REF(TYP,1),0)
        L=MAX(REF(TYP,1)-LOW,0)
        CR=SUM(H,N)/SUM(L,N)*100
        CR 与 AR、BR 类似。CR 通过比较最高价、最低价和典型价格来衡
        量市场人气，其衡量昨日典型价格在今日最高价、最低价之间的位置。
        CR 超过 200 时，表示股价上升强势；CR 低于 50 时，表示股价下跌
        强势。如果 CR 上穿 200/下穿 50 则产生买入/卖出信号。
        """
        df['TYP'] = (df['high'] + df['low'] + df['close']) / 3  # TYP=(HIGH+LOW+CLOSE)/3
        df['H_TYP'] = df['high'] - df['TYP'].shift(1)  # HIGH-REF(TYP,1)
        df['H'] = np.where(df['high'] > df['TYP'].shift(1), df['H_TYP'], 0)  # H=MAX(HIGH-REF(TYP,1),0)
        df['L_TYP'] = df['TYP'].shift(1) - df['low']  # REF(TYP,1)-LOW
        df['L'] = np.where(df['TYP'].shift(1) > df['low'], df['L_TYP'], 0)  # L=MAX(REF(TYP,1)-LOW,0)
        df['CR'] = df['H'].rolling(n).sum() / df['L'].rolling(n).sum() * 100  # CR=SUM(H,N)/SUM(L,N)*100
        df['前%dhCR' % n] = df['CR'].shift(1)
        extra_agg_dict['前%dhCR' % n] = 'first'
        # 删除中间数据
        del df['TYP']
        del df['H_TYP']
        del df['H']
        del df['L_TYP']
        del df['L']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCR' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # BOP 指标
    for n in back_hour_list:
        """
        N=20
        BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)
        BOP 的变化范围为-1 到 1，用来衡量收盘价与开盘价的距离（正、负
        距离）占最高价与最低价的距离的比例，反映了市场的多空力量对比。
        如果 BOP>0，则多头更占优势；BOP<0 则说明空头更占优势。BOP
        越大，则说明价格被往最高价的方向推动得越多；BOP 越小，则说
        明价格被往最低价的方向推动得越多。我们可以用 BOP 上穿/下穿 0
        线来产生买入/卖出信号。
        """
        df['co'] = df['close'] - df['open']  # CLOSE-OPEN
        df['hl'] = df['high'] - df['low']  # HIGH-LOW
        df['BOP'] = (df['co'] / df['hl']).rolling(n, min_periods=1).mean()  # BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)

        df['前%dhBOP' % n] = df['BOP'].shift(1)
        extra_agg_dict['前%dhBOP' % n] = 'first'
        # 删除中间过程数据
        del df['co']
        del df['hl']
        del df['BOP']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhBOP' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # HULLMA 指标
    for n in back_hour_list:
        """
        N=20,80
        X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        HULLMA=EMA(X,[√𝑁])
        HULLMA 也是均线的一种，相比于普通均线有着更低的延迟性。我们
        用短期均线上/下穿长期均线来产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,[N/2])
        ema2 = df['close'].ewm(n * 2, adjust=False).mean()  # EMA(CLOSE,N)
        df['X'] = 2 * ema1 - ema2  # X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        df['HULLMA'] = df['X'].ewm(int(np.sqrt(2 * n)), adjust=False).mean()  # HULLMA=EMA(X,[√𝑁])
        # 去量纲
        df['前%dhHULLMA' % n] = df['HULLMA'].shift(1) - 1
        extra_agg_dict['前%dhHULLMA' % n] = 'first'
        # 删除过程数据
        del df['X']
        del df['HULLMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhHULLMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # COPP 指标
    for n in back_hour_list:
        """
        RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        COPP=WMA(RC,M)
        COPP 指标用不同时间长度的价格变化率的加权移动平均值来衡量
        动量。如果 COPP 上穿/下穿 0 则产生买入/卖出信号。
        """
        df['RC'] = 100 * ((df['close'] - df['close'].shift(n)) / df['close'].shift(n) + (
                df['close'] - df['close'].shift(2 * n)) / df['close'].shift(
            2 * n))  # RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        df['COPP'] = df['RC'].rolling(n, min_periods=1).mean()  # COPP=WMA(RC,M)  使用ma代替wma
        df['前%dhCOPP' % n] = df['COPP'].shift(1)
        extra_agg_dict['前%dhCOPP' % n] = 'first'
        # 删除中间过程数据
        del df['RC']
        del df['COPP']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCOPP' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RSIH
    for n in back_hour_list:
        """
        N1=40
        N2=120
        CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
        OSE,1),0)
        RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLO
        SE,1)),N1,1)*100
        RSI_SIGNAL=EMA(RSI,N2)
        RSIH=RSI-RSI_SIGNAL
        RSI 指标的一个缺点波动性太大，为了使其更平滑我们可以对其作移
        动平均处理。类似于由 MACD 产生 MACD_SIGNAL 并取其差得到
        MACD_HISTOGRAM，我们对 RSI 作移动平均得到 RSI_SIGNAL，
        取两者的差得到 RSI HISTOGRAM。当 RSI HISTORGRAM 上穿 0
        时产生买入信号；当 RSI HISTORGRAM 下穿 0 产生卖出信号。
        """
        # CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
        # sma_diff_pos = df['close_diff_pos'].rolling(n, min_periods=1).mean()
        sma_diff_pos = df['close_diff_pos'].ewm(span=n).mean()  # SMA(CLOSE_DIFF_POS,N1,1)
        # abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).rolling(n, min_periods=1).mean()
        # SMA(ABS(CLOSE-REF(CLOSE,1)),N1,1
        abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).ewm(span=n).mean()
        # RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N1,1)*100
        df['RSI'] = sma_diff_pos / abs_sma_diff_pos * 100
        # RSI_SIGNAL=EMA(RSI,N2)
        df['RSI_ema'] = df['RSI'].ewm(4 * n, adjust=False).mean()
        # RSIH=RSI-RSI_SIGNAL
        df['RSIH'] = df['RSI'] - df['RSI_ema']

        df['前%dhRSIH' % n] = df['RSIH'].shift(1)
        extra_agg_dict['前%dhRSIH' % n] = 'first'
        # 删除中间过程数据
        del df['close_diff_pos']
        del df['RSI']
        del df['RSI_ema']
        del df['RSIH']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRSIH' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # HLMA 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        HMA=MA(HIGH,N1)
        LMA=MA(LOW,N2)
        HLMA 指标是把普通的移动平均中的收盘价换为最高价和最低价分
        别得到 HMA 和 LMA。当收盘价上穿 HMA/下穿 LMA 时产生买入/卖
        出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean()  # HMA=MA(HIGH,N1)
        lma = df['low'].rolling(n, min_periods=1).mean()  # LMA=MA(LOW,N2)
        df['HLMA'] = hma - lma  # 可自行改造
        df['HLMA_mean'] = df['HLMA'].rolling(n, min_periods=1).mean()

        # 去量纲
        df['前%dhHLMA' % n] = df['HLMA'] / df['HLMA_mean'] - 1
        df['前%dhHLMA' % n] = df['前%dhHLMA' % n].shift(1)
        extra_agg_dict['前%dhHLMA' % n] = 'first'
        # 删除中间过程数据
        del df['HLMA']
        del df['HLMA_mean']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhHLMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # TRIX 指标
    for n in back_hour_list:
        """
        TRIPLE_EMA=EMA(EMA(EMA(CLOSE,N),N),N)
        TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
        TRIX 求价格的三重指数移动平均的变化率。当 TRIX>0 时，当前可
        能处于上涨趋势；当 TRIX<0 时，当前可能处于下跌趋势。TRIX 相
        比于普通移动平均的优点在于它通过三重移动平均去除了一些小的
        趋势和市场的噪音。我们可以通过 TRIX 上穿/下穿 0 线产生买入/卖
        出信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean()  # EMA(EMA(EMA(CLOSE,N),N),N)
        # TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
        df['TRIX'] = (df['ema_ema_ema'] - df['ema_ema_ema'].shift(1)) / df['ema_ema_ema'].shift(1)

        df['前%dhTRIX' % n] = df['TRIX'].shift(1)
        extra_agg_dict['前%dhTRIX' % n] = 'first'
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TRIX']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhTRIX' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # WC 指标
    for n in back_hour_list:
        """
        WC=(HIGH+LOW+2*CLOSE)/4
        N1=20
        N2=40
        EMA1=EMA(WC,N1)
        EMA2=EMA(WC,N2)
        WC 也可以用来代替收盘价构造一些技术指标（不过相对比较少用
        到）。我们这里用 WC 的短期均线和长期均线的交叉来产生交易信号。
        """
        WC = (df['high'] + df['low'] + 2 * df['close']) / 4  # WC=(HIGH+LOW+2*CLOSE)/4
        df['ema1'] = WC.ewm(n, adjust=False).mean()  # EMA1=EMA(WC,N1)
        df['ema2'] = WC.ewm(2 * n, adjust=False).mean()  # EMA2=EMA(WC,N2)
        # 去量纲
        df['前%dhWC' % n] = df['ema1'] / df['ema2'] - 1
        df['前%dhWC' % n] = df['前%dhWC' % n].shift(1)
        extra_agg_dict['前%dhWC' % n] = 'first'
        # 删除中间过程数据
        del df['ema1']
        del df['ema2']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhWC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ADX 指标
    for n in back_hour_list:
        """
        N1=14
        MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
        MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
        XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
        PDM=SUM(XPDM,N1)
        XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
        NDM=SUM(XNDM,N1)
        TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
        TR=SUM(TR,N1)
        DI+=PDM/TR
        DI-=NDM/TR
        ADX 指标计算过程中的 DI+与 DI-指标用相邻两天的最高价之差与最
        低价之差来反映价格的变化趋势。当 DI+上穿 DI-时，产生买入信号；
        当 DI+下穿 DI-时，产生卖出信号。
        """
        # MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
        df['max_high'] = np.where(df['high'] > df['high'].shift(1), df['high'] - df['high'].shift(1), 0)
        # MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
        df['max_low'] = np.where(df['low'].shift(1) > df['low'], df['low'].shift(1) - df['low'], 0)
        # XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
        df['XPDM'] = np.where(df['max_high'] > df['max_low'], df['high'] - df['high'].shift(1), 0)
        # PDM=SUM(XPDM,N1)
        df['PDM'] = df['XPDM'].rolling(n).sum()
        # XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
        df['XNDM'] = np.where(df['max_low'] > df['max_high'], df['low'].shift(1) - df['low'], 0)
        # NDM=SUM(XNDM,N1)
        df['NDM'] = df['XNDM'].rolling(n).sum()
        # ABS(HIGH-LOW)
        df['c1'] = abs(df['high'] - df['low'])
        # ABS(HIGH-CLOSE)
        df['c2'] = abs(df['high'] - df['close'])
        # ABS(LOW-CLOSE)
        df['c3'] = abs(df['low'] - df['close'])
        # TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
        # TR=SUM(TR,N1)
        df['TR_sum'] = df['TR'].rolling(n).sum()
        # DI+=PDM/TR
        df['DI+'] = df['PDM'] / df['TR']
        # DI-=NDM/TR
        df['DI-'] = df['NDM'] / df['TR']

        df['前%dhADX_DI+' % n] = df['DI+'].shift(1)
        df['前%dhADX_DI-' % n] = df['DI-'].shift(1)
        # 去量纲
        df['ADX'] = (df['PDM'] + df['NDM']) / df['TR']

        df['前%dhADX' % n] = df['ADX'].shift(1)
        extra_agg_dict['前%dhADX' % n] = 'first'
        extra_agg_dict['前%dhADX_DI+' % n] = 'first'
        extra_agg_dict['前%dhADX_DI-' % n] = 'first'
        # 删除中间过程数据
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
        del df['DI+']
        del df['DI-']
        del df['ADX']
        for _ in ['前%dhADX' % n, '前%dhADX_DI+' % n, '前%dhADX_DI-' % n]:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')

    # FISHER指标
    for n in back_hour_list:
        """
        N=20
        PARAM=0.3
        PRICE=(HIGH+LOW)/2
        PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-
        0.5)
        PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99 
        PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        PRICE_CHANGE=PARAM*PRICE_CH+(1-PARAM)*REF(PRICE_CHANGE,1)
        FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        PRICE_CH 用来衡量当前价位于过去 N 天的最高价和最低价之间的
        位置。Fisher Transformation 是一个可以把股价数据变为类似于正态
        分布的方法。Fisher 指标的优点是减少了普通技术指标的滞后性。
        """
        PARAM = 1 / n
        df['price'] = (df['high'] + df['low']) / 2  # PRICE=(HIGH+LOW)/2
        df['min_low'] = df['low'].rolling(n).min()  # MIN(LOW,N)
        df['max_high'] = df['high'].rolling(n).max()  # MAX(HIGH,N)
        df['price_ch'] = 2 * (df['price'] - df['min_low']) / (
                    df['max_high'] - df['low']) - 0.5  # PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-0.5)
        df['price_change'] = PARAM * df['price_ch'] + (1 - PARAM) * df['price_ch'].shift(1)
        df['price_change'] = np.where(df['price_change'] > 0.99, 0.999,
                                      df['price_change'])  # PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99
        df['price_change'] = np.where(df['price_change'] < -0.99, -0.999,
                                      df['price_change'])  # PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        # 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        df['FISHER'] = 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        # FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        df['FISHER'] = 0.5 * df['FISHER'].shift(1) + 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))

        df['前%dhFISHER' % n] = df['FISHER'].shift(1)
        extra_agg_dict['前%dhFISHER' % n] = 'first'
        # 删除中间数据
        del df['price']
        del df['min_low']
        del df['max_high']
        del df['price_ch']
        del df['price_change']
        del df['FISHER']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhFISHER' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # Demakder 指标
    for n in back_hour_list:
        """
        N=20
        Demax=HIGH-REF(HIGH,1)
        Demax=IF(Demax>0,Demax,0)
        Demin=REF(LOW,1)-LOW
        Demin=IF(Demin>0,Demin,0)
        Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        当 Demaker>0.7 时上升趋势强烈，当 Demaker<0.3 时下跌趋势强烈。
        当 Demaker 上穿 0.7/下穿 0.3 时产生买入/卖出信号。
        """
        df['Demax'] = df['high'] - df['high'].shift(1)  # Demax=HIGH-REF(HIGH,1)
        df['Demax'] = np.where(df['Demax'] > 0, df['Demax'], 0)  # Demax=IF(Demax>0,Demax,0)
        df['Demin'] = df['low'].shift(1) - df['low']  # Demin=REF(LOW,1)-LOW
        df['Demin'] = np.where(df['Demin'] > 0, df['Demin'], 0)  # Demin=IF(Demin>0,Demin,0)
        df['Demax_ma'] = df['Demax'].rolling(n, min_periods=1).mean()  # MA(Demax,N)
        df['Demin_ma'] = df['Demin'].rolling(n, min_periods=1).mean()  # MA(Demin,N)
        df['Demaker'] = df['Demax_ma'] / (
                    df['Demax_ma'] + df['Demin_ma'])  # Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        df['前%dhDemaker' % n] = df['Demaker'].shift(1)
        extra_agg_dict['前%dhDemaker' % n] = 'first'
        # 删除中间过程数据
        del df['Demax']
        del df['Demin']
        del df['Demax_ma']
        del df['Demin_ma']
        del df['Demaker']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDemaker' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # IC 指标
    for n in back_hour_list:
        """
        N1=9
        N2=26
        N3=52
        TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        SPAN_A=(TS+KS)/2
        SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2
        在 IC 指标中，SPAN_A 与 SPAN_B 之间的部分称为云。如果价格在
        云上，则说明是上涨趋势（如果 SPAN_A>SPAN_B，则上涨趋势强
        烈；否则上涨趋势较弱）；如果价格在云下，则为下跌趋势（如果
        SPAN_A<SPAN_B，则下跌趋势强烈；否则下跌趋势较弱）。该指
        标的使用方式与移动平均线有许多相似之处，比如较快的线（TS）突
        破较慢的线（KS），价格突破 KS,价格突破云，SPAN_A 突破 SPAN_B
        等。我们产生信号的方式是：如果价格在云上方 SPAN_A>SPAN_B，
        则当价格上穿 KS 时买入；如果价格在云下方且 SPAN_A<SPAN_B，
        则当价格下穿 KS 时卖出。
        """
        n2 = 3 * n
        n3 = 2 * n2
        df['max_high_1'] = df['high'].rolling(n, min_periods=1).max()  # MAX(HIGH,N1)
        df['min_low_1'] = df['low'].rolling(n, min_periods=1).min()  # MIN(LOW,N1)
        df['TS'] = (df['max_high_1'] + df['min_low_1']) / 2  # TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['max_high_2'] = df['high'].rolling(n2, min_periods=1).max()  # MAX(HIGH,N2)
        df['min_low_2'] = df['low'].rolling(n2, min_periods=1).min()  # MIN(LOW,N2)
        df['KS'] = (df['max_high_2'] + df['min_low_2']) / 2  # KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        df['span_A'] = (df['TS'] + df['KS']) / 2  # SPAN_A=(TS+KS)/2
        df['max_high_3'] = df['high'].rolling(n3, min_periods=1).max()  # MAX(HIGH,N3)
        df['min_low_3'] = df['low'].rolling(n3, min_periods=1).min()  # MIN(LOW,N3)
        df['span_B'] = (df['max_high_3'] + df['min_low_3']) / 2  # SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2

        # 去量纲
        df['前%dhIC' % n] = df['span_A'] / df['span_B']
        df['前%dhIC' % n] = df['前%dhIC' % n].shift(1)
        extra_agg_dict['前%dhIC' % n] = 'first'
        # 删除中间过程数据
        del df['max_high_1']
        del df['max_high_2']
        del df['max_high_3']
        del df['min_low_1']
        del df['min_low_2']
        del df['min_low_3']
        del df['TS']
        del df['KS']
        del df['span_A']
        del df['span_B']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhIC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # TSI 指标
    for n in back_hour_list:
        """
        N1=25
        N2=13
        TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(
        CLOSE-REF(CLOSE,1)),N1),N2)*100
        TSI 是一种双重移动平均指标。与常用的移动平均指标对收盘价取移
        动平均不同，TSI 对两天收盘价的差值取移动平均。如果 TSI 上穿 10/
        下穿-10 则产生买入/卖出指标。
        """
        n1 = 2 * n
        df['diff_close'] = df['close'] - df['close'].shift(1)  # CLOSE-REF(CLOSE,1)
        df['ema'] = df['diff_close'].ewm(n1, adjust=False).mean()  # EMA(CLOSE-REF(CLOSE,1),N1)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)

        df['abs_diff_close'] = abs(df['diff_close'])  # ABS(CLOSE-REF(CLOSE,1))
        df['abs_ema'] = df['abs_diff_close'].ewm(n1, adjust=False).mean()  # EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        df['abs_ema_ema'] = df['abs_ema'].ewm(n, adjust=False).mean()  # EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        # TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1),N2)*100
        df['TSI'] = df['ema_ema'] / df['abs_ema_ema'] * 100

        df['前%dhTSI' % n] = df['TSI'].shift(1)
        extra_agg_dict['前%dhTSI' % n] = 'first'
        # 删除中间过程数据
        del df['diff_close']
        del df['ema']
        del df['ema_ema']
        del df['abs_diff_close']
        del df['abs_ema']
        del df['abs_ema_ema']
        del df['TSI']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhTSI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # LMA 指标
    for n in back_hour_list:
        """
        N=20
        LMA=MA(LOW,N)
        LMA 为简单移动平均把收盘价替换为最低价。如果最低价上穿/下穿
        LMA 则产生买入/卖出信号。
        """
        df['low_ma'] = df['low'].rolling(n, min_periods=1).mean()  # LMA=MA(LOW,N)
        # 进行去量纲
        df['前%dhLMA' % n] = df['low'] / df['low_ma'] - 1
        df['前%dhLMA' % n] = df['前%dhLMA' % n].shift(1)
        extra_agg_dict['前%dhLMA' % n] = 'first'
        # 删除中间过程数据
        del df['low_ma']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhLMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # IMI 指标
    for n in back_hour_list:
        """
        N=14
        INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        IMI=INC/(INC+DEC)
        IMI 的计算方法与 RSI 很相似。其区别在于，在 IMI 计算过程中使用
        的是收盘价和开盘价，而 RSI 使用的是收盘价和前一天的收盘价。所
        以，RSI 做的是前后两天的比较，而 IMI 做的是同一个交易日内的比
        较。如果 IMI 上穿 80，则产生买入信号；如果 IMI 下穿 20，则产生
        卖出信号。
        """
        df['INC'] = np.where(df['close'] > df['open'], df['close'] - df['open'], 0)  # IF(CLOSE>OPEN,CLOSE-OPEN,0)
        df['INC_sum'] = df['INC'].rolling(n).sum()  # INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        df['DEC'] = np.where(df['open'] > df['close'], df['open'] - df['close'], 0)  # IF(OPEN>CLOSE,OPEN-CLOSE,0)
        df['DEC_sum'] = df['DEC'].rolling(n).sum()  # DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        df['IMI'] = df['INC_sum'] / (df['INC_sum'] + df['DEC_sum'])  # IMI=INC/(INC+DEC)

        df['前%dhIMI' % n] = df['IMI'].shift(1)
        extra_agg_dict['前%dhIMI' % n] = 'first'
        # 删除中间过程数据
        del df['INC']
        del df['INC_sum']
        del df['DEC']
        del df['DEC_sum']
        del df['IMI']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhIMI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # VI 指标
    for n in back_hour_list:
        """
        TR=MAX([ABS(HIGH-LOW),ABS(LOW-REF(CLOSE,1)),ABS(HIG
        H-REF(CLOSE,1))])
        VMPOS=ABS(HIGH-REF(LOW,1))
        VMNEG=ABS(LOW-REF(HIGH,1))
        N=40
        SUMPOS=SUM(VMPOS,N)
        SUMNEG=SUM(VMNEG,N)
        TRSUM=SUM(TR,N)
        VI+=SUMPOS/TRSUM
        VI-=SUMNEG/TRSUM
        VI 指标可看成 ADX 指标的变形。VI 指标中的 VI+与 VI-与 ADX 中的
        DI+与 DI-类似。不同的是 ADX 中用当前高价与前一天高价的差和当
        前低价与前一天低价的差来衡量价格变化，而 VI 指标用当前当前高
        价与前一天低价和当前低价与前一天高价的差来衡量价格变化。当
        VI+上穿/下穿 VI-时，多/空的信号更强，产生买入/卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low'])  # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1))  # ABS(LOW-REF(CLOSE,1)
        df['c3'] = abs(df['high'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(
            axis=1)  # TR=MAX([ABS(HIGH-LOW),ABS(LOW-REF(CLOSE,1)),ABS(HIGH-REF(CLOSE,1))])

        df['VMPOS'] = abs(df['high'] - df['low'].shift(1))  # VMPOS=ABS(HIGH-REF(LOW,1))
        df['VMNEG'] = abs(df['low'] - df['high'].shift(1))  # VMNEG=ABS(LOW-REF(HIGH,1))
        df['sum_pos'] = df['VMPOS'].rolling(n).sum()  # SUMPOS=SUM(VMPOS,N)
        df['sum_neg'] = df['VMNEG'].rolling(n).sum()  # SUMNEG=SUM(VMNEG,N)

        df['sum_tr'] = df['TR'].rolling(n).sum()  # TRSUM=SUM(TR,N)
        df['VI+'] = df['sum_pos'] / df['sum_tr']  # VI+=SUMPOS/TRSUM
        df['VI-'] = df['sum_neg'] / df['sum_tr']  # VI-=SUMNEG/TRSUM
        df['前%dhVI+' % n] = df['VI+'].shift(1)
        df['前%dhVI-' % n] = df['VI-'].shift(1)
        extra_agg_dict['前%dhVI+' % n] = 'first'
        extra_agg_dict['前%dhVI-' % n] = 'first'
        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['VMPOS']
        del df['VMNEG']
        del df['sum_pos']
        del df['sum_neg']
        del df['sum_tr']
        del df['VI+']
        del df['VI-']
        for _ in ['前%dhVI+' % n, '前%dhVI-' % n]:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_,  _agg_dict=extra_agg_dict, _agg_type='first')

    # RWI 指标
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(
        CLOSE,1)-LOW))
        ATR=MA(TR,N)
        RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        RWI（随机漫步指标）对一段时间股票的随机漫步区间与真实运动区
        间进行比较以判断股票价格的走势。
        如果 RWIH>1，说明股价长期是上涨趋势，则产生买入信号；
        如果 RWIL>1，说明股价长期是下跌趋势，则产生卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low'])  # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1))
        df['c3'] = abs(df['high'] - df['close'].shift(1))  # ABS(REF(CLOSE,1)-LOW)
        df['TR'] = df[['c1', 'c2', 'c3']].max(
            axis=1)  # TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-LOW))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()  # ATR=MA(TR,N)
        df['RWIH'] = (df['high'] - df['low'].shift(1)) / (df['ATR'] * np.sqrt(n))  # RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        df['RWIL'] = (df['high'].shift(1) - df['low']) / (df['ATR'] * np.sqrt(n))  # RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        df['前%dhRWIH' % n] = df['RWIH'].shift(1)
        df['前%dhRWIL' % n] = df['RWIL'].shift(1)
        extra_agg_dict['前%dhRWIH' % n] = 'first'
        extra_agg_dict['前%dhRWIL' % n] = 'first'
        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['RWIH']
        del df['RWIL']
        for _ in ['前%dhRWIH' % n, '前%dhRWIL' % n]:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')

    # CMO 指标
    for n in back_hour_list:
        """
        N=20
        SU=SUM(MAX(CLOSE-REF(CLOSE,1),0),N)
        SD=SUM(MAX(REF(CLOSE,1)-CLOSE,0),N)
        CMO=(SU-SD)/(SU+SD)*100
        CMO指标用过去N天的价格上涨量和价格下跌量得到，可以看作RSI
        指标的变形。CMO>(<)0 表示当前处于上涨（下跌）趋势，CMO 越
        大（小）则当前上涨（下跌）趋势越强。我们用 CMO 上穿 30/下穿-30
        来产生买入/卖出信号。
        """
        # MAX(CLOSE-REF(CLOSE,1), 0
        df['max_su'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)

        df['sum_su'] = df['max_su'].rolling(n).sum()  # SU=SUM(MAX(CLOSE-REF(CLOSE,1),0),N)
        # MAX(REF(CLOSE,1)-CLOSE,0)
        df['max_sd'] = np.where(df['close'].shift(1) > df['close'], df['close'].shift(1) - df['close'], 0)
        # SD=SUM(MAX(REF(CLOSE,1)-CLOSE,0),N)
        df['sum_sd'] = df['max_su'].rolling(n).sum()
        # CMO=(SU-SD)/(SU+SD)*100
        df['CMO'] = (df['sum_su'] - df['sum_sd']) / (df['sum_su'] + df['sum_sd']) * 100

        df['前%dhCMO' % n] = df['CMO'].shift(1)
        extra_agg_dict['前%dhCMO' % n] = 'first'
        # 删除中间过程数据
        del df['max_su']
        del df['sum_su']
        del df['max_sd']
        del df['sum_sd']
        del df['CMO']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCMO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # OSC 指标
    for n in back_hour_list:
        """
        N=40
        M=20
        OSC=CLOSE-MA(CLOSE,N)
        OSCMA=MA(OSC,M)
        OSC 反映收盘价与收盘价移动平均相差的程度。如果 OSC 上穿/下 穿 OSCMA 则产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,N)
        df['OSC'] = df['close'] - df['ma']  # OSC=CLOSE-MA(CLOSE,N)
        df['OSCMA'] = df['OSC'].rolling(n, min_periods=1).mean()  # OSCMA=MA(OSC,M)
        df['前%dhOSC' % n] = df['OSCMA'].shift(1)
        extra_agg_dict['前%dhOSC' % n] = 'first'
        # 删除中间过程数据
        del df['ma']
        del df['OSC']
        del df['OSCMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhOSC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # CLV 指标
    for n in back_hour_list:
        """
        N=60
        CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        CLVMA=MA(CLV,N)
        CLV 用来衡量收盘价在最低价和最高价之间的位置。当
        CLOSE=HIGH 时，CLV=1;当 CLOSE=LOW 时，CLV=-1;当 CLOSE
        位于 HIGH 和 LOW 的中点时，CLV=0。CLV>0（<0），说明收盘价
        离最高（低）价更近。我们用 CLVMA 上穿/下穿 0 来产生买入/卖出
        信号。
        """
        # CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        df['CLV'] = (2 * df['close'] - df['low'] - df['high']) / (df['high'] - df['low'])
        df['CLVMA'] = df['CLV'].rolling(n, min_periods=1).mean()  # CLVMA=MA(CLV,N)
        df['前%dhCLV' % n] = df['CLVMA'].shift(1)
        extra_agg_dict['前%dhCLV' % n] = 'first'
        # 删除中间过程数据
        del df['CLV']
        del df['CLVMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCLV' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    #  WAD 指标
    for n in back_hour_list:
        """
        TRH=MAX(HIGH,REF(CLOSE,1))
        TRL=MIN(LOW,REF(CLOSE,1))
        AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH) 
        AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))  # 该指标怀疑有误
        WAD=CUMSUM(AD)
        N=20
        WADMA=MA(WAD,N)
        我们用 WAD 上穿/下穿其均线来产生买入/卖出信号。
        """
        df['ref_close'] = df['close'].shift(1)  # REF(CLOSE,1)
        df['TRH'] = df[['high', 'ref_close']].max(axis=1)  # TRH=MAX(HIGH,REF(CLOSE,1))
        df['TRL'] = df[['low', 'ref_close']].min(axis=1)  # TRL=MIN(LOW,REF(CLOSE,1))
        # AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH)
        df['AD'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['TRL'], df['close'] - df['TRH'])
        # AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))
        df['AD'] = np.where(df['close'] > df['close'].shift(1), 0, df['close'] - df['close'].shift(1))
        # WAD=CUMSUM(AD)
        df['WAD'] = df['AD'].cumsum()
        # WADMA=MA(WAD,N)
        df['WADMA'] = df['WAD'].rolling(n, min_periods=1).mean()
        # 去量纲
        df['前%dhWAD' % n] = df['WAD'] / df['WADMA'] - 1
        df['前%dhWAD' % n] = df['前%dhWAD' % n].shift(1)
        extra_agg_dict['前%dhWAD' % n] = 'first'
        # 删除中间过程数据
        del df['ref_close']
        del df['TRH']
        del df['AD']
        del df['WAD']
        del df['WADMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhWAD' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # BIAS36
    for n in back_hour_list:
        """
        N=6
        BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        MABIAS36=MA(BIAS36,N)
        类似于乖离用来衡量当前价格与移动平均价的差距，三六乖离用来衡
        量不同的移动平均价间的差距。当三六乖离上穿/下穿其均线时，产生
        买入/卖出信号。
        """
        df['ma3'] = df['close'].rolling(n, min_periods=1).mean()  # MA(CLOSE,3)
        df['ma6'] = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,6)
        df['BIAS36'] = df['ma3'] - df['ma6']  # BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        df['MABIAS36'] = df['BIAS36'].rolling(2 * n, min_periods=1).mean()  # MABIAS36=MA(BIAS36,N)
        # 去量纲
        df['前%dhBIAS36' % n] = df['BIAS36'] / df['MABIAS36']
        df['前%dhBIAS36' % n] = df['前%dhBIAS36' % n].shift(1)
        extra_agg_dict['前%dhBIAS36' % n] = 'first'
        # 删除中间过程数据
        del df['ma3']
        del df['ma6']
        del df['BIAS36']
        del df['MABIAS36']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhBIAS36' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # TEMA 指标
    for n in back_hour_list:
        """
        N=20,40
        TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        TEMA 结合了单重、双重和三重的 EMA，相比于一般均线延迟性较
        低。我们用快、慢 TEMA 的交叉来产生交易信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean()  # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean()  # EMA(EMA(EMA(CLOSE,N),N),N)
        df['TEMA'] = 3 * df['ema'] - 3 * df['ema_ema'] + df[
            'ema_ema_ema']  # TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        # 去量纲
        df['前%dhTEMA' % n] = df['ema'] / df['TEMA'] - 1
        df['前%dhTEMA' % n] = df['前%dhTEMA' % n].shift(1)
        extra_agg_dict['前%dhTEMA' % n] = 'first'
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TEMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhTEMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # REG 指标
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """

        # df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
        # df['reg'] = df['close'] / df['ref_close'] - 1

        # sklearn 线性回归
        def reg_ols(_y):
            _x = np.arange(n) + 1
            model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
            y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
            return y_pred[-1]

        df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y))  # 求数据拟合的线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1

        df['前%dhREG' % n] = df['reg'].shift(1)
        extra_agg_dict['前%dhREG' % n] = 'first'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhREG' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PSY 指标
    for n in back_hour_list:
        """
        N=12
        PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        PSY 指标为过去 N 天股价上涨的天数的比例*100，用来衡量投资者
        心理和市场的人气。当 PSY 处于 40 和 60 之间时，多、空力量相对
        平衡，当 PSY 上穿 60 时，多头力量比较强，产生买入信号；当 PSY
        下穿 40 时，空头力量比较强，产生卖出信号。
        """
        df['P'] = np.where(df['close'] > df['close'].shift(1), 1, 0)  # IF(CLOSE>REF(CLOSE,1),1,0)

        df['PSY'] = df['P'] / n * 100  # PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        df['前%dhPSY' % n] = df['PSY'].shift(1)
        extra_agg_dict['前%dhPSY' % n] = 'first'
        # 删除中间过程数据
        del df['P']
        del df['PSY']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPSY' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # DMA 指标
    for n in back_hour_list:
        """
        DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        AMA=MA(DMA,N1)
        DMA 衡量快速移动平均与慢速移动平均之差。用 DMA 上穿/下穿其
        均线产生买入/卖出信号。
        """
        df['ma1'] = df['close'].rolling(n, min_periods=1).mean()  # MA(CLOSE,N1)
        df['ma2'] = df['close'].rolling(n * 3, min_periods=1).mean()  # MA(CLOSE,N2)
        df['DMA'] = df['ma1'] - df['ma2']  # DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        df['AMA'] = df['DMA'].rolling(n, min_periods=1).mean()  # AMA=MA(DMA,N1)
        # 去量纲
        df['前%dhDMA' % n] = df['DMA'] / df['AMA'] - 1
        df['前%dhDMA' % n] = df['前%dhDMA' % n].shift(1)
        extra_agg_dict['前%dhDMA' % n] = 'first'
        # 删除中间过程数据
        del df['ma1']
        del df['ma2']
        del df['DMA']
        del df['AMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # KST 指标
    for n in back_hour_list:
        """
        ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        ROC_MA2=MA(CLOSE -REF(CLOSE,15),10)
        ROC_MA3=MA(CLOSE -REF(CLOSE,20),10)
        ROC_MA4=MA(CLOSE -REF(CLOSE,30),10)
        KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        KST=MA(KST_IND,9)
        KST 结合了不同时间长度的 ROC 指标。如果 KST 上穿/下穿 0 则产
        生买入/卖出信号。
        """
        df['ROC1'] = df['close'] - df['close'].shift(n)  # CLOSE-REF(CLOSE,10)
        df['ROC_MA1'] = df['ROC1'].rolling(n, min_periods=1).mean()  # ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        df['ROC2'] = df['close'] - df['close'].shift(int(n * 1.5))
        df['ROC_MA2'] = df['ROC2'].rolling(n, min_periods=1).mean()
        df['ROC3'] = df['close'] - df['close'].shift(int(n * 2))
        df['ROC_MA3'] = df['ROC3'].rolling(n, min_periods=1).mean()
        df['ROC4'] = df['close'] - df['close'].shift(int(n * 3))
        df['ROC_MA4'] = df['ROC4'].rolling(n, min_periods=1).mean()
        # KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        df['KST_IND'] = df['ROC_MA1'] + df['ROC_MA2'] * 2 + df['ROC_MA3'] * 3 + df['ROC_MA4'] * 4
        # KST=MA(KST_IND,9)
        df['KST'] = df['KST_IND'].rolling(n, min_periods=1).mean()
        # 去量纲
        df['前%dhKST' % n] = df['KST_IND'] / df['KST'] - 1
        df['前%dhKST' % n] = df['前%dhKST' % n].shift(1)
        extra_agg_dict['前%dhKST' % n] = 'first'
        # 删除中间过程数据
        del df['ROC1']
        del df['ROC2']
        del df['ROC3']
        del df['ROC4']
        del df['ROC_MA1']
        del df['ROC_MA2']
        del df['ROC_MA3']
        del df['ROC_MA4']
        del df['KST_IND']
        del df['KST']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhKST' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # MICD 指标
    for n in back_hour_list:
        """
        N=20
        N1=10
        N2=20
        M=10
        MI=CLOSE-REF(CLOSE,1)
        MTMMA=SMA(MI,N,1)
        DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        MICD=SMA(DIF,M,1)
        如果 MICD 上穿 0，则产生买入信号；
        如果 MICD 下穿 0，则产生卖出信号。
        """
        df['MI'] = df['close'] - df['close'].shift(1)  # MI=CLOSE-REF(CLOSE,1)
        # df['MIMMA'] = df['MI'].rolling(n, min_periods=1).mean()
        df['MIMMA'] = df['MI'].ewm(span=n).mean()  # MTMMA=SMA(MI,N,1)
        df['MIMMA_MA1'] = df['MIMMA'].shift(1).rolling(n, min_periods=1).mean()  # MA(REF(MTMMA,1),N1)
        df['MIMMA_MA2'] = df['MIMMA'].shift(1).rolling(2 * n, min_periods=1).mean()  # MA(REF(MTMMA,1),N2)
        df['DIF'] = df['MIMMA_MA1'] - df['MIMMA_MA2']  # DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        # df['MICD'] = df['DIF'].rolling(n, min_periods=1).mean()
        df['MICD'] = df['DIF'].ewm(span=n).mean()
        # 去量纲
        df['前%dhMICD' % n] = df['DIF'] / df['MICD']
        df['前%dhMICD' % n] = df['前%dhMICD' % n].shift(1)
        extra_agg_dict['前%dhMICD' % n] = 'first'
        # 删除中间过渡数据
        del df['MI']
        del df['MIMMA']
        del df['MIMMA_MA1']
        del df['MIMMA_MA2']
        del df['DIF']
        del df['MICD']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhMICD' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PMO 指标
    for n in back_hour_list:
        """
        N1=10
        N2=40
        N3=20
        ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        ROC_MA=DMA(ROC,2/N1)
        ROC_MA10=ROC_MA*10
        PMO=DMA(ROC_MA10,2/N2)
        PMO_SIGNAL=DMA(PMO,2/(N3+1))
        PMO 指标是 ROC 指标的双重平滑（移动平均）版本。与 SROC 不 同(SROC 是先对价格作平滑再求 ROC)，而 PMO 是先求 ROC 再对
        ROC 作平滑处理。PMO 越大（大于 0），则说明市场上涨趋势越强；
        PMO 越小（小于 0），则说明市场下跌趋势越强。如果 PMO 上穿/
        下穿其信号线，则产生买入/卖出指标。
        """
        df['ROC'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(
            1) * 100  # ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        df['ROC_MA'] = df['ROC'].rolling(n, min_periods=1).mean()  # ROC_MA=DMA(ROC,2/N1)
        df['ROC_MA10'] = df['ROC_MA'] * 10  # ROC_MA10=ROC_MA*10
        df['PMO'] = df['ROC_MA10'].rolling(4 * n, min_periods=1).mean()  # PMO=DMA(ROC_MA10,2/N2)
        df['PMO_SIGNAL'] = df['PMO'].rolling(2 * n, min_periods=1).mean()  # PMO_SIGNAL=DMA(PMO,2/(N3+1))

        df['前%dhPMO' % n] = df['PMO_SIGNAL'].shift(1)
        extra_agg_dict['前%dhPMO' % n] = 'first'
        # 删除中间过渡数据
        del df['ROC']
        del df['ROC_MA']
        del df['ROC_MA10']
        del df['PMO']
        del df['PMO_SIGNAL']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPMO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RCCD 指标
    for n in back_hour_list:
        """
        M=40
        N1=20
        N2=40
        RC=CLOSE/REF(CLOSE,M)
        ARC1=SMA(REF(RC,1),M,1)
        DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        RCCD=SMA(DIF,M,1)
        RC 指标为当前价格与昨日价格的比值。当 RC 指标>1 时，说明价格在上升；当 RC 指标增大时，说明价格上升速度在增快。当 RC 指标
        <1 时，说明价格在下降；当 RC 指标减小时，说明价格下降速度在增
        快。RCCD 指标先对 RC 指标进行平滑处理，再取不同时间长度的移
        动平均的差值，再取移动平均。如 RCCD 上穿/下穿 0 则产生买入/
        卖出信号。
        """
        df['RC'] = df['close'] / df['close'].shift(2 * n)  # RC=CLOSE/REF(CLOSE,M)
        # df['ARC1'] = df['RC'].rolling(2 * n, min_periods=1).mean()
        df['ARC1'] = df['RC'].ewm(span=2 * n).mean()  # ARC1=SMA(REF(RC,1),M,1)
        df['MA1'] = df['ARC1'].shift(1).rolling(n, min_periods=1).mean()  # MA(REF(ARC1,1),N1)
        df['MA2'] = df['ARC1'].shift(1).rolling(2 * n, min_periods=1).mean()  # MA(REF(ARC1,1),N2)
        df['DIF'] = df['MA1'] - df['MA2']  # DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        # df['RCCD'] = df['DIF'].rolling(2 * n, min_periods=1).mean()
        df['RCCD'] = df['DIF'].ewm(span=2 * n).mean()  # RCCD=SMA(DIF,M,1)

        df['前%dhRCCD' % n] = df['RCCD'].shift(1)
        extra_agg_dict['前%dhRCCD' % n] = 'first'
        # 删除中间数据
        del df['RC']
        del df['ARC1']
        del df['MA1']
        del df['MA2']
        del df['DIF']
        del df['RCCD']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRCCD' % n, _agg_dict=extra_agg_dict, _agg_type='first')

        # KAMA 指标
    for n in back_hour_list:
        """
        N=10
        N1=2
        N2=30
        DIRECTION=CLOSE-REF(CLOSE,N)
        VOLATILITY=SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        ER=DIRETION/VOLATILITY
        FAST=2/(N1+1)
        SLOW=2/(N2+1)
        SMOOTH=ER*(FAST-SLOW)+SLOW
        COF=SMOOTH*SMOOTH
        KAMA=COF*CLOSE+(1-COF)*REF(KAMA,1)
        KAMA 指标与 VIDYA 指标类似，都是把 ER(EfficiencyRatio)指标加
        入到移动平均的权重中，其用法与其他移动平均线类似。在当前趋势
        较强时，ER 值较大，KAMA 会赋予当前价格更大的权重，使得 KAMA
        紧随价格变动，减小其滞后性；在当前趋势较弱（比如振荡市中）,ER
        值较小，KAMA 会赋予当前价格较小的权重，增大 KAMA 的滞后性，
        使其更加平滑，避免产生过多的交易信号。与 VIDYA 指标不同的是，
        KAMA 指标可以设置权值的上界 FAST 和下界 SLOW。
        舍弃
        """
        N = 5 * n
        N2 = 15 * n

        df['DIRECTION'] = df['close'] - df['close'].shift(N)  # DIRECTION=CLOSE-REF(CLOSE,N)
        df['abs_ref'] = abs(df['close'] - df['close'].shift(1))  # ABS(CLOSE-REF(CLOSE,1))
        df['VOLATILITY'] = df['abs_ref'].rolling(N).sum()  # VOLATILITY=SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        df['ER'] = df['DIRECTION'] / df['VOLATILITY']
        fast = 2 / (n + 1)  # FAST=2/(N1+1)
        slow = 2 / (N2 + 1)  # SLOW=2/(N2+1)
        df['SMOOTH'] = df['ER'] * (fast - slow) + slow  # SMOOTH=ER*(FAST-SLOW)+SLOW
        df['COF'] = df['SMOOTH'] * df['SMOOTH']  # COF=SMOOTH*SMOOTH
        # KAMA=COF*CLOSE+(1-COF)*REF(KAMA,1)
        df['KAMA'] = df['COF'] * df['close'] + (1 - df['COF'])
        df['KAMA'] = df['COF'] * df['close'] + (1 - df['COF']) + df['KAMA'].shift(1)
        # 进行归一化
        df['KAMA_min'] = df['KAMA'].rolling(n, min_periods=1).min()
        df['KAMA_max'] = df['KAMA'].rolling(n, min_periods=1).max()
        df['KAMA_norm'] = (df['KAMA'] - df['KAMA_min']) / (df['KAMA_max'] - df['KAMA_min'])

        df['前%dhKAMA' % n] = df['KAMA_norm'].shift(1)
        extra_agg_dict['前%dhKAMA' % n] = 'first'
        # 删除中间过渡数据
        del df['DIRECTION']
        del df['abs_ref']
        del df['VOLATILITY']
        del df['ER']
        del df['SMOOTH']
        del df['COF']
        del df['KAMA']
        del df['KAMA_min']
        del df['KAMA_max']
        del df['KAMA_norm']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhKAMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PPO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        N3=9
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        PPO_SIGNAL=EMA(PPO,N3)
        PPO 是 MACD 的变化率版本。
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)，而
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)。
        PPO 上穿/下穿 PPO_SIGNAL 产生买入/卖出信号。
        """
        #
        N3 = n
        N1 = int(n * 1.382)  # 黄金分割线
        N2 = 3 * n
        df['ema_1'] = df['close'].ewm(N1, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema_2'] = df['close'].ewm(N2, adjust=False).mean()  # EMA(CLOSE,N2)
        df['PPO'] = (df['ema_1'] - df['ema_2']) / df['ema_2']  # PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        df['PPO_SIGNAL'] = df['PPO'].ewm(N3, adjust=False).mean()  # PPO_SIGNAL=EMA(PPO,N3)

        df['前%dhPPO' % n] = df['PPO_SIGNAL'].shift(1)
        extra_agg_dict['前%dhPPO' % n] = 'first'
        # 删除中间数据
        del df['ema_1']
        del df['ema_2']
        del df['PPO']
        del df['PPO_SIGNAL']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPPO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # SMI 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        N3=20
        M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        D=CLOSE-M
        DS=EMA(EMA(D,N2),N2)
        DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        SMI=100*DS/DHL
        SMIMA=MA(SMI,N3)
        SMI 指标可以看作 KDJ 指标的变形。不同的是，KD 指标衡量的是当
        天收盘价位于最近 N 天的最高价和最低价之间的位置，而 SMI 指标
        是衡量当天收盘价与最近 N 天的最高价与最低价均值之间的距离。我
        们用 SMI 指标上穿/下穿其均线产生买入/卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).max()  # MAX(HIGH,N1)
        df['min_low'] = df['low'].rolling(n, min_periods=1).min()  # MIN(LOW,N1)
        df['M'] = (df['max_high'] + df['min_low']) / 2  # M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['D'] = df['close'] - df['M']  # D=CLOSE-M
        df['ema'] = df['D'].ewm(n, adjust=False).mean()  # EMA(D,N2)
        df['DS'] = df['ema'].ewm(n, adjust=False).mean()  # DS=EMA(EMA(D,N2),N2)
        df['HL'] = df['max_high'] - df['min_low']  # MAX(HIGH,N1) - MIN(LOW,N1)
        df['ema_hl'] = df['HL'].ewm(n, adjust=False).mean()  # EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2)
        df['DHL'] = df['ema_hl'].ewm(n, adjust=False).mean()  # DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        df['SMI'] = 100 * df['DS'] / df['DHL']  # SMI=100*DS/DHL
        df['SMIMA'] = df['SMI'].rolling(n, min_periods=1).mean()  # SMIMA=MA(SMI,N3)

        df['前%dhSMI' % n] = df['SMIMA'].shift(1)
        extra_agg_dict['前%dhSMI' % n] = 'first'
        # 删除中间数据
        del df['max_high']
        del df['min_low']
        del df['M']
        del df['D']
        del df['ema']
        del df['DS']
        del df['HL']
        del df['ema_hl']
        del df['DHL']
        del df['SMI']
        del df['SMIMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhSMI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ARBR指标
    for n in back_hour_list:
        """
        AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100
        AR 衡量开盘价在最高价、最低价之间的位置；BR 衡量昨日收盘价在
        今日最高价、最低价之间的位置。AR 为人气指标，用来计算多空双
        方的力量对比。当 AR 值偏低（低于 50）时表示人气非常低迷，股价
        很低，若从 50 下方上穿 50，则说明股价未来可能要上升，低点买入。
        当 AR 值下穿 200 时卖出。
        """
        df['HO'] = df['high'] - df['open']  # (HIGH-OPEN)
        df['OL'] = df['open'] - df['low']  # (OPEN-LOW)
        df['AR'] = df['HO'].rolling(n).sum() / df['OL'].rolling(
            n).sum() * 100  # AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        df['HC'] = df['high'] - df['close'].shift(1)  # (HIGH-REF(CLOSE,1))
        df['CL'] = df['close'].shift(1) - df['low']  # (REF(CLOSE,1)-LOW)
        df['BR'] = df['HC'].rolling(n).sum() / df['CL'].rolling(
            n).sum() * 100  # BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100

        df['前%dhARBR_AR' % n] = df['AR'].shift(1)
        df['前%dhARBR_BR' % n] = df['BR'].shift(1)
        extra_agg_dict['前%dhARBR_AR' % n] = 'first'
        extra_agg_dict['前%dhARBR_BR' % n] = 'first'
        # 删除中间数据
        del df['HO']
        del df['OL']
        del df['AR']
        del df['HC']
        del df['CL']
        del df['BR']
        for _ in ['前%dhARBR_AR' % n, '前%dhARBR_BR' % n]:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')

    # DO 指标
    for n in back_hour_list:
        """
        DO=EMA(EMA(RSI,N),M)
        DO 是平滑处理（双重移动平均）后的 RSI 指标。DO 大于 0 则说明
        市场处于上涨趋势，小于 0 说明市场处于下跌趋势。我们用 DO 上穿
        /下穿其移动平均线来产生买入/卖出信号。
        """
        # 计算RSI
        # 以下为基础策略分享会代码
        # diff = df['close'].diff()
        # df['up'] = np.where(diff > 0, diff, 0)
        # df['down'] = np.where(diff < 0, abs(diff), 0)
        # A = df['up'].rolling(n).sum()
        # B = df['down'].rolling(n).sum()
        # df['rsi'] = A / (A + B)
        diff = df['close'].diff()  # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0)  # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff),
                              0)  # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()  # SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean()  # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df['rsi'] = A / (A + B)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df['ema_rsi'] = df['rsi'].ewm(n, adjust=False).mean()  # EMA(RSI,N)
        df['DO'] = df['ema_rsi'].ewm(n, adjust=False).mean()  # DO=EMA(EMA(RSI,N),M)
        df['前%dhDO' % n] = df['DO'].shift(1)
        extra_agg_dict['前%dhDO' % n] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']
        del df['rsi']
        del df['ema_rsi']
        del df['DO']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # SI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+
        0.5*(CLOSE-OPEN))/R*K/M
        SI 用价格变化（即两天收盘价之差，昨日收盘与开盘价之差，今日收
        盘与开盘价之差）的加权平均来反映价格的变化。如果 SI 上穿/下穿
        0 则产生买入/卖出信号。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1))  # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1))  # B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1))  # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1))  # D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1)  # K=MAX(A,B)
        df['M'] = (df['high'] - df['low']).rolling(n).max()  # M=MAX(HIGH-LOW,N)
        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D']  # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D']  # R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D']  # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'],
                            df['R2'])  # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] >= df['A']) & (df['C'] >= df['B']), df['R3'],
                           df['R4'])  # R=IF((C>=A) & (C>=B),R3,R4)
        # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / df['R'] * df['K'] / df['M']
        df['前%dhSI' % n] = df['SI'].shift(1)
        extra_agg_dict['前%dhSI' % n] = 'first'
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['M']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhSI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # DBCD 指标
    for n in back_hour_list:
        """
        N=5
        M=16
        T=17
        BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        BIAS_DIF=BIAS-REF(BIAS,M)
        DBCD=SMA(BIAS_DIFF,T,1)
        DBCD（异同离差乖离率）为乖离率离差的移动平均。我们用 DBCD
        上穿 5%/下穿-5%来产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(n, min_periods=1).mean()  # MA(CLOSE,N)

        df['BIAS'] = (df['close'] - df['ma']) / df['ma'] * 100  # BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        df['BIAS_DIF'] = df['BIAS'] - df['BIAS'].shift(3 * n)  # BIAS_DIF=BIAS-REF(BIAS,M)
        # df['DBCD'] = df['BIAS_DIF'].rolling(3 * n + 2, min_periods=1).mean()
        df['DBCD'] = df['BIAS_DIF'].ewm(span=3 * n).mean()  # DBCD=SMA(BIAS_DIFF,T,1)
        df['前%dhDBCD' % n] = df['DBCD'].shift(1)
        extra_agg_dict['前%dhDBCD' % n] = 'first'
        # 删除中间数据
        del df['ma']
        del df['BIAS']
        del df['BIAS_DIF']
        del df['DBCD']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhDBCD' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # CV 指标
    for n in back_hour_list:
        """
        N=10
        H_L_EMA=EMA(HIGH-LOW,N)
        CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
        CV 指标用来衡量股价的波动，反映一段时间内最高价与最低价之差
        （价格变化幅度）的变化率。如果 CV 的绝对值下穿 30，买入；
        如果 CV 的绝对值上穿 70，卖出。
        """
        df['H_L_ema'] = (df['high'] - df['low']).ewm(n, adjust=False).mean()  # H_L_EMA=EMA(HIGH-LOW,N)
        df['CV'] = (df['H_L_ema'] - df['H_L_ema'].shift(n)) / df['H_L_ema'].shift(
            n) * 100  # CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
        df['前%dhCV' % n] = df['CV'].shift(1)
        extra_agg_dict['前%dhCV' % n] = 'first'
        # 删除中间数据
        del df['H_L_ema']
        del df['CV']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCV' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RMI 指标
    for n in back_hour_list:
        """
        N=7
        RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        RMI 与 RSI 的计算方式类似，将 RSI 中的动量与前一天价格之差
        CLOSE-REF(CLOSE,1)项改为了与前四天价格之差 CLOSEREF(CLOSE,4)
        """
        # MAX(CLOSE-REF(CLOSE,4),0)
        df['max_close'] = np.where(df['close'] > df['close'].shift(4), df['close'] - df['close'].shift(4), 0)
        # ABS(CLOSE-REF(CLOSE,1)
        df['abs_close'] = df['close'] - df['close'].shift(1)

        # df['sma_1'] = df['max_close'].rolling(n, min_periods=1).mean()
        df['sma_1'] = df['max_close'].ewm(span=n).mean()  # SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)
        # df['sma_2'] = df['abs_close'].rolling(n, min_periods=1).mean()
        df['sma_2'] = df['abs_close'].ewm(span=n).mean()  # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['RMI'] = df['sma_1'] / df[
            'sma_2'] * 100  # RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        df['前%dhRMI' % n] = df['RMI'].shift(1)
        extra_agg_dict['前%dhRMI' % n] = 'first'
        # 删除中间数据
        del df['max_close']
        del df['abs_close']
        del df['sma_1']
        del df['sma_2']
        del df['RMI']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRMI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # SKDJ 指标
    for n in back_hour_list:
        """
        N=60
        M=5
        RSV=(CLOSE-MIN(LOW,N))/(MAX(HIGH,N)-MIN(LOW,N))*100
        MARSV=SMA(RSV,3,1)
        K=SMA(MARSV,3,1)
        D=MA(K,3)
        SKDJ 为慢速随机波动（即慢速 KDJ）。SKDJ 中的 K 即 KDJ 中的 D，
        SKJ 中的 D 即 KDJ 中的 D 取移动平均。其用法与 KDJ 相同。
        当 D<40(处于超卖状态)且 K 上穿 D 时买入，当 D>60（处于超买状
        态）K 下穿 D 时卖出。
        """
        # RSV=(CLOSE-MIN(LOW,N))/(MAX(HIGH,N)-MIN(LOW,N))*100
        df['RSV'] = (df['close'] - df['low'].rolling(n, min_periods=1).min()) / (
                df['high'].rolling(n, min_periods=1).max() - df['low'].rolling(n, min_periods=1).min()) * 100
        # MARSV=SMA(RSV,3,1)
        df['MARSV'] = df['RSV'].ewm(com=2).mean()
        # K=SMA(MARSV,3,1)
        df['K'] = df['MARSV'].ewm(com=2).mean()
        # D=MA(K,3)
        df['D'] = df['K'].rolling(3, min_periods=1).mean()
        df['前%dhSKDJ' % n] = df['D'].shift(1)
        extra_agg_dict['前%dhSKDJ' % n] = 'first'
        # 删除中间过渡数据
        del df['RSV']
        del df['MARSV']
        del df['K']
        del df['D']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhSKDJ' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ROC 指标
    for n in back_hour_list:
        """
        ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
        ROC 衡量价格的涨跌幅。ROC 可以用来反映市场的超买超卖状态。
        当 ROC 过高时，市场处于超买状态；当 ROC 过低时，市场处于超
        卖状态。这些情况下，可能会发生反转。
        如果 ROC 上穿 5%，则产生买入信号；
        如果 ROC 下穿-5%，则产生卖出信号。
        """
        # ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
        df['ROC'] = df['close'] / df['close'].shift(n) - 1

        df['前%dhROC' % n] = df['ROC'].shift(1)
        extra_agg_dict['前%dhROC' % n] = 'first'
        del df['ROC']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhROC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # WR 指标
    for n in back_hour_list:
        """
        HIGH(N)=MAX(HIGH,N)
        LOW(N)=MIN(LOW,N)
        WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
        WR 指标事实上就是 100-KDJ 指标计算过程中的 Stochastics。WR
        指标用来衡量市场的强弱和超买超卖状态。一般认为，当 WR 小于
        20 时，市场处于超买状态；当 WR 大于 80 时，市场处于超卖状态；
        当 WR 处于 20 到 80 之间时，多空较为平衡。
        如果 WR 上穿 80，则产生买入信号；
        如果 WR 下穿 20，则产生卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).max()  # HIGH(N)=MAX(HIGH,N)
        df['min_low'] = df['low'].rolling(n, min_periods=1).min()  # LOW(N)=MIN(LOW,N)
        # WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
        df['WR'] = (df['max_high'] - df['close']) / (df['max_high'] - df['min_low']) * 100
        df['前%dhWR' % n] = df['WR'].shift(1)
        extra_agg_dict['前%dhWR' % n] = 'first'
        # 删除中间过渡数据
        del df['max_high']
        del df['min_low']
        del df['WR']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhWR' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # STC 指标
    for n in back_hour_list:
        """
        N1=23
        N2=50
        N=40
        MACDX=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        V1=MIN(MACDX,N)
        V2=MAX(MACDX,N)-V1
        FK=IF(V2>0,(MACDX-V1)/V2*100,REF(FK,1))
        FD=SMA(FK,N,1)
        V3=MIN(FD,N)
        V4=MAX(FD,N)-V3
        SK=IF(V4>0,(FD-V3)/V4*100,REF(SK,1))
        STC=SD=SMA(SK,N,1) 
        STC 指标结合了 MACD 指标和 KDJ 指标的算法。首先用短期均线与
        长期均线之差算出 MACD，再求 MACD 的随机快速随机指标 FK 和
        FD，最后求 MACD 的慢速随机指标 SK 和 SD。其中慢速随机指标就
        是 STC 指标。STC 指标可以用来反映市场的超买超卖状态。一般认
        为 STC 指标超过 75 为超买，STC 指标低于 25 为超卖。
        如果 STC 上穿 25，则产生买入信号；
        如果 STC 下穿 75，则产生卖出信号。
        """
        N1 = n
        N2 = int(N1 * 1.5)  # 大约值
        N = 2 * n
        df['ema1'] = df['close'].ewm(N1, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema2'] = df['close'].ewm(N, adjust=False).mean()  # EMA(CLOSE,N2)
        df['MACDX'] = df['ema1'] - df['ema2']  # MACDX=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['V1'] = df['MACDX'].rolling(N2, min_periods=1).min()  # V1=MIN(MACDX,N)
        df['V2'] = df['MACDX'].rolling(N2, min_periods=1).max() - df['V1']  # V2=MAX(MACDX,N)-V1
        # FK=IF(V2>0,(MACDX-V1)/V2*100,REF(FK,1))
        df['FK'] = (df['MACDX'] - df['V1']) / df['V2'] * 100
        df['FK'] = np.where(df['V2'] > 0, (df['MACDX'] - df['V1']) / df['V2'] * 100, df['FK'].shift(1))

        df['FD'] = df['FK'].rolling(N2, min_periods=1).mean()  # FD=SMA(FK,N,1)  直接使用均线代替sma
        df['V3'] = df['FD'].rolling(N2, min_periods=1).min()  # V3=MIN(FD,N)
        df['V4'] = df['FD'].rolling(N2, min_periods=1).max() - df['V3']  # V4=MAX(FD,N)-V3
        # SK=IF(V4>0,(FD-V3)/V4*100,REF(SK,1))
        df['SK'] = (df['FD'] - df['V3']) / df['V4'] * 100
        df['SK'] = np.where(df['V4'] > 0, (df['FD'] - df['V3']) / df['V4'] * 100, df['SK'].shift(1))
        # STC = SD = SMA(SK, N, 1)
        df['STC'] = df['SK'].rolling(N1, min_periods=1).mean()
        df['前%dhSTC' % n] = df['STC'].shift(1)
        extra_agg_dict['前%dhSTC' % n] = 'first'
        # 删除中间过渡数据
        del df['ema1']
        del df['ema2']
        del df['MACDX']
        del df['V1']
        del df['V2']
        del df['V3']
        del df['V4']
        del df['FK']
        del df['FD']
        del df['SK']
        del df['STC']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhSTC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RVI 指标
    for n in back_hour_list:
        """
        N1=10
        N2=20
        STD=STD(CLOSE,N)
        USTD=SUM(IF(CLOSE>REF(CLOSE,1),STD,0),N2)
        DSTD=SUM(IF(CLOSE<REF(CLOSE,1),STD,0),N2)
        RVI=100*USTD/(USTD+DSTD)
        RVI 的计算方式与 RSI 一样，不同的是将 RSI 计算中的收盘价变化值
        替换为收盘价在过去一段时间的标准差，用来反映一段时间内上升
        的波动率和下降的波动率的对比。我们也可以像计算 RSI 指标时一样
        先对公式中的 USTD 和 DSTD 作移动平均得到 USTD_MA 和
        DSTD_MA 再求出 RVI=100*USTD_MV/(USTD_MV+DSTD_MV)。
        RVI 的用法与 RSI 一样。通常认为当 RVI 大于 70，市场处于强势上
        涨甚至达到超买的状态；当 RVI 小于 30，市场处于强势下跌甚至达
        到超卖的状态。当 RVI 跌到 30 以下又上穿 30 时，通常认为股价要
        从超卖的状态反弹；当 RVI 超过 70 又下穿 70 时，通常认为市场要
        从超买的状态回落了。
        如果 RVI 上穿 30，则产生买入信号；
        如果 RVI 下穿 70，则产生卖出信号。
        """
        df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # STD=STD(CLOSE,N)
        df['ustd'] = np.where(df['close'] > df['close'].shift(1), df['std'], 0)  # IF(CLOSE>REF(CLOSE,1),STD,0)
        df['sum_ustd'] = df['ustd'].rolling(2 * n).sum()  # USTD=SUM(IF(CLOSE>REF(CLOSE,1),STD,0),N2)

        df['dstd'] = np.where(df['close'] < df['close'].shift(1), df['std'], 0)  # IF(CLOSE<REF(CLOSE,1),STD,0)
        df['sum_dstd'] = df['dstd'].rolling(2 * n).sum()  # DSTD=SUM(IF(CLOSE<REF(CLOSE,1),STD,0),N2)

        df['RVI'] = df['sum_ustd'] / (df['sum_ustd'] + df['sum_dstd']) * 100  # RVI=100*USTD/(USTD+DSTD)
        df['前%dhRVI' % n] = df['RVI'].shift(1)
        extra_agg_dict['前%dhRVI' % n] = 'first'
        # 删除中间过渡数据
        del df['std']
        del df['ustd']
        del df['sum_ustd']
        del df['dstd']
        del df['sum_dstd']
        del df['RVI']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRVI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # UOS 指标
    for n in back_hour_list:
        """
        M=7
        N=14
        O=28
        TH=MAX(HIGH,REF(CLOSE,1))
        TL=MIN(LOW,REF(CLOSE,1))
        TR=TH-TL
        XR=CLOSE-TL
        XRM=SUM(XR,M)/SUM(TR,M)
        XRN=SUM(XR,N)/SUM(TR,N)
        XRO=SUM(XR,O)/SUM(TR,O)
        UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
        UOS 的用法与 RSI 指标类似，可以用来反映市场的超买超卖状态。
        一般来说，UOS 低于 30 市场处于超卖状态；UOS 高于 30 市场处于
        超买状态。
        如果 UOS 上穿 30，则产生买入信号；
        如果 UOS 下穿 70，则产生卖出信号。
        """
        # 固定多参数比例倍数
        M = n
        N = 2 * n
        O = 4 * n
        df['ref_close'] = df['close'].shift(1)  # REF(CLOSE,1)
        df['TH'] = df[['high', 'ref_close']].max(axis=1)  # TH=MAX(HIGH,REF(CLOSE,1))
        df['TL'] = df[['low', 'ref_close']].min(axis=1)  # TL=MIN(LOW,REF(CLOSE,1))
        df['TR'] = df['TH'] - df['TL']  # TR=TH-TL
        df['XR'] = df['close'] - df['TL']  # XR=CLOSE-TL
        df['XRM'] = df['XR'].rolling(M).sum() / df['TR'].rolling(M).sum()  # XRM=SUM(XR,M)/SUM(TR,M)
        df['XRN'] = df['XR'].rolling(N).sum() / df['TR'].rolling(N).sum()  # XRN=SUM(XR,N)/SUM(TR,N)
        df['XRO'] = df['XR'].rolling(O).sum() / df['TR'].rolling(O).sum()  # XRO=SUM(XR,O)/SUM(TR,O)
        # UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
        df['UOS'] = 100 * (df['XRM'] * N * O + df['XRN'] * M * O + df['XRO'] * M * N) / (M * N + M * O + N * O)
        df['前%dhUOS' % n] = df['UOS'].shift(1)
        extra_agg_dict['前%dhUOS' % n] = 'first'
        # 删除中间过渡数据
        del df['ref_close']
        del df['TH']
        del df['TL']
        del df['TR']
        del df['XR']
        del df['XRM']
        del df['XRN']
        del df['XRO']
        del df['UOS']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhUOS' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RSIS 指标
    for n in back_hour_list:
        """
        N=120
        M=20
        CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
        OSE,1),0)
        RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOS
        E,1)),N,1)*100
        RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
        RSISMA=EMA(RSIS,M)
        RSIS 反映当前 RSI 在最近 N 天的 RSI 最大值和最小值之间的位置，
        与 KDJ 指标的构造思想类似。由于 RSIS 波动性比较大，我们先取移
        动平均再用其产生信号。其用法与 RSI 指标的用法类似。
        RSISMA 上穿 40 则产生买入信号；
        RSISMA 下穿 60 则产生卖出信号。
        """
        N = 6 * n
        M = n
        # CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
        # df['sma_1'] = df['close_diff_pos'].rolling(N).sum() # SMA(CLOSE_DIFF_POS,N,1)
        df['sma_1'] = df['close_diff_pos'].ewm(span=N).mean()  # SMA(CLOSE_DIFF_POS,N,1)
        # df['sma_2'] = abs(df['close'] - df['close'].shift(1)).rolling(N).sum() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['sma_2'] = abs(df['close'] - df['close'].shift(1)).ewm(span=N).mean()  # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['RSI'] = df['sma_1'] / df['sma_2'] * 100  # RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        # RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
        df['RSIS'] = (df['RSI'] - df['RSI'].rolling(N, min_periods=1).min()) / (
                df['RSI'].rolling(N, min_periods=1).max() - df['RSI'].rolling(N, min_periods=1).min()) * 100
        # RSISMA=EMA(RSIS,M)
        df['RSISMA'] = df['RSIS'].ewm(M, adjust=False).mean()

        df['前%dhRSISMA' % n] = df['RSISMA'].shift(1)
        extra_agg_dict['前%dhRSISMA' % n] = 'first'

        del df['close_diff_pos']
        del df['sma_1']
        del df['sma_2']
        del df['RSI']
        del df['RSIS']
        del df['RSISMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRSISMA' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # MAAMT 指标
    for n in back_hour_list:
        """
        N=40
        MAAMT=MA(AMOUNT,N)
        MAAMT 是成交额的移动平均线。当成交额上穿/下穿移动平均线时产
        生买入/卖出信号。
        """
        df['MAAMT'] = df['volume'].rolling(n, min_periods=1).mean()  # MAAMT=MA(AMOUNT,N)
        df['前%dhMAAMT' % n] = df['MAAMT'].shift(1)
        extra_agg_dict['前%dhMAAMT' % n] = 'first'
        del df['MAAMT']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhMAAMT' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # SROCVOL 指标
    for n in back_hour_list:
        """
        N=20
        M=10
        EMAP=EMA(VOLUME,N)
        SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROCVOL 与 ROCVOL 类似，但是会先对成交量进行移动平均平滑
        处理之后再取其变化率。（SROCVOL 是 SROC 的成交量版本。）
        SROCVOL 上穿 0 买入，下穿 0 卖出。
        """
        df['emap'] = df['volume'].ewm(2 * n, adjust=False).mean()  # EMAP=EMA(VOLUME,N)
        # SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df['SROCVOL'] = (df['emap'] - df['emap'].shift(n)) / df['emap'].shift(n)
        df['前%dhSROCVOL' % n] = df['SROCVOL'].shift(1)
        extra_agg_dict['前%dhSROCVOL' % n] = 'first'
        del df['emap']
        del df['SROCVOL']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhSROCVOL' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PVO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
        PVO 用成交量的指数移动平均来反应成交量的变化。PVO 上穿 0 线
        买入；PVO 下穿 0 线卖出。
        """
        df['emap_1'] = df['volume'].ewm(n, min_periods=1).mean()  # EMA(VOLUME,N1)
        df['emap_2'] = df['volume'].ewm(n * 2, min_periods=1).mean()  # EMA(VOLUME,N2)
        df['PVO'] = (df['emap_1'] - df['emap_2']) / df['emap_2']  # PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
        df['前%dhPVO' % n] = df['PVO'].shift(1)
        extra_agg_dict['前%dhPVO' % n] = 'first'
        # 删除中间过渡数据
        del df['emap_1']
        del df['emap_2']
        del df['PVO']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPVO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # BIASVOL 指标
    for n in back_hour_list:
        """
        N=6，12，24
        BIASVOL(N)=(VOLUME-MA(VOLUME,N))/MA(VOLUME,N)
        BIASVOL 是乖离率 BIAS 指标的成交量版本。如果 BIASVOL6 大于
        5 且 BIASVOL12 大于 7 且 BIASVOL24 大于 11，则产生买入信号；
        如果 BIASVOL6 小于-5 且 BIASVOL12 小于-7 且 BIASVOL24 小于
        -11，则产生卖出信号。
        """
        df['ma_volume'] = df['volume'].rolling(n, min_periods=1).mean()  # MA(VOLUME,N)
        df['BIASVOL'] = (df['volume'] - df['ma_volume']) / df[
            'ma_volume']  # BIASVOL(N)=(VOLUME-MA(VOLUME,N))/MA(VOLUME,N)
        df['前%dhBIASVOL' % n] = df['BIASVOL'].shift(1)
        extra_agg_dict['前%dhBIASVOL' % n] = 'first'
        del df['ma_volume']
        del df['BIASVOL']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhBIASVOL' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # MACDVOL 指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=10
        MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
        SIGNAL=MA(MACDVOL,N3)
        MACDVOL 是 MACD 的成交量版本。如果 MACDVOL 上穿 SIGNAL，
        则买入；下穿 SIGNAL 则卖出。
        """
        N1 = 2 * n
        N2 = 4 * n
        N3 = n
        df['ema_volume_1'] = df['volume'].ewm(N1, adjust=False).mean()  # EMA(VOLUME,N1)
        df['ema_volume_2'] = df['volume'].ewm(N2, adjust=False).mean()  # EMA(VOLUME,N2)
        df['MACDV'] = df['ema_volume_1'] - df['ema_volume_2']  # MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
        df['SIGNAL'] = df['MACDV'].rolling(N3, min_periods=1).mean()  # SIGNAL=MA(MACDVOL,N3)
        # 去量纲
        df['MACDVOL'] = df['MACDV'] / df['SIGNAL'] - 1
        df['前%dhMACDVOL' % n] = df['MACDVOL'].shift(1)
        extra_agg_dict['前%dhMACDVOL' % n] = 'first'
        # 删除中间过程数据
        del df['ema_volume_1']
        del df['ema_volume_2']
        del df['MACDV']
        del df['SIGNAL']
        del df['MACDVOL']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhMACDVOL' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ROCVOL 指标
    for n in back_hour_list:
        """
        N = 80
        ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)
        ROCVOL 是 ROC 的成交量版本。如果 ROCVOL 上穿 0 则买入，下
        穿 0 则卖出。
        """
        df['ROCVOL'] = df['volume'] / df['volume'].shift(n) - 1  # ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)

        df['前%dhROCVOL' % n] = df['ROCVOL'].shift(1)
        extra_agg_dict['前%dhROCVOL' % n] = 'first'
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhROCVOL' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # FI 指标
    for n in back_hour_list:
        """
        N=13
        FI=(CLOSE-REF(CLOSE,1))*VOLUME
        FIMA=EMA(FI,N)
        FI 用价格的变化来衡量价格的趋势，用成交量大小来衡量趋势的强
        弱。我们先对 FI 取移动平均，当均线上穿 0 线时产生买入信号，下
        穿 0 线时产生卖出信号。
        """
        df['FI'] = (df['close'] - df['close'].shift(1)) * df['volume']  # FI=(CLOSE-REF(CLOSE,1))*VOLUME
        df['FIMA'] = df['FI'].ewm(n, adjust=False).mean()  # FIMA=EMA(FI,N)
        # 去量纲
        df['前%dhFI' % n] = df['FI'] / df['FIMA'] - 1
        df['前%dhFI' % n] = df['前%dhFI' % n].shift(1)
        extra_agg_dict['前%dhFI' % n] = 'first'
        # 删除中间过程数据
        del df['FI']
        del df['FIMA']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhFI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PVT 指标
    for n in back_hour_list:
        """
        PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
        PVT_MA1=MA(PVT,N1)
        PVT_MA2=MA(PVT,N2)
        PVT 指标用价格的变化率作为权重求成交量的移动平均。PVT 指标
        与 OBV 指标的思想类似，但与 OBV 指标相比，PVT 考虑了价格不
        同涨跌幅的影响，而 OBV 只考虑了价格的变化方向。我们这里用 PVT
        短期和长期均线的交叉来产生交易信号。
        如果 PVT_MA1 上穿 PVT_MA2，则产生买入信号；
        如果 PVT_MA1 下穿 PVT_MA2，则产生卖出信号。
        """
        # PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
        df['PVT'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * df['volume']
        df['PVT_MA1'] = df['PVT'].rolling(n, min_periods=1).mean()  # PVT_MA1=MA(PVT,N1)
        # df['PVT_MA2'] = df['PVT'].rolling(2 * n, min_periods=1).mean()

        # 去量纲  只引入一个ma做因子
        df['前%dhPVT' % n] = df['PVT'] / df['PVT_MA1'] - 1
        df['前%dhPVT' % n] = df['前%dhPVT' % n].shift(1)
        extra_agg_dict['前%dhPVT' % n] = 'first'
        # 删除中间过程数据
        del df['PVT']
        del df['PVT_MA1']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPVT' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # RSIV 指标
    for n in back_hour_list:
        """
        N=20
        VOLUP=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        VOLDOWN=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        SUMUP=SUM(VOLUP,N)
        SUMDOWN=SUM(VOLDOWN,N)
        RSIV=100*SUMUP/(SUMUP+SUMDOWN)
        RSIV 的计算方式与 RSI 相同，只是把其中的价格变化 CLOSEREF(CLOSE,1)替换成了成交量 VOLUME。用法与 RSI 类似。我们
        这里将其用作动量指标，上穿 60 买入，下穿 40 卖出。
        """
        df['VOLUP'] = np.where(df['close'] > df['close'].shift(1), df['volume'],
                               0)  # VOLUP=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        df['VOLDOWN'] = np.where(df['close'] < df['close'].shift(1), df['volume'],
                                 0)  # VOLDOWN=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['SUMUP'] = df['VOLUP'].rolling(n).sum()  # SUMUP=SUM(VOLUP,N)
        df['SUMDOWN'] = df['VOLDOWN'].rolling(n).sum()  # SUMDOWN=SUM(VOLDOWN,N)
        df['RSIV'] = df['SUMUP'] / (df['SUMUP'] + df['SUMDOWN']) * 100  # RSIV=100*SUMUP/(SUMUP+SUMDOWN)

        df['前%dhRSIV' % n] = df['RSIV'].shift(1)
        extra_agg_dict['前%dhRSIV' % n] = 'first'
        # 删除中间过渡数据
        del df['VOLUP']
        del df['VOLDOWN']
        del df['SUMUP']
        del df['SUMDOWN']
        del df['RSIV']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhRSIV' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # AMV 指标
    for n in back_hour_list:
        """
        N1=13
        N2=34
        AMOV=VOLUME*(OPEN+CLOSE)/2
        AMV1=SUM(AMOV,N1)/SUM(VOLUME,N1)
        AMV2=SUM(AMOV,N2)/SUM(VOLUME,N2)
        AMV 指标用成交量作为权重对开盘价和收盘价的均值进行加权移动
        平均。成交量越大的价格对移动平均结果的影响越大，AMV 指标减
        小了成交量小的价格波动的影响。当短期 AMV 线上穿/下穿长期 AMV
        线时，产生买入/卖出信号。
        """
        df['AMOV'] = df['volume'] * (df['open'] + df['close']) / 2  # AMOV=VOLUME*(OPEN+CLOSE)/2
        df['AMV1'] = df['AMOV'].rolling(n).sum() / df['volume'].rolling(n).sum()  # AMV1=SUM(AMOV,N1)/SUM(VOLUME,N1)
        # df['AMV2'] = df['AMOV'].rolling(n * 3).sum() / df['volume'].rolling(n * 3).sum()
        # 去量纲
        df['AMV'] = (df['AMV1'] - df['AMV1'].rolling(n).min()) / (
                df['AMV1'].rolling(n).max() - df['AMV1'].rolling(n).min())  # 标准化
        df['前%dhAMV' % n] = df['AMV'].shift(1)
        extra_agg_dict['前%dhAMV' % n] = 'first'
        # 删除中间过程数据
        del df['AMOV']
        del df['AMV1']
        del df['AMV']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhAMV' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # VRAMT 指标
    for n in back_hour_list:
        """
        N=40
        AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
        BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
        CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
        AVS=SUM(AV,N)
        BVS=SUM(BV,N)
        CVS=SUM(CV,N)
        VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
        VRAMT 的计算与 VR 指标（Volume Ratio）一样，只是把其中的成
        交量替换成了成交额。
        如果 VRAMT 上穿 180，则产生买入信号；
        如果 VRAMT 下穿 70，则产生卖出信号。
        """
        df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0)  # AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
        df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0)  # BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
        df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0)  # CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
        df['AVS'] = df['AV'].rolling(n).sum()  # AVS=SUM(AV,N)
        df['BVS'] = df['BV'].rolling(n).sum()  # BVS=SUM(BV,N)
        df['CVS'] = df['CV'].rolling(n).sum()  # CVS=SUM(CV,N)
        df['VRAMT'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2)  # VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
        df['前%dhVRAMT' % n] = df['VRAMT'].shift(1)
        extra_agg_dict['前%dhVRAMT' % n] = 'first'
        # 删除中间过程数据
        del df['AV']
        del df['BV']
        del df['CV']
        del df['AVS']
        del df['BVS']
        del df['CVS']
        del df['VRAMT']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhVRAMT' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # WVAD 指标
    for n in back_hour_list:
        """
        N=20
        WVAD=SUM(((CLOSE-OPEN)/(HIGH-LOW)*VOLUME),N)
        WVAD 是用价格信息对成交量加权的价量指标，用来比较开盘到收盘
        期间多空双方的力量。WVAD 的构造与 CMF 类似，但是 CMF 的权
        值用的是 CLV(反映收盘价在最高价、最低价之间的位置)，而 WVAD
        用的是收盘价与开盘价的距离（即蜡烛图的实体部分的长度）占最高
        价与最低价的距离的比例，且没有再除以成交量之和。
        WVAD 上穿 0 线，代表买方力量强；
        WVAD 下穿 0 线，代表卖方力量强。
        """
        # ((CLOSE-OPEN)/(HIGH-LOW)*VOLUME)
        df['VAD'] = (df['close'] - df['open']) / (df['high'] - df['low']) * df['volume']
        df['WVAD'] = df['VAD'].rolling(n).sum()  # WVAD=SUM(((CLOSE-OPEN)/(HIGH-LOW)*VOLUME),N)

        # 标准化
        df['前%dhWVAD' % n] = (df['WVAD'] - df['WVAD'].rolling(n).min()) / (
                df['WVAD'].rolling(n).max() - df['WVAD'].rolling(n).min())
        df['前%dhWVAD' % n] = df['前%dhWVAD' % n].shift(1)
        extra_agg_dict['前%dhWVAD' % n] = 'first'
        del df['VAD']
        del df['WVAD']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhWVAD' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # OBV 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
        VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
        OBV=REF(OBV,1)+VOL
        OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
        OBV 指标把成交量分为正的成交量（价格上升时的成交量）和负的
        成交量（价格下降时）的成交量。OBV 就是分了正负之后的成交量
        的累计和。如果 OBV 和价格的均线一起上涨（下跌），则上涨（下
        跌）趋势被确认。如果 OBV 上升（下降）而价格的均线下降（上升），
        说明价格可能要反转，可能要开始新的下跌（上涨）行情。
        如果 OBV_HISTOGRAM 上穿 0 则买入，下穿 0 则卖出。
        """
        # VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
        df['VOL'] = np.where(df['close'] > df['close'].shift(1), df['volume'], -df['volume'])
        # VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
        df['VOL'] = np.where(df['close'] != df['close'].shift(1), df['VOL'], 0)
        # OBV=REF(OBV,1)+VOL
        df['OBV'] = df['VOL']
        df['OBV'] = df['VOL'] + df['OBV'].shift(1)
        # OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
        df['OBV_HISTOGRAM'] = df['OBV'].ewm(n, adjust=False).mean() - df['OBV'].ewm(3 * n, adjust=False).mean()
        df['前%dhOBV_HISTOGRAM' % n] = df['OBV_HISTOGRAM'].shift(1)
        extra_agg_dict['前%dhOBV_HISTOGRAM' % n] = 'first'
        # 删除中间过程数据
        del df['VOL']
        del df['OBV']
        del df['OBV_HISTOGRAM']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhOBV_HISTOGRAM' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # CMF 指标
    for n in back_hour_list:
        """
        N=60
        CMF=SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW),N)/SUM(VOLUME,N)
        CMF 用 CLV 对成交量进行加权，如果收盘价在高低价的中点之上，
        则为正的成交量（买方力量占优势）；若收盘价在高低价的中点之下，
        则为负的成交量（卖方力量占优势）。
        如果 CMF 上穿 0，则产生买入信号；
        如果 CMF 下穿 0，则产生卖出信号。
        """
        # ((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW)
        A = ((df['close'] - df['low']) - (df['high'] - df['close'])) * df['volume'] / (df['high'] - df['low'])
        # CMF=SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW),N)/SUM(VOLUME,N)
        df['CMF'] = A.rolling(n).sum() / df['volume'].rolling(n).sum()

        df['前%dhCMF' % n] = df['CMF'].shift(1)
        extra_agg_dict['前%dhCMF' % n] = 'first'
        del df['CMF']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhCMF' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # PVI 指标
    for n in back_hour_list:
        """
        N=40
        PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/ CLOSE,0)
        PVI=CUM_SUM(PVI_INC)
        PVI_MA=MA(PVI,N)
        PVI 是成交量升高的交易日的价格变化百分比的累积。
        PVI 相关理论认为，如果当前价涨量增，则说明散户主导市场，PVI
        可以用来识别价涨量增的市场（散户主导的市场）。
        如果 PVI 上穿 PVI_MA，则产生买入信号；
        如果 PVI 下穿 PVI_MA，则产生卖出信号。
        """
        df['ref_close'] = (df['close'] - df['close'].shift(1)) / df['close']  # (CLOSE-REF(CLOSE))/ CLOSE
        df['PVI_INC'] = np.where(df['volume'] > df['volume'].shift(1), df['ref_close'],
                                 0)  # PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/ CLOSE,0)
        df['PVI'] = df['PVI_INC'].cumsum()  # PVI=CUM_SUM(PVI_INC)
        df['PVI_INC_MA'] = df['PVI'].rolling(n, min_periods=1).mean()  # PVI_MA=MA(PVI,N)

        df['前%dhPVI' % n] = df['PVI_INC_MA'].shift(1)
        extra_agg_dict['前%dhPVI' % n] = 'first'
        # 删除中间数据
        del df['ref_close']
        del df['PVI_INC']
        del df['PVI']
        del df['PVI_INC_MA']

        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhPVI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # TMF 指标
    for n in back_hour_list:
        """
        N=80
        HIGH_TRUE=MAX(HIGH,REF(CLOSE,1))
        LOW_TRUE=MIN(LOW,REF(CLOSE,1))
        TMF=EMA(VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TR
        UE-LOW_TRUE),N)/EMA(VOL,N)
        TMF 指标和 CMF 指标类似，都是用价格对成交量加权。但是 CMF
        指标用 CLV 做权重，而 TMF 指标用的是真实最低价和真实最高价，
        且取的是移动平均而不是求和。如果 TMF 上穿 0，则产生买入信号；
        如果 TMF 下穿 0，则产生卖出信号。
        """
        df['ref'] = df['close'].shift(1)  # REF(CLOSE,1)
        df['max_high'] = df[['high', 'ref']].max(axis=1)  # HIGH_TRUE=MAX(HIGH,REF(CLOSE,1))
        df['min_low'] = df[['low', 'ref']].min(axis=1)  # LOW_TRUE=MIN(LOW,REF(CLOSE,1))
        # VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TRUE-LOW_TRUE)
        T = df['volume'] * (2 * df['close'] - df['max_high'] - df['min_low']) / (df['max_high'] - df['min_low'])
        # TMF=EMA(VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TRUE-LOW_TRUE),N)/EMA(VOL,N)
        df['TMF'] = T.ewm(n, adjust=False).mean() / df['volume'].ewm(n, adjust=False).mean()
        df['前%dhTMF' % n] = df['TMF'].shift(1)
        extra_agg_dict['前%dhTMF' % n] = 'first'
        # 删除中间数据
        del df['ref']
        del df['max_high']
        del df['min_low']
        del df['TMF']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhTMF' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # MFI 指标
    for n in back_hour_list:
        """
        N=14
        TYPICAL_PRICE=(HIGH+LOW+CLOSE)/3
        MF=TYPICAL_PRICE*VOLUME
        MF_POS=SUM(IF(TYPICAL_PRICE>=REF(TYPICAL_PRICE,1),M
        F,0),N)
        MF_NEG=SUM(IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),
        MF,0),N)
        MFI=100-100/(1+MF_POS/MF_NEG)
        MFI 指标的计算与 RSI 指标类似，不同的是，其在上升和下跌的条件
        判断用的是典型价格而不是收盘价，且其是对 MF 求和而不是收盘价
        的变化值。MFI 同样可以用来判断市场的超买超卖状态。
        如果 MFI 上穿 80，则产生买入信号；
        如果 MFI 下穿 20，则产生卖出信号。
        """
        df['price'] = (df['high'] + df['low'] + df['close']) / 3  # TYPICAL_PRICE=(HIGH+LOW+CLOSE)/3
        df['MF'] = df['price'] * df['volume']  # MF=TYPICAL_PRICE*VOLUME
        df['pos'] = np.where(df['price'] >= df['price'].shift(1), df['MF'],
                             0)  # IF(TYPICAL_PRICE>=REF(TYPICAL_PRICE,1),MF,0)MF,0),N)
        df['MF_POS'] = df['pos'].rolling(n).sum()
        df['neg'] = np.where(df['price'] <= df['price'].shift(1), df['MF'],
                             0)  # IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),MF,0)
        df['MF_NEG'] = df['neg'].rolling(n).sum()  # MF_NEG=SUM(IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),MF,0),N)

        df['MFI'] = 100 - 100 / (1 + df['MF_POS'] / df['MF_NEG'])  # MFI=100-100/(1+MF_POS/MF_NEG)

        df['前%dhMFI' % n] = df['MFI'].shift(1)
        extra_agg_dict['前%dhMFI' % n] = 'first'
        # 删除中间数据
        del df['price']
        del df['MF']
        del df['pos']
        del df['MF_POS']
        del df['neg']
        del df['MF_NEG']
        del df['MFI']

        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhMFI' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # ADOSC 指标
    for n in back_hour_list:
        """
        AD=CUM_SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW))
        AD_EMA1=EMA(AD,N1)
        AD_EMA2=EMA(AD,N2) 
        ADOSC=AD_EMA1-AD_EMA2
        ADL（收集派发线）指标是成交量的加权累计求和，其中权重为 CLV
        指标。ADL 指标可以与 OBV 指标进行类比。不同的是 OBV 指标只
        根据价格的变化方向把成交量分为正、负成交量再累加，而 ADL 是 用 CLV 指标作为权重进行成交量的累加。我们知道，CLV 指标衡量
        收盘价在最低价和最高价之间的位置，CLV>0(<0),则收盘价更靠近最
        高（低）价。CLV 越靠近 1(-1)，则收盘价越靠近最高（低）价。如
        果当天的 CLV>0，则 ADL 会加上成交量*CLV（收集）；如果当天的
        CLV<0，则 ADL 会减去成交量*CLV（派发）。
        ADOSC 指标是 ADL（收集派发线）指标的短期移动平均与长期移动
        平均之差。如果 ADOSC 上穿 0，则产生买入信号；如果 ADOSC 下 穿 0，则产生卖出信号。
        """
        # ((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW)
        df['AD'] = ((df['close'] - df['low']) - (df['high'] - df['close'])) * df['volume'] / (
                df['high'] - df['low'])
        df['AD_sum'] = df['AD'].cumsum()  # AD=CUM_SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW))
        df['AD_EMA1'] = df['AD_sum'].ewm(n, adjust=False).mean()  # AD_EMA1=EMA(AD,N1)
        df['AD_EMA2'] = df['AD_sum'].ewm(n * 2, adjust=False).mean()  # AD_EMA2=EMA(AD,N2)
        df['ADOSC'] = df['AD_EMA1'] - df['AD_EMA2']  # ADOSC=AD_EMA1-AD_EMA2

        # 标准化
        df['前%dhADOSC' % n] = (df['ADOSC'] - df['ADOSC'].rolling(n).min()) / (
                df['ADOSC'].rolling(n).max() - df['ADOSC'].rolling(n).min())
        df['前%dhADOSC' % n] = df['前%dhADOSC' % n].shift(1)
        extra_agg_dict['前%dhADOSC' % n] = 'first'
        # 删除中间数据
        del df['AD']
        del df['AD_sum']
        del df['AD_EMA2']
        del df['AD_EMA1']
        del df['ADOSC']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhADOSC' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # VR 指标
    for n in back_hour_list:
        """
        N=40
        AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        AVS=SUM(AV,N)
        BVS=SUM(BV,N)
        CVS=SUM(CV,N)
        VR=(AVS+CVS/2)/(BVS+CVS/2)

        VR 用过去 N 日股价上升日成交量与下降日成交量的比值来衡量多空
        力量对比。当 VR 小于 70 时，表示市场较为低迷；上穿 70 时表示市
        场可能有好转；上穿 250 时表示多方力量压倒空方力量。当 VR>300
        时，市场可能过热、买方力量过强，下穿 300 表明市场可能要反转。
        如果 VR 上穿 250，则产生买入信号；
        如果 VR 下穿 300，则产生卖出信号。
        """
        df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0)  # AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0)  # BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0)  # BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['AVS'] = df['AV'].rolling(n).sum()  # AVS=SUM(AV,N)
        df['BVS'] = df['BV'].rolling(n).sum()  # BVS=SUM(BV,N)
        df['CVS'] = df['CV'].rolling(n).sum()  # CVS=SUM(CV,N)
        df['VR'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2)  # VR=(AVS+CVS/2)/(BVS+CVS/2)
        df['前%dhVR' % n] = df['VR'].shift(1)
        extra_agg_dict['前%dhVR' % n] = 'first'
        # 删除中间数据
        del df['AV']
        del df['BV']
        del df['CV']
        del df['AVS']
        del df['BVS']
        del df['CVS']
        del df['VR']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhVR' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    # KO 指标
    for n in back_hour_list:
        """
        N1=34
        N2=55
        TYPICAL=(HIGH+LOW+CLOSE)/3
        VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
        VOLUME_EMA1=EMA(VOLUME,N1)
        VOLUME_EMA2=EMA(VOLUME,N2)
        KO=VOLUME_EMA1-VOLUME_EMA2
        这个技术指标的目的是为了观察短期和长期股票资金的流入和流出
        的情况。它的主要用途是确认股票价格趋势的方向和强度。KO 与
        OBV,VPT 等指标类似，都是用价格对成交量进行加权。KO 用的是典
        型价格的变化（只考虑变化方向，不考虑变化量），OBV 用的是收
        盘价的变化（只考虑变化方向，不考虑变化量），VPT 用的是价格的
        变化率（即考虑方向又考虑变化幅度）。
        如果 KO 上穿 0，则产生买入信号；
        如果 KO 下穿 0，则产生卖出信号。
        """
        df['price'] = (df['high'] + df['low'] + df['close']) / 3  # TYPICAL=(HIGH+LOW+CLOSE)/3
        df['V'] = np.where(df['price'] > df['price'].shift(1), df['volume'],
                           -df['volume'])  # VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
        df['V_ema1'] = df['V'].ewm(n, adjust=False).mean()  # VOLUME_EMA1=EMA(VOLUME,N1)
        df['V_ema2'] = df['V'].ewm(int(n * 1.618), adjust=False).mean()  # VOLUME_EMA2=EMA(VOLUME,N2)
        df['KO'] = df['V_ema1'] - df['V_ema2']  # KO=VOLUME_EMA1-VOLUME_EMA2
        # 标准化
        df['前%dhKO' % n] = (df['KO'] - df['KO'].rolling(n).min()) / (
                df['KO'].rolling(n).max() - df['KO'].rolling(n).min())
        df['前%dhKO' % n] = df['前%dhKO' % n].shift(1)
        extra_agg_dict['前%dhKO' % n] = 'first'
        # 删除中间数据
        del df['price']
        del df['V']
        del df['V_ema1']
        del df['V_ema2']
        del df['KO']
        # 差分
        add_diff(_df=df, _d_list=diff_d, _name='前%dhKO' % n, _agg_dict=extra_agg_dict, _agg_type='first')

    return df, extra_agg_dict


def signal_factor_avg_price(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 均价
    for n in back_hour_list:
        df['price'] = df['quote_volume'].rolling(n, min_periods=1).sum() / df['volume'].rolling(n, min_periods=1).sum()

        df[f'avg_price_bh_{n}'] = (df['price'] - df['price'].rolling(n, min_periods=1).min()) / (
                    df['price'].rolling(n, min_periods=1).max() - df['price'].rolling(n, min_periods=1).min() + eps)

        df[f'avg_price_bh_{n}'] = df[f'avg_price_bh_{n}'].shift(1)
        extra_agg_dict[f'avg_price_bh_{n}'] = 'first'
        del df['price']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'avg_price_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['avg_price', 'avg_price_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['avg_price']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_zhang_die_fu(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 涨跌幅
    for n in back_hour_list:
        df[f'zhang_die_fu_bh_{n}'] = df['close'].pct_change(n)
        df[f'zhang_die_fu_bh_{n}'] = df[f'zhang_die_fu_bh_{n}'].shift(1)
        extra_agg_dict[f'zhang_die_fu_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'zhang_die_fu_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['zhang_die_fu', 'zhang_die_fu_diff']
    else:
        return df, extra_agg_dict, ['zhang_die_fu']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bias(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bias # 涨跌幅更好的表达方式：bias，币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df[f'bias_bh_{n}'] = df['close'] / ma - 1
        df[f'bias_bh_{n}'] = df[f'bias_bh_{n}'].shift(1)
        extra_agg_dict[f'bias_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bias_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bias', 'bias_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bias']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_zhenfu(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 振幅：最高价最低价
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df[f'zhenfu_bh_{n}'] = high / low - 1
        df[f'zhenfu_bh_{n}'] = df[f'zhenfu_bh_{n}'].shift(1)
        extra_agg_dict[f'zhenfu_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'zhenfu_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['zhenfu', 'zhenfu_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['zhenfu']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_zhenfu2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 振幅：收盘价、开盘价
    high = df[['close', 'open']].max(axis=1)
    low = df[['close', 'open']].min(axis=1)
    for n in back_hour_list:
        high = high.rolling(n, min_periods=1).max()
        low = low.rolling(n, min_periods=1).min()
        df[f'zhenfu2_bh_{n}'] = high / low - 1
        df[f'zhenfu2_bh_{n}'] = df[f'zhenfu2_bh_{n}'].shift(1)
        extra_agg_dict[f'zhenfu2_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'zhenfu2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['zhenfu2', 'zhenfu2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['zhenfu2']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_zhang_die_fu_std(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 涨跌幅std，振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'zhang_die_fu_std_bh_{n}'] = change.rolling(n).std()
        df[f'zhang_die_fu_std_bh_{n}'] = df[f'zhang_die_fu_std_bh_{n}'].shift(1)
        extra_agg_dict[f'zhang_die_fu_std_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'zhang_die_fu_std_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['zhang_die_fu_std', 'zhang_die_fu_std_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['zhang_die_fu_std']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_zhang_die_fu_skew(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 涨跌幅偏度：在商品期货市场有效
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'zhang_die_fu_skew_bh_{n}'] = change.rolling(n).skew()
        df[f'zhang_die_fu_skew_bh_{n}'] = df[f'zhang_die_fu_skew_bh_{n}'].shift(1)
        extra_agg_dict[f'zhang_die_fu_skew_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'zhang_die_fu_skew_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['zhang_die_fu_skew', 'zhang_die_fu_skew_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['zhang_die_fu_skew']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_volume(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 成交额：对应小市值概念
    for n in back_hour_list:
        df[f'volume_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=1).sum()
        df[f'volume_bh_{n}'] = df[f'volume_bh_{n}'].shift(1)
        extra_agg_dict[f'volume_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'volume_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['volume', 'volume_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['volume']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_volume_std(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 成交额：对应小市值概念
    for n in back_hour_list:
        df[f'volume_std_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=2).std()
        df[f'volume_std_bh_{n}'] = df[f'volume_std_bh_{n}'].shift(1)
        extra_agg_dict[f'volume_std_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'volume_std_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['volume_std', 'volume_std_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['volume_std']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_taker_buy_ratio(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 资金流入比例，币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df[f'taker_buy_ratio_bh_{n}'] = buy_volume / volume
        df[f'taker_buy_ratio_bh_{n}'] = df[f'taker_buy_ratio_bh_{n}'].shift(1)
        extra_agg_dict[f'taker_buy_ratio_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'taker_buy_ratio_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['taker_buy_ratio', 'taker_buy_ratio_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['taker_buy_ratio']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_quote_volume_ratio(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 量比
    for n in back_hour_list:
        df[f'quote_volume_ratio_bh_{n}'] = df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()
        df[f'quote_volume_ratio_bh_{n}'] = df[f'quote_volume_ratio_bh_{n}'].shift(1)
        extra_agg_dict[f'quote_volume_ratio_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'quote_volume_ratio_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['quote_volume_ratio', 'quote_volume_ratio_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['quote_volume_ratio']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_trade_num(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 成交笔数
    for n in back_hour_list:
        df[f'trade_num_bh_{n}'] = df['trade_num'].rolling(n, min_periods=1).sum()
        df[f'trade_num_bh_{n}'] = df[f'trade_num_bh_{n}'].shift(1)
        extra_agg_dict[f'trade_num_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'trade_num_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['trade_num', 'trade_num_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['trade_num']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_quanlity_price_corr(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):    # 量价相关系数：量价相关选股策略
    for n in back_hour_list:
        df[f'quanlity_price_corr_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume'].rolling(n))
        df[f'quanlity_price_corr_bh_{n}'] = df[f'quanlity_price_corr_bh_{n}'].shift(1)
        extra_agg_dict[f'quanlity_price_corr_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'quanlity_price_corr_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['quanlity_price_corr', 'quanlity_price_corr_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['quanlity_price_corr']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_RSI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # RSI 指标
    for n in back_hour_list:
        """
        CLOSEUP=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        CLOSEDOWN=IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0)
        CLOSEUP_MA=SMA(CLOSEUP,N,1)
        CLOSEDOWN_MA=SMA(CLOSEDOWN,N,1)
        RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)
        RSI 反映一段时间内平均收益与平均亏损的对比。通常认为当 RSI 大 于 70，市场处于强势上涨甚至达到超买的状态；当 RSI 小于 30，市
        场处于强势下跌甚至达到超卖的状态。当 RSI 跌到 30 以下又上穿 30
        时，通常认为股价要从超卖的状态反弹；当 RSI 超过 70 又下穿 70
        时，通常认为市场要从超买的状态回落了。实际应用中，不一定要使
        用 70/30 的阈值选取。这里我们用 60/40 作为信号产生的阈值。
        RSI 上穿 40 则产生买入信号；
        RSI 下穿 60 则产生卖出信号。
        """
        diff = df['close'].diff() # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0) # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff), 0) # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()# SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean() # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df[f'RSI_bh_{n}'] = A / (A + B + eps)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df[f'RSI_bh_{n}'] = df[f'RSI_bh_{n}'].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'RSI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RSI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RSI', 'RSI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RSI']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_RSI2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # RSI 指标
    for n in back_hour_list:
        """
        CLOSEUP=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        CLOSEDOWN=IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0)
        CLOSEUP_MA=SMA(CLOSEUP,N,1)
        CLOSEDOWN_MA=SMA(CLOSEDOWN,N,1)
        RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)
        RSI 反映一段时间内平均收益与平均亏损的对比。通常认为当 RSI 大 于 70，市场处于强势上涨甚至达到超买的状态；当 RSI 小于 30，市
        场处于强势下跌甚至达到超卖的状态。当 RSI 跌到 30 以下又上穿 30
        时，通常认为股价要从超卖的状态反弹；当 RSI 超过 70 又下穿 70
        时，通常认为市场要从超买的状态回落了。实际应用中，不一定要使
        用 70/30 的阈值选取。这里我们用 60/40 作为信号产生的阈值。
        RSI 上穿 40 则产生买入信号；
        RSI 下穿 60 则产生卖出信号。
        """
        diff = df['close'].diff() # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0) # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff), 0) # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].rolling(n, min_periods=1).sum()# SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].rolling(n, min_periods=1).sum() # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df[f'RSI2_bh_{n}'] = A / (A + B + eps)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df[f'RSI2_bh_{n}'] = df[f'RSI2_bh_{n}'].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'RSI2_bh_{n}'] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RSI2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RSI2', 'RSI2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RSI2']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_KDJ(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # KDJ 指标
    for n in back_hour_list:
        """
        N=40
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        K=SMA(Stochastics,3,1)
        D=SMA(K,3,1) 
        先计算 N 日内的未成熟随机值 RSV，然后计算 K 值=（2*前日 K 值+
        当日 RSV）/3，D 值=（2*前日 D 值+当日 K 值）/3
        KDJ 指标用来衡量当前收盘价在过去 N 天的最低价与最高价之间的
        位置。值越高（低），则说明其越靠近过去 N 天的最高（低）价。当
        值过高或过低时，价格可能发生反转。通常认为 D 值小于 20 处于超
        卖状态，D 值大于 80 属于超买状态。
        如果 D 小于 20 且 K 上穿 D，则产生买入信号；
        如果 D 大于 80 且 K 下穿 D，则产生卖出信号。
        """
        low_list = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N) 求周期内low的最小值
        high_list = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N) 求周期内high 的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list + eps) * 100 # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100 计算一个随机值
        # K D J的值在固定的范围内
        df[f'K_bh_{n}'] = rsv.ewm(com=2).mean() # K=SMA(Stochastics,3,1) 计算k
        df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D=SMA(K,3,1)  计算D
        df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}'] # 计算J
        df[f'K_bh_{n}'] = df[f'K_bh_{n}'].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        df[f'D_bh_{n}'] = df[f'D_bh_{n}'].shift(1)
        df[f'J_bh_{n}'] = df[f'J_bh_{n}'].shift(1)
        extra_agg_dict[f'K_bh_{n}'] = 'first'
        extra_agg_dict[f'D_bh_{n}'] = 'first'
        extra_agg_dict[f'J_bh_{n}'] = 'first'

        if add:
            for f in [f'K_bh_{n}', f'D_bh_{n}', f'J_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=f, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['K', 'D', 'J', 'K_diff', 'D_diff', 'J_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['K', 'D', 'J'] # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_CCI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        df['oma'] = df['open'].ewm(span=n, adjust=False).mean() # 取 open 的ema
        df['hma'] = df['high'].ewm(span=n, adjust=False).mean() # 取 high 的ema
        df['lma'] = df['low'].ewm(span=n, adjust=False).mean() # 取 low的ema
        df['cma'] = df['close'].ewm(span=n, adjust=False).mean() # 取 close的ema
        df['tp'] = (df['oma'] + df['hma'] + df['lma'] + df['cma']) / 4 # 魔改CCI基础指标 将TP=(HIGH+LOW+CLOSE)/3  替换成以open/high/low/close的ema 的均值
        df['ma'] = df['tp'].ewm(span=n, adjust=False).mean() # MA(TP,N)  将移动平均改成 ema
        df['abs_diff_close'] = abs(df['tp'] - df['ma']) # ABS(TP-MA)
        df['md'] = df['abs_diff_close'].ewm(span=n, adjust=False).mean() # MD=MA(ABS(TP-MA),N)  将移动平均替换成ema

        df[f'CCI_bh_{n}'] = (df['tp'] - df['ma']) / (eps + df['md']) # CCI=(TP-MA)/(0.015MD)  CCI在一定范围内
        df[f'CCI_bh_{n}'] = df[f'CCI_bh_{n}'].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'CCI_bh_{n}'] = 'first'
        # # 删除中间数据
        del df['oma']
        del df['hma']
        del df['lma']
        del df['cma']
        del df['tp']
        del df['ma']
        del df['abs_diff_close']
        del df['md']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CCI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CCI', 'CCI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CCI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_CCI2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        open_ma = df['open'].rolling(n, min_periods=1).mean()
        high_ma = df['high'].rolling(n, min_periods=1).mean()
        low_ma = df['low'].rolling(n, min_periods=1).mean()
        close_ma = df['close'].rolling(n, min_periods=1).mean()
        tp = (high_ma + low_ma + close_ma) / 3 # TP=(HIGH+LOW+CLOSE)/3
        ma = tp.rolling(n, min_periods=1).mean() # MA=MA(TP,N)
        md = abs(ma - close_ma).rolling(n, min_periods=1).mean() # MD=MA(ABS(TP-MA),N)
        df[f'CCI2_bh_{n}'] = (tp - ma) / (md * 0.015 + eps) # CCI=(TP-MA)/(0.015MD)
        df[f'CCI2_bh_{n}'] = df[f'CCI2_bh_{n}'].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'CCI2_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CCI2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CCI2', 'CCI2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CCI2']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_CCI3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24],add=True):
    # 计算CCI指标
    for n in back_hour_list:
        """
        N=14
        TP=(HIGH+LOW+CLOSE)/3
        MA=MA(TP,N)
        MD=MA(ABS(TP-MA),N)
        CCI=(TP-MA)/(0.015MD)
        CCI 指标用来衡量典型价格（最高价、最低价和收盘价的均值）与其
        一段时间的移动平均的偏离程度。CCI 可以用来反映市场的超买超卖
        状态。一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于-100
        则市场处于超卖状态。当 CCI 下穿 100/上穿-100 时，说明股价可能
        要开始发生反转，可以考虑卖出/买入。
        """
        df['tp'] = (df['high'] + df['low'] + df['close']) / 3
        df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
        df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
        df[f'CCI3_bh_{n}'] = (df['tp'] - df['ma']) / (df['md'] * 0.015 + eps)
        df[f'CCI3_bh_{n}'] = df[f'CCI3_bh_{n}'].shift(1)
        extra_agg_dict[f'CCI3_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CCI3_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CCI3', 'CCI3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CCI3']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_MACD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24],add=True):
    # 计算macd指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=5
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        MACD_SIGNAL=EMA(MACD,N3)
        MACD_HISTOGRAM=MACD-MACD_SIGNAL

        MACD 指标衡量快速均线与慢速均线的差值。由于慢速均线反映的是
        之前较长时间的价格的走向，而快速均线反映的是较短时间的价格的
        走向，所以在上涨趋势中快速均线会比慢速均线涨的快，而在下跌趋
        势中快速均线会比慢速均线跌得快。所以 MACD 上穿/下穿 0 可以作
        为一种构造交易信号的方式。另外一种构造交易信号的方式是求
        MACD 与其移动平均（信号线）的差值得到 MACD 柱，利用 MACD
        柱上穿/下穿 0（即 MACD 上穿/下穿其信号线）来构造交易信号。这
        种方式在其他指标的使用中也可以借鉴。
        """
        short_windows = n
        long_windows = 3 * n
        macd_windows = int(1.618 * n)

        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean()  # EMA(CLOSE,N2)
        df['dif'] = df['ema_short'] - df['ema_long']  # MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['dea'] = df['dif'].ewm(span=macd_windows, adjust=False).mean()  # MACD_SIGNAL=EMA(MACD,N3)
        df['macd'] = 2 * (df['dif'] - df['dea'])  # MACD_HISTOGRAM=MACD-MACD_SIGNAL  一般看图指标计算对应实际乘以了2倍
        # 进行去量纲
        df[f'MACD_bh_{n}'] = df['macd'] / (df['macd'].rolling(macd_windows, min_periods=1).mean() + eps) - 1

        # df['前%dhdif' % n] = df['前%dhdif' % n].shift(1)
        # extra_agg_dict['前%dhdif' % n] = 'first'
        #
        # df['前%dhdea' % n] = df['前%dhdea' % n].shift(1)
        # extra_agg_dict['前%dhdea' % n] = 'first'

        df[f'MACD_bh_{n}'] = df[f'MACD_bh_{n}'].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'MACD_bh_{n}'] = 'first'

        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['dif']
        del df['dea']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MACD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MACD', 'MACD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MACD']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_MACD2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24],add=True):
    # 计算macd指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=5
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        MACD_SIGNAL=EMA(MACD,N3)
        MACD_HISTOGRAM=MACD-MACD_SIGNAL

        MACD 指标衡量快速均线与慢速均线的差值。由于慢速均线反映的是
        之前较长时间的价格的走向，而快速均线反映的是较短时间的价格的
        走向，所以在上涨趋势中快速均线会比慢速均线涨的快，而在下跌趋
        势中快速均线会比慢速均线跌得快。所以 MACD 上穿/下穿 0 可以作
        为一种构造交易信号的方式。另外一种构造交易信号的方式是求
        MACD 与其移动平均（信号线）的差值得到 MACD 柱，利用 MACD
        柱上穿/下穿 0（即 MACD 上穿/下穿其信号线）来构造交易信号。这
        种方式在其他指标的使用中也可以借鉴。
        """
        short_windows = n
        long_windows = 3 * n
        macd_windows = int(1.618 * n)
        df['dif_close'] = df['close'] - df['close'].shift(1)
        df['ema_short'] = df['dif_close'].ewm(span=short_windows, adjust=False).mean()  # EMA(CLOSE,N1)
        df['ema_long'] = df['dif_close'].ewm(span=long_windows, adjust=False).mean()  # EMA(CLOSE,N2)
        df['dif'] = df['ema_short'] - df['ema_long']  # MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['dea'] = df['dif'].ewm(span=macd_windows, adjust=False).mean()  # MACD_SIGNAL=EMA(MACD,N3)
        df['macd'] = 2 * (df['dif'] - df['dea'])  # MACD_HISTOGRAM=MACD-MACD_SIGNAL  一般看图指标计算对应实际乘以了2倍
        # 进行去量纲
        df[f'MACD2_bh_{n}'] = scale_zscore(df['macd'], n)

        # df['前%dhdif' % n] = df['前%dhdif' % n].shift(1)
        # extra_agg_dict['前%dhdif' % n] = 'first'
        #
        # df['前%dhdea' % n] = df['前%dhdea' % n].shift(1)
        # extra_agg_dict['前%dhdea' % n] = 'first'

        df[f'MACD2_bh_{n}'] = df[f'MACD2_bh_{n}'].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'MACD2_bh_{n}'] = 'first'

        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['dif']
        del df['dea']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MACD2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MACD2', 'MACD2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MACD2']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_dif_ema(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算ema的差值
    for n in back_hour_list:
        """
        与求MACD的dif线一样
        """
        short_windows = n
        long_windows = 3 * n
        df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean() # 计算短周期ema
        df['ema_long'] = df['close'].ewm(span=long_windows, adjust=False).mean() # 计算长周期的ema
        df['dif_ema'] = df['ema_short'] - df['ema_long'] # 计算俩条线之间的差值

        df['dif_ema_mean'] = df['dif_ema'].ewm(span=n, adjust=False).mean()

        df[f'dif_ema_bh_{n}'] = df['dif_ema'] / (df['dif_ema_mean'] + eps) - 1  # 去量纲
        df[f'dif_ema_bh_{n}'] = df[f'dif_ema_bh_{n}'].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'dif_ema_bh_{n}'] = 'first'
        # 删除中间数据
        del df['ema_short']
        del df['ema_long']
        del df['dif_ema']
        del df['dif_ema_mean']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'dif_ema_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['dif_ema', 'dif_ema_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['dif_ema']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_vwap_bias(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bias因子以均价表示
    for n in back_hour_list:
        """
        将bias 的close替换成vwap
        """
        df['vwap'] = df['volume'] / df['quote_volume']  # 在周期内成交额除以成交量等于成交均价
        ma = df['vwap'].rolling(n, min_periods=1).mean() # 求移动平均线
        df[f'vwap_bias_bh_{n}'] = df['vwap'] / (ma + eps) - 1  # 去量纲
        df[f'vwap_bias_bh_{n}'] = df[f'vwap_bias_bh_{n}'].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'vwap_bias_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'vwap_bias_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['vwap_bias', 'vwap_bias_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['vwap_bias']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bbi_bias(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算BBI 的bias
    for n in back_hour_list:
        """
        BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        BBI 是对不同时间长度的移动平均线取平均，能够综合不同移动平均
        线的平滑性和滞后性。如果收盘价上穿/下穿 BBI 则产生买入/卖出信
        号。
        """
        # 将BBI指标计算出来求bias
        ma1 = df['close'].rolling(n, min_periods=1).mean()
        ma2 = df['close'].rolling(2 * n, min_periods=1).mean()
        ma3 = df['close'].rolling(4 * n, min_periods=1).mean()
        ma4 = df['close'].rolling(8 * n, min_periods=1).mean()
        bbi = (ma1 + ma2 + ma3 + ma4) / 4 # BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        df[f'bbi_bias_bh_{n}'] = df['close'] / (bbi + eps) - 1
        df[f'bbi_bias_bh_{n}'] = df[f'bbi_bias_bh_{n}'].shift(1) # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'bbi_bias_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bbi_bias_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bbi_bias', 'bbi_bias_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bbi_bias']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_DPO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算 DPO
    for n in back_hour_list:
        """
        N=20
        DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        DPO 是当前价格与延迟的移动平均线的差值，通过去除前一段时间
        的移动平均价格来减少长期的趋势对短期价格波动的影响。DPO>0
        表示目前处于多头市场；DPO<0 表示当前处于空头市场。我们通过
        DPO 上穿/下穿 0 线来产生买入/卖出信号。

        """
        ma = df['close'].rolling(n, min_periods=1).mean()# 求close移动平均线
        ref = ma.shift(int(n / 2 + 1)) # REF(MA(CLOSE,N),N/2+1)
        df['DPO'] = df['close'] - ref # DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
        df['DPO_ma'] = df['DPO'].rolling(n, min_periods=1).mean()  # 求均值
        df[f'DPO_bh_{n}'] = df['DPO'] / (df['DPO_ma'] + eps) - 1  # 去量纲
        df[f'DPO_bh_{n}'] = df[f'DPO_bh_{n}'].shift(1)  # 取前一周期防止未来函数  实盘中不需要
        extra_agg_dict[f'DPO_bh_{n}'] = 'first'
        # 删除中间数据
        del df['DPO']
        del df['DPO_ma']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DPO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DPO', 'DPO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DPO']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_ER(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算 ER
    for n in back_hour_list:
        """
        N=20
        BullPower=HIGH-EMA(CLOSE,N)
        BearPower=LOW-EMA(CLOSE,N)
        ER 为动量指标。用来衡量市场的多空力量对比。在多头市场，人们
        会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量
        越强；而在空头市场，人们可能因为恐惧而在接近低价的地方卖出。
        BearPower 越低则当前空头力量越强。当两者都大于 0 时，反映当前
        多头力量占据主导地位；两者都小于0则反映空头力量占据主导地位。
        如果 BearPower 上穿 0，则产生买入信号；
        如果 BullPower 下穿 0，则产生卖出信号。
        """
        ema = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        bull_power = df['high'] - ema  # 越高表示上涨 牛市 BullPower=HIGH-EMA(CLOSE,N)
        bear_power = df['low'] - ema  # 越低表示下降越厉害  熊市 BearPower=LOW-EMA(CLOSE,N)
        df[f'ER_bull_bh_{n}'] = bull_power / (ema + eps)  # 去量纲
        df[f'ER_bear_bh_{n}'] = bear_power / (ema + eps)  # 去量纲
        df[f'ER_bull_bh_{n}'] = df[f'ER_bull_bh_{n}'].shift(1)
        df[f'ER_bear_bh_{n}'] = df[f'ER_bear_bh_{n}'].shift(1)
        extra_agg_dict[f'ER_bull_bh_{n}'] = 'first'
        extra_agg_dict[f'ER_bear_bh_{n}'] = 'first'
        if add:
            for f in [f'ER_bull_bh_{n}', f'ER_bear_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=f, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ER_bull', 'ER_bear', 'ER_bull_diff', 'ER_bear_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ER_bull', 'ER_bear']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_PO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # PO指标
    for n in back_hour_list:
        """
        EMA_SHORT=EMA(CLOSE,9)
        EMA_LONG=EMA(CLOSE,26)
        PO=(EMA_SHORT-EMA_LONG)/EMA_LONG*100
        PO 指标求的是短期均线与长期均线之间的变化率。
        如果 PO 上穿 0，则产生买入信号；
        如果 PO 下穿 0，则产生卖出信号。
        """
        ema_short = df['close'].ewm(n, adjust=False).mean() # 短周期的ema
        ema_long = df['close'].ewm(n * 3, adjust=False).mean() # 长周期的ema   固定倍数关系 减少参数
        df[f'PO_bh_{n}'] = (ema_short - ema_long) / (ema_long + eps) * 100 # 去量纲
        df[f'PO_bh_{n}'] = df[f'PO_bh_{n}'].shift(1)
        extra_agg_dict[f'PO_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PO', 'PO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PO']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_MADisplaced(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # MADisplaced 指标
    for n in back_hour_list:
        """
        N=20
        M=10
        MA_CLOSE=MA(CLOSE,N)
        MADisplaced=REF(MA_CLOSE,M)
        MADisplaced 指标把简单移动平均线向前移动了 M 个交易日，用法
        与一般的移动平均线一样。如果收盘价上穿/下穿 MADisplaced 则产
        生买入/卖出信号。
        有点变种bias
        """
        ma = df['close'].rolling(2 * n, min_periods=1).mean()  # MA(CLOSE,N) 固定俩个参数之间的关系  减少参数
        ref = ma.shift(n)  # MADisplaced=REF(MA_CLOSE,M)

        df[f'MADisplaced_bh_{n}'] = df['close'] / (ref + eps) - 1 # 去量纲
        df[f'MADisplaced_bh_{n}'] = df[f'MADisplaced_bh_{n}'].shift(1)
        extra_agg_dict[f'MADisplaced_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MADisplaced_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MADisplaced', 'MADisplaced_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MADisplaced']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_T3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # T3 指标
    for n in back_hour_list:
        """
        N=20
        VA=0.5
        T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        当 VA 是 0 时，T3 就是三重指数平均线，此时具有严重的滞后性；当
        VA 是 0 时，T3 就是三重双重指数平均线（DEMA），此时可以快速
        反应价格的变化。VA 值是 T3 指标的一个关键参数，可以用来调节
        T3 指标的滞后性。如果收盘价上穿/下穿 T3，则产生买入/卖出信号。
        """
        va = 0.5
        ema = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N),N)
        T1 = ema * (1 + va) - ema_ema * va # T1=EMA(CLOSE,N)*(1+VA)-EMA(EMA(CLOSE,N),N)*VA
        T1_ema = T1.ewm(n, adjust=False).mean() # EMA(T1,N)
        T1_ema_ema = T1_ema.ewm(n, adjust=False).mean()  # EMA(EMA(T1,N),N)
        T2 = T1_ema * (1 + va) - T1_ema_ema * va # T2=EMA(T1,N)*(1+VA)-EMA(EMA(T1,N),N)*VA
        T2_ema = T2.ewm(n, adjust=False).mean() # EMA(T2,N)
        T2_ema_ema = T2_ema.ewm(n, adjust=False).mean() # EMA(EMA(T2,N),N)
        T3 = T2_ema * (1 + va) - T2_ema_ema * va # T3=EMA(T2,N)*(1+VA)-EMA(EMA(T2,N),N)*VA
        df[f'T3_bh_{n}'] = df['close'] / (T3 + eps) - 1  # 去量纲
        df[f'T3_bh_{n}'] = df[f'T3_bh_{n}'].shift(1)
        extra_agg_dict[f'T3_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'T3_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['T3', 'T3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['T3']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_POS(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # POS指标
    for n in back_hour_list:
        """
        N=100
        PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        POS 指标衡量当前的 N 天收益率在过去 N 天的 N 天收益率最大值和
        最小值之间的位置。当 POS 上穿 80 时产生买入信号；当 POS 下穿
        20 时产生卖出信号。
        """
        ref = df['close'].shift(n) # REF(CLOSE,N)
        price = (df['close'] - ref) / ref # PRICE=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
        min_price = price.rolling(n, min_periods=1).min() # MIN(PRICE,N)
        max_price = price.rolling(n, min_periods=1).max() # MAX(PRICE,N)
        pos = (price - min_price) / (max_price - min_price + eps) # POS=(PRICE-MIN(PRICE,N))/(MAX(PRICE,N)-MIN(PRICE,N))
        df[f'POS_bh_{n}'] = pos.shift(1)
        extra_agg_dict[f'POS_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'POS_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['POS', 'POS_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['POS']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_PAC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # PAC 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        UPPER=SMA(HIGH,N1,1)
        LOWER=SMA(LOW,N2,1)
        用最高价和最低价的移动平均来构造价格变化的通道，如果价格突破
        上轨则做多，突破下轨则做空。
        """
        # upper = df['high'].rolling(n, min_periods=1).mean()
        df['upper'] = df['high'].ewm(span=n).mean() # UPPER=SMA(HIGH,N1,1)
        # lower = df['low'].rolling(n, min_periods=1).mean()
        df['lower'] = df['low'].ewm(span=n).mean() # LOWER=SMA(LOW,N2,1)
        df['width'] = df['upper'] - df['lower'] # 添加指标求宽度进行去量纲
        df['width_ma'] = df['width'].rolling(n, min_periods=1).mean()

        df[f'PAC_bh_{n}'] = df['width'] / (df['width_ma'] + eps) - 1
        df[f'PAC_bh_{n}'] = df[f'PAC_bh_{n}'].shift(1)
        extra_agg_dict[f'PAC_bh_{n}'] = 'first'

        # 删除中间数据
        del df['upper']
        del df['lower']
        del df['width']
        del df['width_ma']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PAC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PAC', 'PAC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PAC']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_ADM(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # ADM 指标
    for n in back_hour_list:
        """
        N=20
        DTM=IF(OPEN>REF(OPEN,1),MAX(HIGH-OPEN,OPEN-REF(OP
        EN,1)),0)
        DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-O
        PEN),0)
        STM=SUM(DTM,N)
        SBM=SUM(DBM,N)
        ADTM=(STM-SBM)/MAX(STM,SBM)
        ADTM 通过比较开盘价往上涨的幅度和往下跌的幅度来衡量市场的
        人气。ADTM 的值在-1 到 1 之间。当 ADTM 上穿 0.5 时，说明市场
        人气较旺；当 ADTM 下穿-0.5 时，说明市场人气较低迷。我们据此构
        造交易信号。
        当 ADTM 上穿 0.5 时产生买入信号；
        当 ADTM 下穿-0.5 时产生卖出信号。

        """
        df['h_o'] = df['high'] - df['open'] # HIGH-OPEN
        df['diff_open'] = df['open'] - df['open'].shift(1) # OPEN-REF(OPEN,1)
        max_value1 = df[['h_o', 'diff_open']].max(axis=1) # MAX(HIGH-OPEN,OPEN-REF(OPEN,1))
        # df.loc[df['open'] > df['open'].shift(1), 'DTM'] = max_value1
        # df['DTM'].fillna(value=0, inplace=True)
        df['DTM'] = np.where(df['open'] > df['open'].shift(1), max_value1, 0) #DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['o_l'] = df['open'] - df['low'] # OPEN-LOW
        max_value2 = df[['o_l', 'diff_open']].max(axis=1) # MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        df['DBM'] = np.where(df['open'] < df['open'].shift(1), max_value2, 0) #DBM=IF(OPEN<REF(OPEN,1),MAX(OPEN-LOW,REF(OPEN,1)-OPEN),0)
        # df.loc[df['open'] < df['open'].shift(1), 'DBM'] = max_value2
        # df['DBM'].fillna(value=0, inplace=True)

        df['STM'] = df['DTM'].rolling(n, min_periods=1).sum() # STM=SUM(DTM,N)
        df['SBM'] = df['DBM'].rolling(n, min_periods=1).sum() # SBM=SUM(DBM,N)
        max_value3 = df[['STM', 'SBM']].max(axis=1) # MAX(STM,SBM)
        ADTM = (df['STM'] - df['SBM']) / (max_value3 + eps) # ADTM=(STM-SBM)/MAX(STM,SBM)
        df[f'ADM_bh_{n}'] = ADTM.shift(1)
        extra_agg_dict[f'ADM_bh_{n}'] = 'first'

        # 删除中间数据
        del df['h_o']
        del df['diff_open']
        del df['o_l']
        del df['STM']
        del df['SBM']
        del df['DBM']
        del df['DTM']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ADM_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ADM', 'ADM_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ADM']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_ZLMACD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # ZLMACD 指标
    for n in back_hour_list:
        """
        N1=20
        N2=100
        ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EM
        A(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        ZLMACD 指标是对 MACD 指标的改进，它在计算中使用 DEMA 而不
        是 EMA，可以克服 MACD 指标的滞后性问题。如果 ZLMACD 上穿/
        下穿 0，则产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N1)
        ema_ema_1 = ema1.ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N1),N1)
        n2 = 5 * n # 固定俩参数的倍数关系减少参数
        ema2 = df['close'].ewm(n2, adjust=False).mean() # EMA(CLOSE,N2)
        ema_ema_2 = ema2.ewm(n2, adjust=False).mean() # EMA(EMA(CLOSE,N2),N2)
        ZLMACD = (2 * ema1 - ema_ema_1) - (2 * ema2 - ema_ema_2) # ZLMACD=(2*EMA(CLOSE,N1)-EMA(EMA(CLOSE,N1),N1))-(2*EMA(CLOSE,N2)-EMA(EMA(CLOSE,N2),N2))
        df[f'ZLMACD_bh_{n}'] = df['close'] / (ZLMACD + eps) - 1
        df[f'ZLMACD_bh_{n}'] = df[f'ZLMACD_bh_{n}'].shift(1)
        extra_agg_dict[f'ZLMACD_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ZLMACD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ZLMACD', 'ZLMACD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ZLMACD']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_TMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # TMA 指标
    for n in back_hour_list:
        """
        N=20
        CLOSE_MA=MA(CLOSE,N)
        TMA=MA(CLOSE_MA,N)
        TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
        越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
        靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
        卖出信号。
        """
        ma = df['close'].rolling(n, min_periods=1).mean() # CLOSE_MA=MA(CLOSE,N)
        tma = ma.rolling(n, min_periods=1).mean() # TMA=MA(CLOSE_MA,N)
        df[f'TMA_bh_{n}'] = df['close'] / (tma + eps) - 1
        df[f'TMA_bh_{n}'] = df[f'TMA_bh_{n}'].shift(1)
        extra_agg_dict[f'TMA_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TMA', 'TMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TMA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_TYP(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # TYP 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1=EMA(TYP,N1)
        TYPMA2=EMA(TYP,N2)
        在技术分析中，典型价格（最高价+最低价+收盘价）/3 经常被用来代
        替收盘价。比如我们在利用均线交叉产生交易信号时，就可以用典型
        价格的均线。
        TYPMA1 上穿/下穿 TYPMA2 时产生买入/卖出信号。
        """
        TYP = (df['close'] + df['high'] + df['low']) / 3 # TYP=(CLOSE+HIGH+LOW)/3
        TYPMA1 = TYP.ewm(n, adjust=False).mean() # TYPMA1=EMA(TYP,N1)
        TYPMA2 = TYP.ewm(n * 3, adjust=False).mean() # TYPMA2=EMA(TYP,N2) 并且固定俩参数倍数关系
        diff_TYP = TYPMA1 - TYPMA2 # 俩ema相差
        diff_TYP_mean = diff_TYP.rolling(n, min_periods=1).mean()
        # diff_TYP_min = diff_TYP.rolling(n, min_periods=1).std()
        # 无量纲
        df[f'TYP_bh_{n}'] = diff_TYP / (diff_TYP_mean + eps) -1
        df[f'TYP_bh_{n}'] = df[f'TYP_bh_{n}'].shift(1)
        extra_agg_dict[f'TYP_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TYP_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TYP', 'TYP_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TYP']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_KDJD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # KDJD 指标
    for n in back_hour_list:
        """
        N=20
        M=60
        LOW_N=MIN(LOW,N)
        HIGH_N=MAX(HIGH,N)
        Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K=SMA(Stochastics_DOUBLE,3,1)
        D=SMA(K,3,1)
        KDJD 可以看作 KDJ 的变形。KDJ 计算过程中的变量 Stochastics 用
        来衡量收盘价位于最近 N 天最高价和最低价之间的位置。而 KDJD 计
        算过程中的 Stochastics_DOUBLE 可以用来衡量 Stochastics 在最近
        N 天的 Stochastics 最大值与最小值之间的位置。我们这里将其用作
        动量指标。当 D 上穿 70/下穿 30 时，产生买入/卖出信号。
        """
        min_low = df['low'].rolling(n, min_periods=1).min()  # LOW_N=MIN(LOW,N)
        max_high = df['high'].rolling(n, min_periods=1).max() # HIGH_N=MAX(HIGH,N)
        Stochastics = (df['close'] - min_low) / (max_high - min_low + eps) * 100 # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100
        # 固定俩参数的倍数关系
        Stochastics_LOW = Stochastics.rolling(n * 3, min_periods=1).min() # Stochastics_LOW=MIN(Stochastics,M)
        Stochastics_HIGH = Stochastics.rolling(n * 3, min_periods=1).max() # Stochastics_HIGH=MAX(Stochastics,M)
        Stochastics_DOUBLE = (Stochastics - Stochastics_LOW) / (Stochastics_HIGH - Stochastics_LOW + eps) # Stochastics_DOUBLE=(Stochastics-Stochastics_LOW)/(Stochastics_HIGH-Stochastics_LOW)*100
        K = Stochastics_DOUBLE.ewm(com=2).mean() #  K=SMA(Stochastics_DOUBLE,3,1)
        D = K.ewm(com=2).mean() # D=SMA(K,3,1)
        df[f'KDJD_K_bh_{n}'] = K.shift(1)
        df[f'KDJD_D_bh_{n}'] = D.shift(1)
        extra_agg_dict[f'KDJD_K_bh_{n}'] = 'first'
        extra_agg_dict[f'KDJD_D_bh_{n}'] = 'first'
        if add:
            for _ in [f'KDJD_K_bh_{n}', f'KDJD_D_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['KDJD_K', 'KDJD_D', 'KDJD_K_diff', 'KDJD_D_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['KDJD_K', 'KDJD_D']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_vma_bias(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # VMA 指标
    for n in back_hour_list:
        """
        N=20
        PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        VMA=MA(PRICE,N)
        VMA 就是简单移动平均把收盘价替换为最高价、最低价、开盘价和
        收盘价的平均值。当 PRICE 上穿/下穿 VMA 时产生买入/卖出信号。
        """
        price = (df['high'] + df['low'] + df['open'] + df['close']) / 4 # PRICE=(HIGH+LOW+OPEN+CLOSE)/4
        vma = price.rolling(n, min_periods=1).mean() # VMA=MA(PRICE,N)
        df[f'vma_bias_bh_{n}'] = price / (vma + eps) - 1 # 去量纲
        df[f'vma_bias_bh_{n}'] = df[f'vma_bias_bh_{n}'].shift(1)
        extra_agg_dict[f'vma_bias_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'vma_bias_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['vma_bias', 'vma_bias_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['vma_bias']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_DDI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # DDI 指标
    for n in back_hour_list:
        """
        n = 40
        HL=HIGH+LOW
        HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        LOW_ABS=ABS(LOW-REF(LOW,1))
        DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DDI=DIZ-DIF
        DDI 指标用来比较向上波动和向下波动的比例。如果 DDI 上穿/下穿 0
        则产生买入/卖出信号。
        """
        df['hl'] = df['high'] + df['low'] # HL=HIGH+LOW
        df['abs_high'] = abs(df['high'] - df['high'].shift(1)) # HIGH_ABS=ABS(HIGH-REF(HIGH,1))
        df['abs_low'] = abs(df['low'] - df['low'].shift(1)) # LOW_ABS=ABS(LOW-REF(LOW,1))
        max_value1 = df[['abs_high', 'abs_low']].max(axis=1)  # MAX(HIGH_ABS,LOW_ABS)
        # df.loc[df['hl'] > df['hl'].shift(1), 'DMZ'] = max_value1
        # df['DMZ'].fillna(value=0, inplace=True)
        df['DMZ'] = np.where((df['hl'] > df['hl'].shift(1)), max_value1, 0) # DMZ=IF(HL>REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)
        # df.loc[df['hl'] < df['hl'].shift(1), 'DMF'] = max_value1
        # df['DMF'].fillna(value=0, inplace=True)
        df['DMF'] = np.where((df['hl'] < df['hl'].shift(1)), max_value1, 0) # DMF=IF(HL<REF(HL,1),MAX(HIGH_ABS,LOW_ABS),0)

        DMZ_SUM = df['DMZ'].rolling(n, min_periods=1).sum() # SUM(DMZ,N)
        DMF_SUM = df['DMF'].rolling(n, min_periods=1).sum() # SUM(DMF,N)
        DIZ = DMZ_SUM / (DMZ_SUM + DMF_SUM + eps) # DIZ=SUM(DMZ,N)/(SUM(DMZ,N)+SUM(DMF,N))
        DIF = DMF_SUM / (DMZ_SUM + DMF_SUM + eps) # DIF=SUM(DMF,N)/(SUM(DMZ,N)+SUM(DMF,N))
        df[f'DDI_bh_{n}'] = DIZ - DIF
        df[f'DDI_bh_{n}'] = df[f'DDI_bh_{n}'].shift(1)
        extra_agg_dict[f'DDI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['hl']
        del df['abs_high']
        del df['abs_low']
        del df['DMZ']
        del df['DMF']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DDI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DDI', 'DDI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DDI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_HMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # HMA 指标
    for n in back_hour_list:
        """
        N=20
        HMA=MA(HIGH,N)
        HMA 指标为简单移动平均线把收盘价替换为最高价。当最高价上穿/
        下穿 HMA 时产生买入/卖出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean() # HMA=MA(HIGH,N)
        df[f'HMA_bh_{n}'] = df['high'] / (hma + eps) - 1 # 去量纲
        df[f'HMA_bh_{n}'] = df[f'HMA_bh_{n}'].shift(1)
        extra_agg_dict[f'HMA_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'HMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['HMA', 'HMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['HMA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_SROC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # SROC 指标
    for n in back_hour_list:
        """
        N=13
        M=21
        EMAP=EMA(CLOSE,N)
        SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROC 与 ROC 类似，但是会对收盘价进行平滑处理后再求变化率。
        """
        ema = df['close'].ewm(n, adjust=False).mean() # EMAP=EMA(CLOSE,N)
        ref = ema.shift(2 * n) # 固定俩参数之间的倍数 REF(EMAP,M)
        df[f'SROC_bh_{n}'] = (ema - ref) / (ref + eps) # SROC=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df[f'SROC_bh_{n}'] = df[f'SROC_bh_{n}'].shift(1)
        extra_agg_dict[f'SROC_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'SROC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['SROC', 'SROC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['SROC']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_DC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # DC 指标
    for n in back_hour_list:
        """
        N=20
        UPPER=MAX(HIGH,N)
        LOWER=MIN(LOW,N)
        MIDDLE=(UPPER+LOWER)/2
        DC 指标用 N 天最高价和 N 天最低价来构造价格变化的上轨和下轨，
        再取其均值作为中轨。当收盘价上穿/下穿中轨时产生买入/卖出信号。
        """
        upper = df['high'].rolling(n, min_periods=1).max() #UPPER=MAX(HIGH,N)
        lower = df['low'].rolling(n, min_periods=1).min() # LOWER=MIN(LOW,N)
        middle = (upper + lower) / 2 # MIDDLE=(UPPER+LOWER)/2
        ma_middle = middle.rolling(n, min_periods=1).mean() # 求中轨的均线
        # 进行无量纲处理
        df[f'DC_bh_{n}'] = middle / (ma_middle + eps) - 1
        df[f'DC_bh_{n}'] = df[f'DC_bh_{n}'].shift(1)
        extra_agg_dict[f'DC_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DC', 'DC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DC']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_DC2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # DC 指标
    for n in back_hour_list:
        """
        N=20
        UPPER=MAX(HIGH,N)
        LOWER=MIN(LOW,N)
        MIDDLE=(UPPER+LOWER)/2
        DC 指标用 N 天最高价和 N 天最低价来构造价格变化的上轨和下轨，
        再取其均值作为中轨。当收盘价上穿/下穿中轨时产生买入/卖出信号。
        """
        upper = df['high'].rolling(n, min_periods=1).max() #UPPER=MAX(HIGH,N)
        lower = df['low'].rolling(n, min_periods=1).min() # LOWER=MIN(LOW,N)
        middle = (upper + lower) / 2 # MIDDLE=(UPPER+LOWER)/2
        # ma_middle = middle.rolling(n, min_periods=1).mean() # 求中轨的均线
        width = upper - lower
        # 进行无量纲处理
        df[f'DC2_bh_{n}'] = width / (middle + eps)
        df[f'DC2_bh_{n}'] = df[f'DC2_bh_{n}'].shift(1)
        extra_agg_dict[f'DC2_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DC2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DC2', 'DC2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DC2']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_VIDYA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # VIDYA
    for n in back_hour_list:
        """
        N=10
        VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER
        （EfficiencyRatio）指标。在当前趋势较强时，ER 值较大，VIDYA
        会赋予当前价格更大的权重，使得 VIDYA 紧随价格变动，减小其滞
        后性；在当前趋势较弱（比如振荡市中）,ER 值较小，VIDYA 会赋予
        当前价格较小的权重，增大 VIDYA 的滞后性，使其更加平滑，避免
        产生过多的交易信号。
        当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
        """
        df['abs_diff_close'] = abs(df['close'] - df['close'].shift(n)) # ABS(CLOSE-REF(CLOSE,N))
        df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n, min_periods=1).sum() # SUM(ABS(CLOSE-REF(CLOSE,1))
        VI = df['abs_diff_close'] / df['abs_diff_close_sum'] # VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA = VI * df['close'] + (1 - VI) * df['close'].shift(1) # VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        # 进行无量纲处理
        df[f'VIDYA_bh_{n}'] = VIDYA / (df['close'] + eps) - 1
        df[f'VIDYA_bh_{n}'] = df[f'VIDYA_bh_{n}'].shift(1)
        extra_agg_dict[f'VIDYA_bh_{n}'] = 'first'
        # 删除中间数据
        del df['abs_diff_close']
        del df['abs_diff_close_sum']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'VIDYA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['VIDYA', 'VIDYA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['VIDYA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_Qstick(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # Qstick 指标
    for n in back_hour_list:
        """
        N=20
        Qstick=MA(CLOSE-OPEN,N)
        Qstick 通过比较收盘价与开盘价来反映股价趋势的方向和强度。如果
        Qstick 上穿/下穿 0 则产生买入/卖出信号。
        """
        cl = df['close'] - df['open'] # CLOSE-OPEN
        Qstick = cl.rolling(n, min_periods=1).mean() # Qstick=MA(CLOSE-OPEN,N)
        # 进行无量纲处理
        df[f'Qstick_bh_{n}'] = cl / (Qstick + eps) - 1
        df[f'Qstick_bh_{n}'] = df[f'Qstick_bh_{n}'].shift(1)
        extra_agg_dict[f'Qstick_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'Qstick_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['Qstick', 'Qstick_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['Qstick']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_ATR(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # ATR 因子
    for n in back_hour_list:
        """
        N=20
        TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        ATR=MA(TR,N)
        MIDDLE=MA(CLOSE,N)
        """
        df['c1'] = df['high'] - df['low'] # HIGH-LOW
        df['c2'] = abs(df['high'] - df['close'].shift(1)) # ABS(HIGH-REF(CLOSE,1)
        df['c3'] = abs(df['low'] - df['close'].shift(1)) # ABS(LOW-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean() # ATR=MA(TR,N)
        df['middle'] = df['close'].rolling(n, min_periods=1).mean() # MIDDLE=MA(CLOSE,N)

        # ATR指标去量纲
        df[f'ATR_bh_{n}'] = df['ATR'] / (df['middle'] + eps)
        df[f'ATR_bh_{n}'] = df[f'ATR_bh_{n}'].shift(1)
        extra_agg_dict[f'ATR_bh_{n}'] = 'first'
        # 删除中间数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['middle']

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ATR_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ATR', 'ATR_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ATR']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_DEMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # DEMA 指标
    for n in back_hour_list:
        """
        N=60
        EMA=EMA(CLOSE,N)
        DEMA=2*EMA-EMA(EMA,N)
        DEMA 结合了单重 EMA 和双重 EMA，在保证平滑性的同时减少滞后
        性。
        """
        ema = df['close'].ewm(n, adjust=False).mean() # EMA=EMA(CLOSE,N)
        ema_ema = ema.ewm(n, adjust=False).mean() # EMA(EMA,N)
        dema = 2 * ema - ema_ema # DEMA=2*EMA-EMA(EMA,N)
        # dema 去量纲
        df[f'DEMA_bh_{n}'] = dema / (ema + eps) - 1
        df[f'DEMA_bh_{n}'] = df[f'DEMA_bh_{n}'].shift(1)
        extra_agg_dict[f'DEMA_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DEMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DEMA', 'DEMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DEMA']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_APZ(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # APZ 指标
    for n in back_hour_list:
        """
        N=10
        M=20
        PARAM=2
        VOL=EMA(EMA(HIGH-LOW,N),N)
        UPPER=EMA(EMA(CLOSE,M),M)+PARAM*VOL
        LOWER= EMA(EMA(CLOSE,M),M)-PARAM*VOL
        APZ（Adaptive Price Zone 自适应性价格区间）与布林线 Bollinger 
        Band 和肯通纳通道 Keltner Channel 很相似，都是根据价格波动性围
        绕均线而制成的价格通道。只是在这三个指标中计算价格波动性的方
        法不同。在布林线中用了收盘价的标准差，在肯通纳通道中用了真波
        幅 ATR，而在 APZ 中运用了最高价与最低价差值的 N 日双重指数平
        均来反映价格的波动幅度。
        """
        df['hl'] = df['high'] - df['low'] # HIGH-LOW,
        df['ema_hl'] = df['hl'].ewm(n, adjust=False).mean() # EMA(HIGH-LOW,N)
        df['vol'] = df['ema_hl'].ewm(n, adjust=False).mean() # VOL=EMA(EMA(HIGH-LOW,N),N)

        # 计算通道 可以作为CTA策略 作为因子的时候进行改造
        df['ema_close'] = df['close'].ewm(2 * n, adjust=False).mean() # EMA(CLOSE,M)
        df['ema_ema_close'] = df['ema_close'].ewm(2 * n, adjust=False).mean() # EMA(EMA(CLOSE,M),M)
        # EMA去量纲
        df[f'APZ_bh_{n}'] = df['vol'] / (df['ema_ema_close'] + eps)
        df[f'APZ_bh_{n}'] = df[f'APZ_bh_{n}'].shift(1)
        extra_agg_dict[f'APZ_bh_{n}'] = 'first'
        # 删除中间数据
        del df['hl']
        del df['ema_hl']
        del df['vol']
        del df['ema_close']
        del df['ema_ema_close']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'APZ_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['APZ', 'APZ_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['APZ']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_ASI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # ASI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        M=20
        ASI=CUMSUM(SI)
        ASIMA=MA(ASI,M)
        由于 SI 的波动性比较大，所以我们一般对 SI 累计求和得到 ASI 并捕
        捉 ASI 的变化趋势。一般我们不会直接看 ASI 的数值（对 SI 累计求
        和的求和起点不同会导致求出 ASI 的值不同），而是会观察 ASI 的变
        化方向。我们利用 ASI 与其均线的交叉来产生交易信号,上穿/下穿均
        线时买入/卖出。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1)) # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1)) # B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1)) # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1)) # D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1) # K=MAX(A,B)

        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D'] # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D'] # R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D'] # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'], df['R2']) # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] > df['A']) & (df['C'] >= df['B']), df['R3'], df['R4']) # R=IF((C>=A) & (C>=B),R3,R4)
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / (eps + df['R'] * n) * df['K']  # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M

        df['ASI'] = df['SI'].cumsum() # ASI=CUMSUM(SI)
        df['ASI_MA'] = df['ASI'].rolling(n, min_periods=1).mean() # ASIMA=MA(ASI,M)

        df[f'ASI_bh_{n}'] = df['ASI'] / (df['ASI_MA'] + eps) - 1
        df[f'ASI_bh_{n}'] = df[f'ASI_bh_{n}'].shift(1)
        extra_agg_dict[f'ASI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']
        del df['ASI']
        del df['ASI_MA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ASI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ASI', 'ASI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ASI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_CR(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # CR 指标
    for n in back_hour_list:
        """
        N=20
        TYP=(HIGH+LOW+CLOSE)/3
        H=MAX(HIGH-REF(TYP,1),0)
        L=MAX(REF(TYP,1)-LOW,0)
        CR=SUM(H,N)/SUM(L,N)*100
        CR 与 AR、BR 类似。CR 通过比较最高价、最低价和典型价格来衡
        量市场人气，其衡量昨日典型价格在今日最高价、最低价之间的位置。
        CR 超过 200 时，表示股价上升强势；CR 低于 50 时，表示股价下跌
        强势。如果 CR 上穿 200/下穿 50 则产生买入/卖出信号。
        """
        df['TYP'] = (df['high'] + df['low'] + df['close']) / 3 # TYP=(HIGH+LOW+CLOSE)/3
        df['H_TYP'] = df['high'] - df['TYP'].shift(1) # HIGH-REF(TYP,1)
        df['H'] = np.where(df['high'] > df['TYP'].shift(1), df['H_TYP'], 0) # H=MAX(HIGH-REF(TYP,1),0)
        df['L_TYP'] = df['TYP'].shift(1) - df['low'] # REF(TYP,1)-LOW
        df['L'] = np.where(df['TYP'].shift(1) > df['low'], df['L_TYP'], 0) # L=MAX(REF(TYP,1)-LOW,0)
        df['CR'] = df['H'].rolling(n, min_periods=1).sum() / df['L'].rolling(n, min_periods=1).sum() * 100 # CR=SUM(H,N)/SUM(L,N)*100
        df[f'CR_bh_{n}'] = df['CR'].shift(1)
        extra_agg_dict[f'CR_bh_{n}'] = 'first'
        # 删除中间数据
        del df['TYP']
        del df['H_TYP']
        del df['H']
        del df['L_TYP']
        del df['L']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CR_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CR', 'CR_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CR']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_BOP(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # BOP 指标
    for n in back_hour_list:
        """
        N=20
        BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)
        BOP 的变化范围为-1 到 1，用来衡量收盘价与开盘价的距离（正、负
        距离）占最高价与最低价的距离的比例，反映了市场的多空力量对比。
        如果 BOP>0，则多头更占优势；BOP<0 则说明空头更占优势。BOP
        越大，则说明价格被往最高价的方向推动得越多；BOP 越小，则说
        明价格被往最低价的方向推动得越多。我们可以用 BOP 上穿/下穿 0
        线来产生买入/卖出信号。
        """
        df['co'] = df['close'] - df['open'] #  CLOSE-OPEN
        df['hl'] = df['high'] - df['low'] # HIGH-LOW
        df['BOP'] = (df['co'] / df['hl']).rolling(n, min_periods=1).mean() # BOP=MA((CLOSE-OPEN)/(HIGH-LOW),N)

        df[f'BOP_bh_{n}'] = df['BOP'].shift(1)
        extra_agg_dict[f'BOP_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['co']
        del df['hl']
        del df['BOP']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'BOP_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['BOP', 'BOP_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['BOP']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_HULLMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # HULLMA 指标
    for n in back_hour_list:
        """
        N=20,80
        X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        HULLMA=EMA(X,[√𝑁])
        HULLMA 也是均线的一种，相比于普通均线有着更低的延迟性。我们
        用短期均线上/下穿长期均线来产生买入/卖出信号。
        """
        ema1 = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,[N/2])
        ema2 = df['close'].ewm(n * 2, adjust=False).mean() # EMA(CLOSE,N)
        df['X'] = 2 * ema1 - ema2 # X=2*EMA(CLOSE,[N/2])-EMA(CLOSE,N)
        df['HULLMA'] = df['X'].ewm(int(np.sqrt(2 * n)), adjust=False).mean() # HULLMA=EMA(X,[√𝑁])
        # 去量纲
        df[f'HULLMA_bh_{n}'] = df['HULLMA'].shift(1) - 1
        extra_agg_dict[f'HULLMA_bh_{n}'] = 'first'
        # 删除过程数据
        del df['X']
        del df['HULLMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'HULLMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['HULLMA', 'HULLMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['HULLMA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_COPP(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # COPP 指标
    for n in back_hour_list:
        """
        RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        COPP=WMA(RC,M)
        COPP 指标用不同时间长度的价格变化率的加权移动平均值来衡量
        动量。如果 COPP 上穿/下穿 0 则产生买入/卖出信号。
        """
        df['RC'] = 100 * ((df['close'] - df['close'].shift(n)) / (df['close'].shift(n) + eps) + (
                df['close'] - df['close'].shift(2 * n)) / (df['close'].shift(2 * n) + eps)) # RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
        df['COPP'] = df['RC'].rolling(n, min_periods=1).mean() # COPP=WMA(RC,M)  使用ma代替wma
        df[f'COPP_bh_{n}'] = df['COPP'].shift(1)
        extra_agg_dict[f'COPP_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['RC']
        del df['COPP']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'COPP_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['COPP', 'COPP_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['COPP']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_RSIH(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # RSIH
    for n in back_hour_list:
        """
        N1=40
        N2=120
        CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
        OSE,1),0)
        RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLO
        SE,1)),N1,1)*100
        RSI_SIGNAL=EMA(RSI,N2)
        RSIH=RSI-RSI_SIGNAL
        RSI 指标的一个缺点波动性太大，为了使其更平滑我们可以对其作移
        动平均处理。类似于由 MACD 产生 MACD_SIGNAL 并取其差得到
        MACD_HISTOGRAM，我们对 RSI 作移动平均得到 RSI_SIGNAL，
        取两者的差得到 RSI HISTOGRAM。当 RSI HISTORGRAM 上穿 0
        时产生买入信号；当 RSI HISTORGRAM 下穿 0 产生卖出信号。
        """
        # CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
        # sma_diff_pos = df['close_diff_pos'].rolling(n, min_periods=1).mean()
        sma_diff_pos = df['close_diff_pos'].ewm(span=n).mean() # SMA(CLOSE_DIFF_POS,N1,1)
        # abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).rolling(n, min_periods=1).mean()
        # SMA(ABS(CLOSE-REF(CLOSE,1)),N1,1
        abs_sma_diff_pos = abs(df['close'] - df['close'].shift(1)).ewm(span=n).mean()
        # RSI=SMA(CLOSE_DIFF_POS,N1,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N1,1)*100
        df['RSI'] = sma_diff_pos / (abs_sma_diff_pos + eps) * 100
        # RSI_SIGNAL=EMA(RSI,N2)
        df['RSI_ema'] = df['RSI'].ewm(4 * n, adjust=False).mean()
        # RSIH=RSI-RSI_SIGNAL
        df['RSIH'] = df['RSI'] - df['RSI_ema']

        df[f'RSIH_bh_{n}'] = df['RSIH'].shift(1)
        extra_agg_dict[f'RSIH_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['close_diff_pos']
        del df['RSI']
        del df['RSI_ema']
        del df['RSIH']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RSIH_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RSIH', 'RSIH_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RSIH']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_HLMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # HLMA 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        HMA=MA(HIGH,N1)
        LMA=MA(LOW,N2)
        HLMA 指标是把普通的移动平均中的收盘价换为最高价和最低价分
        别得到 HMA 和 LMA。当收盘价上穿 HMA/下穿 LMA 时产生买入/卖
        出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean() # HMA=MA(HIGH,N1)
        lma = df['low'].rolling(n, min_periods=1).mean() # LMA=MA(LOW,N2)
        df['HLMA'] = hma - lma # 可自行改造
        df['HLMA_mean'] = df['HLMA'].rolling(n, min_periods=1).mean()

        # 去量纲
        df[f'HLMA_bh_{n}'] = df['HLMA'] / (df['HLMA_mean'] + eps) - 1
        df[f'HLMA_bh_{n}'] = df[f'HLMA_bh_{n}'].shift(1)
        extra_agg_dict[f'HLMA_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['HLMA']
        del df['HLMA_mean']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'HLMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['HLMA', 'HLMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['HLMA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_HLMA2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # HLMA 指标魔改
    for n in back_hour_list:
        """
        N1=20
        N2=20
        HMA=MA(HIGH,N1)
        LMA=MA(LOW,N2)
        HLMA 指标是把普通的移动平均中的收盘价换为最高价和最低价分
        别得到 HMA 和 LMA。当收盘价上穿 HMA/下穿 LMA 时产生买入/卖
        出信号。
        """
        hma = df['high'].rolling(n, min_periods=1).mean() # HMA=MA(HIGH,N1)
        lma = df['low'].rolling(n, min_periods=1).mean() # LMA=MA(LOW,N2)
        # 去量纲
        df[f'HLMA2_bh_{n}'] = (df['close'] - hma) / (hma + eps)
        df[f'HLMA2_bh_{n}'] = df[f'HLMA2_bh_{n}'].shift(1)
        extra_agg_dict[f'HLMA2_bh_{n}'] = 'first'
        # 删除中间过程数据

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'HLMA2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['HLMA2', 'HLMA2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['HLMA2']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_TRIX(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # TRIX 指标
    for n in back_hour_list:
        """
        TRIPLE_EMA=EMA(EMA(EMA(CLOSE,N),N),N)
        TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
        TRIX 求价格的三重指数移动平均的变化率。当 TRIX>0 时，当前可
        能处于上涨趋势；当 TRIX<0 时，当前可能处于下跌趋势。TRIX 相
        比于普通移动平均的优点在于它通过三重移动平均去除了一些小的
        趋势和市场的噪音。我们可以通过 TRIX 上穿/下穿 0 线产生买入/卖
        出信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean() # EMA(EMA(EMA(CLOSE,N),N),N)
        # TRIX=(TRIPLE_EMA-REF(TRIPLE_EMA,1))/REF(TRIPLE_EMA,1)
        df['TRIX'] = (df['ema_ema_ema'] - df['ema_ema_ema'].shift(1)) / (df['ema_ema_ema'].shift(1) + eps)

        df[f'TRIX_bh_{n}'] = df['TRIX'].shift(1)
        extra_agg_dict[f'TRIX_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TRIX']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TRIX_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TRIX', 'TRIX_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TRIX']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_WC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # WC 指标
    for n in back_hour_list:
        """
        WC=(HIGH+LOW+2*CLOSE)/4
        N1=20
        N2=40
        EMA1=EMA(WC,N1)
        EMA2=EMA(WC,N2)
        WC 也可以用来代替收盘价构造一些技术指标（不过相对比较少用
        到）。我们这里用 WC 的短期均线和长期均线的交叉来产生交易信号。
        """
        WC = (df['high'] + df['low'] + 2 * df['close']) / 4  # WC=(HIGH+LOW+2*CLOSE)/4
        df['ema1'] = WC.ewm(n, adjust=False).mean()  # EMA1=EMA(WC,N1)
        df['ema2'] = WC.ewm(2 * n, adjust=False).mean() # EMA2=EMA(WC,N2)
        # 去量纲
        df[f'WC_bh_{n}'] = df['ema1'] / (df['ema2'] + eps) - 1
        df[f'WC_bh_{n}'] = df[f'WC_bh_{n}'].shift(1)
        extra_agg_dict[f'WC_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ema1']
        del df['ema2']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'WC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['WC', 'WC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['WC']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_ADX(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # ADX 指标
    for n in back_hour_list:
        """
        N1=14
        MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
        MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
        XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
        PDM=SUM(XPDM,N1)
        XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
        NDM=SUM(XNDM,N1)
        TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
        TR=SUM(TR,N1)
        DI+=PDM/TR
        DI-=NDM/TR
        ADX 指标计算过程中的 DI+与 DI-指标用相邻两天的最高价之差与最
        低价之差来反映价格的变化趋势。当 DI+上穿 DI-时，产生买入信号；
        当 DI+下穿 DI-时，产生卖出信号。
        """
        # MAX_HIGH=IF(HIGH>REF(HIGH,1),HIGH-REF(HIGH,1),0)
        df['max_high'] = np.where(df['high'] > df['high'].shift(1), df['high'] - df['high'].shift(1), 0)
        # MAX_LOW=IF(REF(LOW,1)>LOW,REF(LOW,1)-LOW,0)
        df['max_low'] = np.where(df['low'].shift(1) > df['low'], df['low'].shift(1) - df['low'], 0)
        # XPDM=IF(MAX_HIGH>MAX_LOW,HIGH-REF(HIGH,1),0)
        df['XPDM'] = np.where(df['max_high'] > df['max_low'], df['high'] - df['high'].shift(1), 0)
        # PDM=SUM(XPDM,N1)
        df['PDM'] = df['XPDM'].rolling(n, min_periods=1).sum()
        # XNDM=IF(MAX_LOW>MAX_HIGH,REF(LOW,1)-LOW,0)
        df['XNDM'] = np.where(df['max_low'] > df['max_high'], df['low'].shift(1) - df['low'], 0)
        # NDM=SUM(XNDM,N1)
        df['NDM'] = df['XNDM'].rolling(n, min_periods=1).sum()
        # ABS(HIGH-LOW)
        df['c1'] = abs(df['high'] - df['low'])
        # ABS(HIGH-CLOSE)
        df['c2'] = abs(df['high'] - df['close'])
        # ABS(LOW-CLOSE)
        df['c3'] = abs(df['low'] - df['close'])
        # TR=MAX([ABS(HIGH-LOW),ABS(HIGH-CLOSE),ABS(LOW-CLOSE)])
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
        # TR=SUM(TR,N1)
        df['TR_sum'] = df['TR'].rolling(n, min_periods=1).sum()
        # DI+=PDM/TR
        df['DI+'] = df['PDM'] / (df['TR'] + eps)
        # DI-=NDM/TR
        df['DI-'] = df['NDM'] / (df['TR'] + eps)

        df[f'ADX_DI+_bh_{n}'] = df['DI+'].shift(1)
        df[f'ADX_DI-_bh_{n}'] = df['DI-'].shift(1)
        # 去量纲
        df['ADX'] = (df['PDM'] + df['NDM']) / (df['TR'] + eps)

        df[f'ADX_bh_{n}'] = df['ADX'].shift(1)
        extra_agg_dict[f'ADX_bh_{n}'] = 'first'
        extra_agg_dict[f'ADX_DI+_bh_{n}'] = 'first'
        extra_agg_dict[f'ADX_DI-_bh_{n}'] = 'first'
        # 删除中间过程数据
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
        del df['DI+']
        del df['DI-']
        del df['ADX']
        if add:
            for _ in [f'ADX_bh_{n}', f'ADX_DI+_bh_{n}', f'ADX_DI-_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ADX','ADX_DI+','ADX_DI-', 'ADX_diff','ADX_DI+_diff','ADX_DI-_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ADX','ADX_DI+','ADX_DI-']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_FISHER(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # FISHER指标
    for n in back_hour_list:
        """
        N=20
        PARAM=0.3
        PRICE=(HIGH+LOW)/2
        PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-
        0.5)
        PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99 
        PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        PRICE_CHANGE=PARAM*PRICE_CH+(1-PARAM)*REF(PRICE_CHANGE,1)
        FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        PRICE_CH 用来衡量当前价位于过去 N 天的最高价和最低价之间的
        位置。Fisher Transformation 是一个可以把股价数据变为类似于正态
        分布的方法。Fisher 指标的优点是减少了普通技术指标的滞后性。
        """
        PARAM = 1 / n
        df['price'] = (df['high'] + df['low']) / 2 # PRICE=(HIGH+LOW)/2
        df['min_low'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N)
        df['max_high'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N)
        df['price_ch'] = 2 * (df['price'] - df['min_low']) / (df['max_high'] - df['low'] + eps) - 0.5 #         PRICE_CH=2*(PRICE-MIN(LOW,N)/(MAX(HIGH,N)-MIN(LOW,N))-0.5)
        df['price_change'] = PARAM * df['price_ch'] + (1 - PARAM) * df['price_ch'].shift(1)
        df['price_change'] = np.where(df['price_change'] > 0.99, 0.999, df['price_change']) # PRICE_CHANGE=0.999 IF PRICE_CHANGE>0.99
        df['price_change'] = np.where(df['price_change'] < -0.99, -0.999, df['price_change']) # PRICE_CHANGE=-0.999 IF PRICE_CHANGE<-0.99
        # 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change']))
        df['FISHER'] = 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change'] + eps))
        # FISHER=0.5*REF(FISHER,1)+0.5*log((1+PRICE_CHANGE)/(1-PRICE_CHANGE))
        df['FISHER'] = 0.5 * df['FISHER'].shift(1) + 0.5 * np.log((1 + df['price_change']) / (1 - df['price_change'] + eps))

        df[f'FISHER_bh_{n}'] = df['FISHER'].shift(1)
        extra_agg_dict[f'FISHER_bh_{n}'] = 'first'
        # 删除中间数据
        del df['price']
        del df['min_low']
        del df['max_high']
        del df['price_ch']
        del df['price_change']
        del df['FISHER']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'FISHER_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['FISHER', 'FISHER_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['FISHER']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_Demaker(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # Demakder 指标
    for n in back_hour_list:
        """
        N=20
        Demax=HIGH-REF(HIGH,1)
        Demax=IF(Demax>0,Demax,0)
        Demin=REF(LOW,1)-LOW
        Demin=IF(Demin>0,Demin,0)
        Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        当 Demaker>0.7 时上升趋势强烈，当 Demaker<0.3 时下跌趋势强烈。
        当 Demaker 上穿 0.7/下穿 0.3 时产生买入/卖出信号。
        """
        df['Demax'] = df['high'] - df['high'].shift(1) # Demax=HIGH-REF(HIGH,1)
        df['Demax'] = np.where(df['Demax'] > 0, df['Demax'], 0) # Demax=IF(Demax>0,Demax,0)
        df['Demin'] = df['low'].shift(1) - df['low'] # Demin=REF(LOW,1)-LOW
        df['Demin'] = np.where(df['Demin'] > 0, df['Demin'], 0) # Demin=IF(Demin>0,Demin,0)
        df['Demax_ma'] = df['Demax'].rolling(n, min_periods=1).mean() # MA(Demax,N)
        df['Demin_ma'] = df['Demin'].rolling(n, min_periods=1).mean() # MA(Demin,N)
        df['Demaker'] = df['Demax_ma'] / (df['Demax_ma'] + df['Demin_ma'] + eps) # Demaker=MA(Demax,N)/(MA(Demax,N)+MA(Demin,N))
        df[f'Demaker_bh_{n}'] = df['Demaker'].shift(1)
        extra_agg_dict[f'Demaker_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['Demax']
        del df['Demin']
        del df['Demax_ma']
        del df['Demin_ma']
        del df['Demaker']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'Demaker_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['Demaker', 'Demaker_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['Demaker']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_IC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # IC 指标
    for n in back_hour_list:
        """
        N1=9
        N2=26
        N3=52
        TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        SPAN_A=(TS+KS)/2
        SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2
        在 IC 指标中，SPAN_A 与 SPAN_B 之间的部分称为云。如果价格在
        云上，则说明是上涨趋势（如果 SPAN_A>SPAN_B，则上涨趋势强
        烈；否则上涨趋势较弱）；如果价格在云下，则为下跌趋势（如果
        SPAN_A<SPAN_B，则下跌趋势强烈；否则下跌趋势较弱）。该指
        标的使用方式与移动平均线有许多相似之处，比如较快的线（TS）突
        破较慢的线（KS），价格突破 KS,价格突破云，SPAN_A 突破 SPAN_B
        等。我们产生信号的方式是：如果价格在云上方 SPAN_A>SPAN_B，
        则当价格上穿 KS 时买入；如果价格在云下方且 SPAN_A<SPAN_B，
        则当价格下穿 KS 时卖出。
        """
        n2 = 3 * n
        n3 = 2 * n2
        df['max_high_1'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N1)
        df['min_low_1'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N1)
        df['TS'] = (df['max_high_1'] + df['min_low_1']) / 2 # TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['max_high_2'] = df['high'].rolling(n2, min_periods=1).max() # MAX(HIGH,N2)
        df['min_low_2'] = df['low'].rolling(n2, min_periods=1).min() # MIN(LOW,N2)
        df['KS'] = (df['max_high_2'] + df['min_low_2']) / 2 # KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        df['span_A'] = (df['TS'] + df['KS']) / 2 # SPAN_A=(TS+KS)/2
        df['max_high_3'] = df['high'].rolling(n3, min_periods=1).max() # MAX(HIGH,N3)
        df['min_low_3'] = df['low'].rolling(n3, min_periods=1).min() # MIN(LOW,N3)
        df['span_B'] = (df['max_high_3'] + df['min_low_3']) / 2 # SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2

        # 去量纲
        df[f'IC_bh_{n}'] = df['span_A'] / (df['span_B'] + eps)
        df[f'IC_bh_{n}'] = df[f'IC_bh_{n}'].shift(1)
        extra_agg_dict[f'IC_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['max_high_1']
        del df['max_high_2']
        del df['max_high_3']
        del df['min_low_1']
        del df['min_low_2']
        del df['min_low_3']
        del df['TS']
        del df['KS']
        del df['span_A']
        del df['span_B']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'IC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['IC', 'IC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['IC']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_TSI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # TSI 指标
    for n in back_hour_list:
        """
        N1=25
        N2=13
        TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(
        CLOSE-REF(CLOSE,1)),N1),N2)*100
        TSI 是一种双重移动平均指标。与常用的移动平均指标对收盘价取移
        动平均不同，TSI 对两天收盘价的差值取移动平均。如果 TSI 上穿 10/
        下穿-10 则产生买入/卖出指标。
        """
        n1 = 2 * n
        df['diff_close'] = df['close'] - df['close'].shift(1)  # CLOSE-REF(CLOSE,1)
        df['ema'] = df['diff_close'].ewm(n1, adjust=False).mean()  # EMA(CLOSE-REF(CLOSE,1),N1)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean()  # EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)

        df['abs_diff_close'] = abs(df['diff_close'])  # ABS(CLOSE-REF(CLOSE,1))
        df['abs_ema'] = df['abs_diff_close'].ewm(n1, adjust=False).mean()  # EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        df['abs_ema_ema'] = df['abs_ema'].ewm(n, adjust=False).mean()  # EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1)
        # TSI=EMA(EMA(CLOSE-REF(CLOSE,1),N1),N2)/EMA(EMA(ABS(CLOSE-REF(CLOSE,1)),N1),N2)*100
        df['TSI'] = df['ema_ema'] / (df['abs_ema_ema'] + eps) * 100

        df[f'TSI_bh_{n}'] = df['TSI'].shift(1)
        extra_agg_dict[f'TSI_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['diff_close']
        del df['ema']
        del df['ema_ema']
        del df['abs_diff_close']
        del df['abs_ema']
        del df['abs_ema_ema']
        del df['TSI']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TSI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TSI', 'TSI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TSI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_LMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # LMA 指标
    for n in back_hour_list:
        """
        N=20
        LMA=MA(LOW,N)
        LMA 为简单移动平均把收盘价替换为最低价。如果最低价上穿/下穿
        LMA 则产生买入/卖出信号。
        """
        df['low_ma'] = df['low'].rolling(n, min_periods=1).mean() # LMA=MA(LOW,N)
        # 进行去量纲
        df[f'LMA_bh_{n}'] = df['low'] / (df['low_ma'] + eps) - 1
        df[f'LMA_bh_{n}'] = df[f'LMA_bh_{n}'].shift(1)
        extra_agg_dict[f'LMA_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['low_ma']

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'LMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['LMA', 'LMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['LMA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_IMI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # IMI 指标
    for n in back_hour_list:
        """
        N=14
        INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        IMI=INC/(INC+DEC)
        IMI 的计算方法与 RSI 很相似。其区别在于，在 IMI 计算过程中使用
        的是收盘价和开盘价，而 RSI 使用的是收盘价和前一天的收盘价。所
        以，RSI 做的是前后两天的比较，而 IMI 做的是同一个交易日内的比
        较。如果 IMI 上穿 80，则产生买入信号；如果 IMI 下穿 20，则产生
        卖出信号。
        """
        df['INC'] = np.where(df['close'] > df['open'], df['close'] - df['open'], 0) # IF(CLOSE>OPEN,CLOSE-OPEN,0)
        df['INC_sum'] = df['INC'].rolling(n, min_periods=1).sum() # INC=SUM(IF(CLOSE>OPEN,CLOSE-OPEN,0),N)
        df['DEC'] = np.where(df['open'] > df['close'], df['open'] - df['close'], 0) # IF(OPEN>CLOSE,OPEN-CLOSE,0)
        df['DEC_sum'] = df['DEC'].rolling(n, min_periods=1).sum() # DEC=SUM(IF(OPEN>CLOSE,OPEN-CLOSE,0),N)
        df['IMI'] = df['INC_sum'] / (df['INC_sum'] + df['DEC_sum'] + eps) # IMI=INC/(INC+DEC)

        df[f'IMI_bh_{n}'] = df['IMI'].shift(1)
        extra_agg_dict[f'IMI_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['INC']
        del df['INC_sum']
        del df['DEC']
        del df['DEC_sum']
        del df['IMI']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'IMI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['IMI', 'IMI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['IMI']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_VI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # VI 指标
    for n in back_hour_list:
        """
        TR=MAX([ABS(HIGH-LOW),ABS(LOW-REF(CLOSE,1)),ABS(HIG
        H-REF(CLOSE,1))])
        VMPOS=ABS(HIGH-REF(LOW,1))
        VMNEG=ABS(LOW-REF(HIGH,1))
        N=40
        SUMPOS=SUM(VMPOS,N)
        SUMNEG=SUM(VMNEG,N)
        TRSUM=SUM(TR,N)
        VI+=SUMPOS/TRSUM
        VI-=SUMNEG/TRSUM
        VI 指标可看成 ADX 指标的变形。VI 指标中的 VI+与 VI-与 ADX 中的
        DI+与 DI-类似。不同的是 ADX 中用当前高价与前一天高价的差和当
        前低价与前一天低价的差来衡量价格变化，而 VI 指标用当前当前高
        价与前一天低价和当前低价与前一天高价的差来衡量价格变化。当
        VI+上穿/下穿 VI-时，多/空的信号更强，产生买入/卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low']) # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1)) # ABS(LOW-REF(CLOSE,1)
        df['c3'] = abs(df['high'] - df['close'].shift(1))# ABS(HIGH-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX([ABS(HIGH-LOW),ABS(LOW-REF(CLOSE,1)),ABS(HIGH-REF(CLOSE,1))])

        df['VMPOS'] = abs(df['high'] - df['low'].shift(1)) # VMPOS=ABS(HIGH-REF(LOW,1))
        df['VMNEG'] = abs(df['low'] - df['high'].shift(1)) # VMNEG=ABS(LOW-REF(HIGH,1))
        df['sum_pos'] = df['VMPOS'].rolling(n, min_periods=1).sum()  # SUMPOS=SUM(VMPOS,N)
        df['sum_neg'] = df['VMNEG'].rolling(n, min_periods=1).sum() # SUMNEG=SUM(VMNEG,N)

        df['sum_tr'] = df['TR'].rolling(n, min_periods=1).sum() # TRSUM=SUM(TR,N)
        df['VI+'] = df['sum_pos'] / (df['sum_tr'] + eps) # VI+=SUMPOS/TRSUM
        df['VI-'] = df['sum_neg'] / (df['sum_tr'] + eps) # VI-=SUMNEG/TRSUM
        df[f'VI+_bh_{n}'] = df['VI+'].shift(1)
        df[f'VI-_bh_{n}'] = df['VI-'].shift(1)
        extra_agg_dict[f'VI+_bh_{n}'] = 'first'
        extra_agg_dict[f'VI-_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['VMPOS']
        del df['VMNEG']
        del df['sum_pos']
        del df['sum_neg']
        del df['sum_tr']
        del df['VI+']
        del df['VI-']
        if add:
            for _ in [f'VI+_bh_{n}', f'VI-_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['VI+', 'VI-', 'VI+_diff', 'VI-_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['VI+', 'VI-']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_RWI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # RWI 指标
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(
        CLOSE,1)-LOW))
        ATR=MA(TR,N)
        RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        RWI（随机漫步指标）对一段时间股票的随机漫步区间与真实运动区
        间进行比较以判断股票价格的走势。
        如果 RWIH>1，说明股价长期是上涨趋势，则产生买入信号；
        如果 RWIL>1，说明股价长期是下跌趋势，则产生卖出信号。
        """
        df['c1'] = abs(df['high'] - df['low']) # ABS(HIGH-LOW)
        df['c2'] = abs(df['close'] - df['close'].shift(1)) # ABS(HIGH-REF(CLOSE,1))
        df['c3'] = abs(df['high'] - df['close'].shift(1)) # ABS(REF(CLOSE,1)-LOW)
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-LOW))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean() # ATR=MA(TR,N)
        df['RWIH'] = (df['high'] - df['low'].shift(1)) / (df['ATR'] * np.sqrt(n) + eps) # RWIH=(HIGH-REF(LOW,1))/(ATR*√N)
        df['RWIL'] = (df['high'].shift(1) - df['low']) / (df['ATR'] * np.sqrt(n) + eps) # RWIL=(REF(HIGH,1)-LOW)/(ATR*√N)
        df[f'RWIH_bh_{n}'] = df['RWIH'].shift(1)
        df[f'RWIL_bh_{n}'] = df['RWIL'].shift(1)
        extra_agg_dict[f'RWIH_bh_{n}'] = 'first'
        extra_agg_dict[f'RWIL_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['RWIH']
        del df['RWIL']
        if add:
            for _ in [f'RWIH_bh_{n}', f'RWIL_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RWIH', 'RWIL', 'RWIH_diff', 'RWIL_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RWIH', 'RWIL']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_CMO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # CMO 指标
    for n in back_hour_list:
        """
        N=20
        SU=SUM(MAX(CLOSE-REF(CLOSE,1),0),N)
        SD=SUM(MAX(REF(CLOSE,1)-CLOSE,0),N)
        CMO=(SU-SD)/(SU+SD)*100
        CMO指标用过去N天的价格上涨量和价格下跌量得到，可以看作RSI
        指标的变形。CMO>(<)0 表示当前处于上涨（下跌）趋势，CMO 越
        大（小）则当前上涨（下跌）趋势越强。我们用 CMO 上穿 30/下穿-30
        来产生买入/卖出信号。
        """
        # MAX(CLOSE-REF(CLOSE,1), 0
        df['max_su'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)

        df['sum_su'] = df['max_su'].rolling(n, min_periods=1).sum() # SU=SUM(MAX(CLOSE-REF(CLOSE,1),0),N)
        # MAX(REF(CLOSE,1)-CLOSE,0)
        df['max_sd'] = np.where(df['close'].shift(1) > df['close'], df['close'].shift(1) - df['close'], 0)
        # SD=SUM(MAX(REF(CLOSE,1)-CLOSE,0),N)
        df['sum_sd'] = df['max_sd'].rolling(n, min_periods=1).sum()
        # CMO=(SU-SD)/(SU+SD)*100
        df['CMO'] = (df['sum_su'] - df['sum_sd']) / (df['sum_su'] + df['sum_sd'] + eps) * 100

        df[f'CMO_bh_{n}'] = df['CMO'].shift(1)
        extra_agg_dict[f'CMO_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['max_su']
        del df['sum_su']
        del df['max_sd']
        del df['sum_sd']
        del df['CMO']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CMO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CMO','CMO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CMO']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_OSC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # OSC 指标
    for n in back_hour_list:
        """
        N=40
        M=20
        OSC=CLOSE-MA(CLOSE,N)
        OSCMA=MA(OSC,M)
        OSC 反映收盘价与收盘价移动平均相差的程度。如果 OSC 上穿/下 穿 OSCMA 则产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(2 * n, min_periods=1).mean() #MA(CLOSE,N)
        df['OSC'] = df['close'] - df['ma'] # OSC=CLOSE-MA(CLOSE,N)
        df['OSCMA'] = df['OSC'].rolling(n, min_periods=1).mean() # OSCMA=MA(OSC,M)
        df[f'OSC_bh_{n}'] = df['OSCMA'].shift(1)
        extra_agg_dict[f'OSC_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ma']
        del df['OSC']
        del df['OSCMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'OSC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['OSC', 'OSC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['OSC']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_CLV(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # CLV 指标
    for n in back_hour_list:
        """
        N=60
        CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        CLVMA=MA(CLV,N)
        CLV 用来衡量收盘价在最低价和最高价之间的位置。当
        CLOSE=HIGH 时，CLV=1;当 CLOSE=LOW 时，CLV=-1;当 CLOSE
        位于 HIGH 和 LOW 的中点时，CLV=0。CLV>0（<0），说明收盘价
        离最高（低）价更近。我们用 CLVMA 上穿/下穿 0 来产生买入/卖出
        信号。
        """
        # CLV=(2*CLOSE-LOW-HIGH)/(HIGH-LOW)
        df['CLV'] = (2 * df['close'] - df['low'] - df['high']) / (df['high'] - df['low'] + eps)
        df['CLVMA'] = df['CLV'].rolling(n, min_periods=1).mean() # CLVMA=MA(CLV,N)
        df[f'CLV_bh_{n}'] = df['CLVMA'].shift(1)
        extra_agg_dict[f'CLV_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['CLV']
        del df['CLVMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CLV_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CLV', 'CLV_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CLV']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_WAD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    #  WAD 指标
    for n in back_hour_list:
        """
        TRH=MAX(HIGH,REF(CLOSE,1))
        TRL=MIN(LOW,REF(CLOSE,1))
        AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH) 
        AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))  # 该指标怀疑有误
        WAD=CUMSUM(AD)
        N=20
        WADMA=MA(WAD,N)
        我们用 WAD 上穿/下穿其均线来产生买入/卖出信号。
        """
        df['ref_close'] = df['close'].shift(1) # REF(CLOSE,1)
        df['TRH'] = df[['high', 'ref_close']].max(axis=1) # TRH=MAX(HIGH,REF(CLOSE,1))
        df['TRL'] = df[['low', 'ref_close']].min(axis=1) # TRL=MIN(LOW,REF(CLOSE,1))
        # AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH)
        df['AD'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['TRL'], df['close'] - df['TRH'])
        # AD=IF(CLOSE>REF(CLOSE,1),0,CLOSE-REF(CLOSE,1))
        df['AD'] = np.where(df['close'] > df['close'].shift(1), 0, df['close'] - df['close'].shift(1))
        # WAD=CUMSUM(AD)
        df['WAD'] = df['AD'].cumsum()
        # WADMA=MA(WAD,N)
        df['WADMA'] = df['WAD'].rolling(n, min_periods=1).mean()
        # 去量纲
        df[f'WAD_bh_{n}'] = df['WAD'] / (df['WADMA'] + eps) - 1
        df[f'WAD_bh_{n}'] = df[f'WAD_bh_{n}'].shift(1)
        extra_agg_dict[f'WAD_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ref_close']
        del df['TRH']
        del df['AD']
        del df['WAD']
        del df['WADMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'WAD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['WAD', 'WAD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['WAD']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_BIAS36(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # BIAS36
    for n in back_hour_list:
        """
        N=6
        BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        MABIAS36=MA(BIAS36,N)
        类似于乖离用来衡量当前价格与移动平均价的差距，三六乖离用来衡
        量不同的移动平均价间的差距。当三六乖离上穿/下穿其均线时，产生
        买入/卖出信号。
        """
        df['ma3'] = df['close'].rolling(n, min_periods=1).mean() # MA(CLOSE,3)
        df['ma6'] = df['close'].rolling(2 * n, min_periods=1).mean() # MA(CLOSE,6)
        df['BIAS36'] = df['ma3'] - df['ma6'] # BIAS36=MA(CLOSE,3)-MA(CLOSE,6)
        df['MABIAS36'] = df['BIAS36'].rolling(2 * n, min_periods=1).mean() # MABIAS36=MA(BIAS36,N)
        # 去量纲
        df[f'BIAS36_bh_{n}'] = df['BIAS36'] / (df['MABIAS36'] + eps)
        df[f'BIAS36_bh_{n}'] = df[f'BIAS36_bh_{n}'].shift(1)
        extra_agg_dict[f'BIAS36_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ma3']
        del df['ma6']
        del df['BIAS36']
        del df['MABIAS36']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'BIAS36_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['BIAS36', 'BIAS36_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['BIAS36']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_TEMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # TEMA 指标
    for n in back_hour_list:
        """
        N=20,40
        TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        TEMA 结合了单重、双重和三重的 EMA，相比于一般均线延迟性较
        低。我们用快、慢 TEMA 的交叉来产生交易信号。
        """
        df['ema'] = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        df['ema_ema'] = df['ema'].ewm(n, adjust=False).mean() # EMA(EMA(CLOSE,N),N)
        df['ema_ema_ema'] = df['ema_ema'].ewm(n, adjust=False).mean() # EMA(EMA(EMA(CLOSE,N),N),N)
        df['TEMA'] = 3 * df['ema'] - 3 * df['ema_ema'] + df['ema_ema_ema'] # TEMA=3*EMA(CLOSE,N)-3*EMA(EMA(CLOSE,N),N)+EMA(EMA(EMA(CLOSE,N),N),N)
        # 去量纲
        df[f'TEMA_bh_{n}'] = df['ema'] / (df['TEMA'] + eps) - 1
        df[f'TEMA_bh_{n}'] = df[f'TEMA_bh_{n}'].shift(1)
        extra_agg_dict[f'TEMA_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ema']
        del df['ema_ema']
        del df['ema_ema_ema']
        del df['TEMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TEMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TEMA', 'TEMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TEMA']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_REG(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # REG 指标
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """

        # df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
        # df['reg'] = df['close'] / df['ref_close'] - 1

        # sklearn 线性回归
        def reg_ols(_y):
            _x = np.arange(n) + 1
            model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
            y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
            return y_pred[-1]

        df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y)) # 求数据拟合的线性回归
        df['reg'] = df['close'] / (df['reg_close'] + eps) - 1

        df[f'REG_bh_{n}'] = df['reg'].shift(1)
        extra_agg_dict[f'REG_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'REG_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['REG', 'REG_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['REG']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_REG2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # REG 指标
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """
        # 由于sklearn计算太慢 使用talib代替
        df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
        df['reg'] = df['close'] / (df['reg_close'] + eps) - 1


        df[f'REG2_bh_{n}'] = df['reg'].shift(1)
        extra_agg_dict[f'REG2_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'REG2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['REG2', 'REG2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['REG2']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_REG3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # REG 指标
    for n in back_hour_list:
        """
        N=40
        X=[1,2,...,N]
        Y=[REF(CLOSE,N-1),...,REF(CLOSE,1),CLOSE]
        做回归得 REG_CLOSE=aX+b
        REG=(CLOSE-REG_CLOSE)/REG_CLOSE
        在过去的 N 天内收盘价对序列[1,2,...,N]作回归得到回归直线，当收盘
        价超过回归直线的一定范围时买入，低过回归直线的一定范围时卖
        出。如果 REG 上穿 0.05/下穿-0.05 则产生买入/卖出信号。
        """
        # 由于sklearn计算太慢 使用talib代替
        df['dif_close'] = df['close'] - df['close'].shift(1)
        df['reg_close'] = talib.LINEARREG(df['dif_close'], timeperiod=n) # 该部分为talib内置求线性回归
        df['reg'] = df['close'] / (df['reg_close'] + eps) - 1

        df[f'REG3_bh_{n}'] = scale_zscore(df['reg'], n)
        df[f'REG3_bh_{n}'] = df[f'REG3_bh_{n}'].shift(1)
        extra_agg_dict[f'REG3_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['reg']
        del df['reg_close']
        del df['dif_close']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'REG3_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['REG3', 'REG3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['REG3']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_PSY(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # PSY 指标
    for n in back_hour_list:
        """
        N=12
        PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        PSY 指标为过去 N 天股价上涨的天数的比例*100，用来衡量投资者
        心理和市场的人气。当 PSY 处于 40 和 60 之间时，多、空力量相对
        平衡，当 PSY 上穿 60 时，多头力量比较强，产生买入信号；当 PSY
        下穿 40 时，空头力量比较强，产生卖出信号。
        """
        df['P'] = np.where(df['close'] > df['close'].shift(1), 1, 0) #IF(CLOSE>REF(CLOSE,1),1,0)

        df['PSY'] = df['P'] / n * 100 # PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        df[f'PSY_bh_{n}'] = df['PSY'].shift(1)
        extra_agg_dict[f'PSY_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['P']
        del df['PSY']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PSY_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PSY', 'PSY_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PSY']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_PSY2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # PSY 指标 公式修正版本
    for n in back_hour_list:
        """
        N=12
        PSY=IF(CLOSE>REF(CLOSE,1),1,0)/N*100
        PSY 指标为过去 N 天股价上涨的天数的比例*100，用来衡量投资者
        心理和市场的人气。当 PSY 处于 40 和 60 之间时，多、空力量相对
        平衡，当 PSY 上穿 60 时，多头力量比较强，产生买入信号；当 PSY
        下穿 40 时，空头力量比较强，产生卖出信号。
        """
        df['P'] = np.where(df['close'] > df['close'].shift(1), 1, 0) #IF(CLOSE>REF(CLOSE,1),1,0)

        df['PSY'] = df['P'].rolling(n, min_periods=1).sum() / n * 100
        df[f'PSY2_bh_{n}'] = df['PSY'].shift(1)
        extra_agg_dict[f'PSY2_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['P']
        del df['PSY2']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PSY2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PSY2', 'PSY2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PSY2']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_DMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # DMA 指标
    for n in back_hour_list:
        """
        DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        AMA=MA(DMA,N1)
        DMA 衡量快速移动平均与慢速移动平均之差。用 DMA 上穿/下穿其
        均线产生买入/卖出信号。
        """
        df['ma1'] = df['close'].rolling(n, min_periods=1).mean() # MA(CLOSE,N1)
        df['ma2'] = df['close'].rolling(n * 3, min_periods=1).mean() # MA(CLOSE,N2)
        df['DMA'] = df['ma1'] - df['ma2'] # DMA=MA(CLOSE,N1)-MA(CLOSE,N2)
        df['AMA'] = df['DMA'].rolling(n, min_periods=1).mean() # AMA=MA(DMA,N1)
        # 去量纲
        df[f'DMA_bh_{n}'] = df['DMA'] / (df['AMA'] + eps) - 1
        df[f'DMA_bh_{n}'] = df[f'DMA_bh_{n}'].shift(1)
        extra_agg_dict[f'DMA_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ma1']
        del df['ma2']
        del df['DMA']
        del df['AMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DMA', 'DMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DMA']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_KST(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # KST 指标
    for n in back_hour_list:
        """
        ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        ROC_MA2=MA(CLOSE -REF(CLOSE,15),10)
        ROC_MA3=MA(CLOSE -REF(CLOSE,20),10)
        ROC_MA4=MA(CLOSE -REF(CLOSE,30),10)
        KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        KST=MA(KST_IND,9)
        KST 结合了不同时间长度的 ROC 指标。如果 KST 上穿/下穿 0 则产
        生买入/卖出信号。
        """
        df['ROC1'] = df['close'] - df['close'].shift(n) # CLOSE-REF(CLOSE,10)
        df['ROC_MA1'] = df['ROC1'].rolling(n, min_periods=1).mean() # ROC_MA1=MA(CLOSE-REF(CLOSE,10),10)
        df['ROC2'] = df['close'] - df['close'].shift(int(n * 1.5))
        df['ROC_MA2'] = df['ROC2'].rolling(n, min_periods=1).mean()
        df['ROC3'] = df['close'] - df['close'].shift(int(n * 2))
        df['ROC_MA3'] = df['ROC3'].rolling(n, min_periods=1).mean()
        df['ROC4'] = df['close'] - df['close'].shift(int(n * 3))
        df['ROC_MA4'] = df['ROC4'].rolling(n, min_periods=1).mean()
        # KST_IND=ROC_MA1+ROC_MA2*2+ROC_MA3*3+ROC_MA4*4
        df['KST_IND'] = df['ROC_MA1'] + df['ROC_MA2'] * 2 + df['ROC_MA3'] * 3 + df['ROC_MA4'] * 4
        # KST=MA(KST_IND,9)
        df['KST'] = df['KST_IND'].rolling(n, min_periods=1).mean()
        # 去量纲
        df[f'KST_bh_{n}'] = df['KST_IND'] / (df['KST'] + eps) - 1
        df[f'KST_bh_{n}'] = df[f'KST_bh_{n}'].shift(1)
        extra_agg_dict[f'KST_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ROC1']
        del df['ROC2']
        del df['ROC3']
        del df['ROC4']
        del df['ROC_MA1']
        del df['ROC_MA2']
        del df['ROC_MA3']
        del df['ROC_MA4']
        del df['KST_IND']
        del df['KST']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'KST_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['KST', 'KST_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['KST']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_MICD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # MICD 指标
    for n in back_hour_list:
        """
        N=20
        N1=10
        N2=20
        M=10
        MI=CLOSE-REF(CLOSE,1)
        MTMMA=SMA(MI,N,1)
        DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        MICD=SMA(DIF,M,1)
        如果 MICD 上穿 0，则产生买入信号；
        如果 MICD 下穿 0，则产生卖出信号。
        """
        df['MI'] = df['close'] - df['close'].shift(1) # MI=CLOSE-REF(CLOSE,1)
        # df['MIMMA'] = df['MI'].rolling(n, min_periods=1).mean()
        df['MIMMA'] = df['MI'].ewm(span=n).mean() # MTMMA=SMA(MI,N,1)
        df['MIMMA_MA1'] = df['MIMMA'].shift(1).rolling(n, min_periods=1).mean() # MA(REF(MTMMA,1),N1)
        df['MIMMA_MA2'] = df['MIMMA'].shift(1).rolling(2 * n, min_periods=1).mean() # MA(REF(MTMMA,1),N2)
        df['DIF'] = df['MIMMA_MA1'] - df['MIMMA_MA2'] # DIF=MA(REF(MTMMA,1),N1)-MA(REF(MTMMA,1),N2)
        # df['MICD'] = df['DIF'].rolling(n, min_periods=1).mean()
        df['MICD'] = df['DIF'].ewm(span=n).mean()
        # 去量纲
        df[f'MICD_bh_{n}'] = df['DIF'] / (df['MICD'] + eps)
        df[f'MICD_bh_{n}'] = df[f'MICD_bh_{n}'].shift(1)
        extra_agg_dict[f'MICD_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['MI']
        del df['MIMMA']
        del df['MIMMA_MA1']
        del df['MIMMA_MA2']
        del df['DIF']
        del df['MICD']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MICD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MICD', 'MICD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MICD']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_PMO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # PMO 指标
    for n in back_hour_list:
        """
        N1=10
        N2=40
        N3=20
        ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        ROC_MA=DMA(ROC,2/N1)
        ROC_MA10=ROC_MA*10
        PMO=DMA(ROC_MA10,2/N2)
        PMO_SIGNAL=DMA(PMO,2/(N3+1))
        PMO 指标是 ROC 指标的双重平滑（移动平均）版本。与 SROC 不 同(SROC 是先对价格作平滑再求 ROC)，而 PMO 是先求 ROC 再对
        ROC 作平滑处理。PMO 越大（大于 0），则说明市场上涨趋势越强；
        PMO 越小（小于 0），则说明市场下跌趋势越强。如果 PMO 上穿/
        下穿其信号线，则产生买入/卖出指标。
        """
        df['ROC'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100 # ROC=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
        df['ROC_MA'] = df['ROC'].rolling(n, min_periods=1).mean() # ROC_MA=DMA(ROC,2/N1)
        df['ROC_MA10'] = df['ROC_MA'] * 10 # ROC_MA10=ROC_MA*10
        df['PMO'] = df['ROC_MA10'].rolling(4 * n, min_periods=1).mean() # PMO=DMA(ROC_MA10,2/N2)
        df['PMO_SIGNAL'] = df['PMO'].rolling(2 * n, min_periods=1).mean() # PMO_SIGNAL=DMA(PMO,2/(N3+1))

        df[f'PMO_bh_{n}'] = df['PMO_SIGNAL'].shift(1)
        extra_agg_dict[f'PMO_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['ROC']
        del df['ROC_MA']
        del df['ROC_MA10']
        del df['PMO']
        del df['PMO_SIGNAL']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PMO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PMO', 'PMO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PMO']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_RCCD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # RCCD 指标
    for n in back_hour_list:
        """
        M=40
        N1=20
        N2=40
        RC=CLOSE/REF(CLOSE,M)
        ARC1=SMA(REF(RC,1),M,1)
        DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        RCCD=SMA(DIF,M,1)
        RC 指标为当前价格与昨日价格的比值。当 RC 指标>1 时，说明价格在上升；当 RC 指标增大时，说明价格上升速度在增快。当 RC 指标
        <1 时，说明价格在下降；当 RC 指标减小时，说明价格下降速度在增
        快。RCCD 指标先对 RC 指标进行平滑处理，再取不同时间长度的移
        动平均的差值，再取移动平均。如 RCCD 上穿/下穿 0 则产生买入/
        卖出信号。
        """
        df['RC'] = df['close'] / df['close'].shift(2 * n)  # RC=CLOSE/REF(CLOSE,M)
        # df['ARC1'] = df['RC'].rolling(2 * n, min_periods=1).mean()
        df['ARC1'] = df['RC'].ewm(span=2 * n).mean()  # ARC1=SMA(REF(RC,1),M,1)
        df['MA1'] = df['ARC1'].shift(1).rolling(n, min_periods=1).mean()  # MA(REF(ARC1,1),N1)
        df['MA2'] = df['ARC1'].shift(1).rolling(2 * n, min_periods=1).mean()  # MA(REF(ARC1,1),N2)
        df['DIF'] = df['MA1'] - df['MA2']  # DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        # df['RCCD'] = df['DIF'].rolling(2 * n, min_periods=1).mean()
        df['RCCD'] = df['DIF'].ewm(span=2 * n).mean()  # RCCD=SMA(DIF,M,1)

        df[f'RCCD_bh_{n}'] = df['RCCD'].shift(1)
        extra_agg_dict[f'RCCD_bh_{n}'] = 'first'
        # 删除中间数据
        del df['RC']
        del df['ARC1']
        del df['MA1']
        del df['MA2']
        del df['DIF']
        del df['RCCD']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RCCD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RCCD', 'RCCD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RCCD']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_RCCD2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # RCCD 指标
    for n in back_hour_list:
        """
        M=40
        N1=20
        N2=40
        RC=CLOSE/REF(CLOSE,M)
        ARC1=SMA(REF(RC,1),M,1)
        DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        RCCD=SMA(DIF,M,1)
        RC 指标为当前价格与昨日价格的比值。当 RC 指标>1 时，说明价格在上升；当 RC 指标增大时，说明价格上升速度在增快。当 RC 指标
        <1 时，说明价格在下降；当 RC 指标减小时，说明价格下降速度在增
        快。RCCD 指标先对 RC 指标进行平滑处理，再取不同时间长度的移
        动平均的差值，再取移动平均。如 RCCD 上穿/下穿 0 则产生买入/
        卖出信号。
        """
        df['RC'] = df['close'] / df['close'].shift(2 * n)  # RC=CLOSE/REF(CLOSE,M)
        df['ARC1'] = df['RC'].rolling(2 * n, min_periods=1).sum()
        # df['ARC1'] = df['RC'].ewm(span=2 * n).mean()  # ARC1=SMA(REF(RC,1),M,1)
        df['MA1'] = df['ARC1'].shift(1).rolling(n, min_periods=1).mean()  # MA(REF(ARC1,1),N1)
        df['MA2'] = df['ARC1'].shift(1).rolling(2 * n, min_periods=1).mean()  # MA(REF(ARC1,1),N2)
        df['DIF'] = df['MA1'] - df['MA2']  # DIF=MA(REF(ARC1,1),N1)-MA(REF(ARC1,1),N2)
        df['RCCD'] = df['DIF'].rolling(2 * n, min_periods=1).sum()
        # df['RCCD'] = df['DIF'].ewm(span=2 * n).mean()  # RCCD=SMA(DIF,M,1)

        df[f'RCCD2_bh_{n}'] = df['RCCD'].shift(1)
        extra_agg_dict[f'RCCD2_bh_{n}'] = 'first'
        # 删除中间数据
        del df['RC']
        del df['ARC1']
        del df['MA1']
        del df['MA2']
        del df['DIF']
        del df['RCCD']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RCCD2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RCCD2', 'RCCD2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RCCD2']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_KAMA(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
        # KAMA 指标
    for n in back_hour_list:
        """
        N=10
        N1=2
        N2=30
        DIRECTION=CLOSE-REF(CLOSE,N)
        VOLATILITY=SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        ER=DIRETION/VOLATILITY
        FAST=2/(N1+1)
        SLOW=2/(N2+1)
        SMOOTH=ER*(FAST-SLOW)+SLOW
        COF=SMOOTH*SMOOTH
        KAMA=COF*CLOSE+(1-COF)*REF(KAMA,1)
        KAMA 指标与 VIDYA 指标类似，都是把 ER(EfficiencyRatio)指标加
        入到移动平均的权重中，其用法与其他移动平均线类似。在当前趋势
        较强时，ER 值较大，KAMA 会赋予当前价格更大的权重，使得 KAMA
        紧随价格变动，减小其滞后性；在当前趋势较弱（比如振荡市中）,ER
        值较小，KAMA 会赋予当前价格较小的权重，增大 KAMA 的滞后性，
        使其更加平滑，避免产生过多的交易信号。与 VIDYA 指标不同的是，
        KAMA 指标可以设置权值的上界 FAST 和下界 SLOW。
        """
        N = 5 * n
        N2 = 15 * n

        df['DIRECTION'] = df['close'] - df['close'].shift(N) #  DIRECTION=CLOSE-REF(CLOSE,N)
        df['abs_ref'] =abs(df['close'] - df['close'].shift(1)) # ABS(CLOSE-REF(CLOSE,1))
        df['VOLATILITY'] = df['abs_ref'].rolling(N, min_periods=1).sum() # VOLATILITY=SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        df['ER'] = df['DIRECTION'] / (df['VOLATILITY'] + eps)
        fast = 2 / (n + 1) # FAST=2/(N1+1)
        slow = 2/ (N2 + 1) # SLOW=2/(N2+1)
        df['SMOOTH'] = df['ER']  * (fast - slow) + slow # SMOOTH=ER*(FAST-SLOW)+SLOW
        df['COF'] = df['SMOOTH'] * df['SMOOTH'] # COF=SMOOTH*SMOOTH
        # KAMA=COF*CLOSE+(1-COF)*REF(KAMA,1)
        df['KAMA'] = df['COF'] * df['close'] + (1- df['COF'])
        df['KAMA'] = df['COF'] * df['close'] + (1- df['COF']) + df['KAMA'].shift(1)
        # 进行归一化
        df['KAMA_min'] = df['KAMA'].rolling(n, min_periods=1).min()
        df['KAMA_max'] = df['KAMA'].rolling(n, min_periods=1).max()
        df['KAMA_norm'] = (df['KAMA'] - df['KAMA_min']) / (df['KAMA_max'] - df['KAMA_min'] + eps)

        df[f'KAMA_bh_{n}'] = df['KAMA_norm'].shift(1)
        extra_agg_dict[f'KAMA_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['DIRECTION']
        del df['abs_ref']
        del df['VOLATILITY']
        del df['ER']
        del df['SMOOTH']
        del df['COF']
        del df['KAMA']
        del df['KAMA_min']
        del df['KAMA_max']
        del df['KAMA_norm']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'KAMA_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['KAMA', 'KAMA_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['KAMA']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_PPO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # PPO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        N3=9
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        PPO_SIGNAL=EMA(PPO,N3)
        PPO 是 MACD 的变化率版本。
        MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2)，而
        PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)。
        PPO 上穿/下穿 PPO_SIGNAL 产生买入/卖出信号。
        """
        #
        N3 = n
        N1 = int(n * 1.382) # 黄金分割线
        N2 = 3 * n
        df['ema_1'] = df['close'].ewm(N1, adjust=False).mean() # EMA(CLOSE,N1)
        df['ema_2'] = df['close'].ewm(N2, adjust=False).mean() # EMA(CLOSE,N2)
        df['PPO'] = (df['ema_1'] - df['ema_2']) / (df['ema_2'] + eps) # PPO=(EMA(CLOSE,N1)-EMA(CLOSE,N2))/EMA(CLOSE,N2)
        df['PPO_SIGNAL'] = df['PPO'].ewm(N3, adjust=False).mean() # PPO_SIGNAL=EMA(PPO,N3)

        df[f'PPO_bh_{n}'] = df['PPO_SIGNAL'].shift(1)
        extra_agg_dict[f'PPO_bh_{n}'] = 'first'
        # 删除中间数据
        del df['ema_1']
        del df['ema_2']
        del df['PPO']
        del df['PPO_SIGNAL']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PPO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PPO', 'PPO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PPO']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_SMI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # SMI 指标
    for n in back_hour_list:
        """
        N1=20
        N2=20
        N3=20
        M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        D=CLOSE-M
        DS=EMA(EMA(D,N2),N2)
        DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        SMI=100*DS/DHL
        SMIMA=MA(SMI,N3)
        SMI 指标可以看作 KDJ 指标的变形。不同的是，KD 指标衡量的是当
        天收盘价位于最近 N 天的最高价和最低价之间的位置，而 SMI 指标
        是衡量当天收盘价与最近 N 天的最高价与最低价均值之间的距离。我
        们用 SMI 指标上穿/下穿其均线产生买入/卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N1)
        df['min_low'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N1)
        df['M'] = (df['max_high'] + df['min_low']) / 2 # M=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['D'] = df['close'] - df['M'] # D=CLOSE-M
        df['ema'] = df['D'].ewm(n, adjust=False).mean() # EMA(D,N2)
        df['DS'] = df['ema'].ewm(n, adjust=False).mean() # DS=EMA(EMA(D,N2),N2)
        df['HL'] = df['max_high'] - df['min_low'] # MAX(HIGH,N1) - MIN(LOW,N1)
        df['ema_hl'] = df['HL'].ewm(n, adjust=False).mean() # EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2)
        df['DHL'] = df['ema_hl'].ewm(n, adjust=False).mean() # DHL=EMA(EMA(MAX(HIGH,N1)-MIN(LOW,N1),N2),N2)
        df['SMI'] = 100 * df['DS'] / (df['DHL'] + eps) #  SMI=100*DS/DHL
        df['SMIMA'] = df['SMI'].rolling(n, min_periods=1).mean() # SMIMA=MA(SMI,N3)

        df[f'SMI_bh_{n}'] = df['SMIMA'].shift(1)
        extra_agg_dict[f'SMI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['max_high']
        del df['min_low']
        del df['M']
        del df['D']
        del df['ema']
        del df['DS']
        del df['HL']
        del df['ema_hl']
        del df['DHL']
        del df['SMI']
        del df['SMIMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'SMI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['SMI', 'SMI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['SMI']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_ARBR(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # ARBR指标
    for n in back_hour_list:
        """
        AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100
        AR 衡量开盘价在最高价、最低价之间的位置；BR 衡量昨日收盘价在
        今日最高价、最低价之间的位置。AR 为人气指标，用来计算多空双
        方的力量对比。当 AR 值偏低（低于 50）时表示人气非常低迷，股价
        很低，若从 50 下方上穿 50，则说明股价未来可能要上升，低点买入。
        当 AR 值下穿 200 时卖出。
        """
        df['HO'] = df['high'] - df['open'] # (HIGH-OPEN)
        df['OL'] = df['open'] - df['low'] # (OPEN-LOW)
        df['AR'] = df['HO'].rolling(n, min_periods=1).sum() / df['OL'].rolling(n, min_periods=1).sum() * 100 # AR=SUM((HIGH-OPEN),N)/SUM((OPEN-LOW),N)*100
        df['HC'] = df['high'] - df['close'].shift(1) # (HIGH-REF(CLOSE,1))
        df['CL'] = df['close'].shift(1) - df['low'] # (REF(CLOSE,1)-LOW)
        df['BR'] = df['HC'].rolling(n, min_periods=1).sum() / (df['CL'].rolling(n, min_periods=1).sum() + eps) * 100 # BR=SUM((HIGH-REF(CLOSE,1)),N)/SUM((REF(CLOSE,1)-LOW),N)*100

        df[f'ARBR_AR_bh_{n}'] = df['AR'].shift(1)
        df[f'ARBR_BR_bh_{n}'] = df['BR'].shift(1)
        extra_agg_dict[f'ARBR_AR_bh_{n}'] = 'first'
        extra_agg_dict[f'ARBR_BR_bh_{n}'] = 'first'
        # 删除中间数据
        del df['HO']
        del df['OL']
        del df['AR']
        del df['HC']
        del df['CL']
        del df['BR']
        if add:
            for _ in [f'ARBR_AR_bh_{n}', f'ARBR_BR_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ARBR_AR', 'ARBR_BR', 'ARBR_AR_diff', 'ARBR_BR_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ARBR_AR', 'ARBR_BR']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_DO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # DO 指标
    for n in back_hour_list:
        """
        DO=EMA(EMA(RSI,N),M)
        DO 是平滑处理（双重移动平均）后的 RSI 指标。DO 大于 0 则说明
        市场处于上涨趋势，小于 0 说明市场处于下跌趋势。我们用 DO 上穿
        /下穿其移动平均线来产生买入/卖出信号。
        """
        # 计算RSI
        # 以下为基础策略分享会代码
        # diff = df['close'].diff()
        # df['up'] = np.where(diff > 0, diff, 0)
        # df['down'] = np.where(diff < 0, abs(diff), 0)
        # A = df['up'].rolling(n).sum()
        # B = df['down'].rolling(n).sum()
        # df['rsi'] = A / (A + B)
        diff = df['close'].diff() # CLOSE-REF(CLOSE,1) 计算当前close 与前一周期的close的差值
        df['up'] = np.where(diff > 0, diff, 0) # IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0) 表示当前是上涨状态，记录上涨幅度
        df['down'] = np.where(diff < 0, abs(diff), 0) # IF(CLOSE<REF(CLOSE,1),ABS(CLOSE-REF(CLOSE,1)),0) 表示当前为下降状态，记录下降幅度
        A = df['up'].ewm(span=n).mean()# SMA(CLOSEUP,N,1) 计算周期内的上涨幅度的sma
        B = df['down'].ewm(span=n).mean() # SMA(CLOSEDOWN,N,1)计算周期内的下降幅度的sma
        df['rsi'] = A / (A + B + eps)  # RSI=100*CLOSEUP_MA/(CLOSEUP_MA+CLOSEDOWN_MA)  没有乘以100   没有量纲即可
        df['ema_rsi'] = df['rsi'].ewm(n, adjust=False).mean() # EMA(RSI,N)
        df['DO'] = df['ema_rsi'].ewm(n, adjust=False).mean() # DO=EMA(EMA(RSI,N),M)
        df[f'DO_bh_{n}'] = df['DO'].shift(1)
        extra_agg_dict[f'DO_bh_{n}'] = 'first'
        # 删除中间数据
        del df['up']
        del df['down']
        del df['rsi']
        del df['ema_rsi']
        del df['DO']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DO', 'DO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DO']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_SI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # SI 指标
    for n in back_hour_list:
        """
        A=ABS(HIGH-REF(CLOSE,1))
        B=ABS(LOW-REF(CLOSE,1))
        C=ABS(HIGH-REF(LOW,1))
        D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        N=20
        K=MAX(A,B)
        M=MAX(HIGH-LOW,N)
        R1=A+0.5*B+0.25*D
        R2=B+0.5*A+0.25*D
        R3=C+0.25*D
        R4=IF((A>=B) & (A>=C),R1,R2)
        R=IF((C>=A) & (C>=B),R3,R4)
        SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+
        0.5*(CLOSE-OPEN))/R*K/M
        SI 用价格变化（即两天收盘价之差，昨日收盘与开盘价之差，今日收
        盘与开盘价之差）的加权平均来反映价格的变化。如果 SI 上穿/下穿
        0 则产生买入/卖出信号。
        """
        df['A'] = abs(df['high'] - df['close'].shift(1)) # A=ABS(HIGH-REF(CLOSE,1))
        df['B'] = abs(df['low'] - df['close'].shift(1))# B=ABS(LOW-REF(CLOSE,1))
        df['C'] = abs(df['high'] - df['low'].shift(1)) # C=ABS(HIGH-REF(LOW,1))
        df['D'] = abs(df['close'].shift(1) - df['open'].shift(1)) #  D=ABS(REF(CLOSE,1)-REF(OPEN,1))
        df['K'] = df[['A', 'B']].max(axis=1) # K=MAX(A,B)
        df['M'] = (df['high'] - df['low']).rolling(n).max() # M=MAX(HIGH-LOW,N)
        df['R1'] = df['A'] + 0.5 * df['B'] + 0.25 * df['D'] # R1=A+0.5*B+0.25*D
        df['R2'] = df['B'] + 0.5 * df['A'] + 0.25 * df['D'] #  R2=B+0.5*A+0.25*D
        df['R3'] = df['C'] + 0.25 * df['D'] # R3=C+0.25*D
        df['R4'] = np.where((df['A'] >= df['B']) & (df['A'] >= df['C']), df['R1'], df['R2']) # R4=IF((A>=B) & (A>=C),R1,R2)
        df['R'] = np.where((df['C'] >= df['A']) & (df['C'] >= df['B']), df['R3'], df['R4']) # R=IF((C>=A) & (C>=B),R3,R4)
        # SI=50*(CLOSE-REF(CLOSE,1)+(REF(CLOSE,1)-REF(OPEN,1))+0.5*(CLOSE-OPEN))/R*K/M
        df['SI'] = 50 * (df['close'] - df['close'].shift(1) + (df['close'].shift(1) - df['open'].shift(1)) +
                         0.5 * (df['close'] - df['open'])) / (df['R'] * df['M'] + eps)* df['K']
        df[f'SI_bh_{n}'] = df['SI'].shift(1)
        extra_agg_dict[f'SI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['A']
        del df['B']
        del df['C']
        del df['D']
        del df['K']
        del df['M']
        del df['R1']
        del df['R2']
        del df['R3']
        del df['R4']
        del df['R']
        del df['SI']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'SI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['SI', 'SI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['SI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_DBCD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # DBCD 指标
    for n in back_hour_list:
        """
        N=5
        M=16
        T=17
        BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        BIAS_DIF=BIAS-REF(BIAS,M)
        DBCD=SMA(BIAS_DIFF,T,1)
        DBCD（异同离差乖离率）为乖离率离差的移动平均。我们用 DBCD
        上穿 5%/下穿-5%来产生买入/卖出信号。
        """
        df['ma'] = df['close'].rolling(n, min_periods=1).mean() # MA(CLOSE,N)

        df['BIAS'] = (df['close'] - df['ma']) / (df['ma'] + eps) * 100 # BIAS=(CLOSE-MA(CLOSE,N)/MA(CLOSE,N))*100
        df['BIAS_DIF'] = df['BIAS'] - df['BIAS'].shift(3 * n) # BIAS_DIF=BIAS-REF(BIAS,M)
        # df['DBCD'] = df['BIAS_DIF'].rolling(3 * n + 2, min_periods=1).mean()
        df['DBCD'] = df['BIAS_DIF'].ewm(span=3 * n).mean() # DBCD=SMA(BIAS_DIFF,T,1)
        df[f'DBCD_bh_{n}'] = df['DBCD'].shift(1)
        extra_agg_dict[f'DBCD_bh_{n}'] = 'first'
        # 删除中间数据
        del df['ma']
        del df['BIAS']
        del df['BIAS_DIF']
        del df['DBCD']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'DBCD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['DBCD', 'DBCD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['DBCD']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_CV(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # CV 指标
    for n in back_hour_list:
        """
        N=10
        H_L_EMA=EMA(HIGH-LOW,N)
        CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
        CV 指标用来衡量股价的波动，反映一段时间内最高价与最低价之差
        （价格变化幅度）的变化率。如果 CV 的绝对值下穿 30，买入；
        如果 CV 的绝对值上穿 70，卖出。
        """
        df['H_L_ema'] = (df['high'] - df['low']).ewm(n, adjust=False).mean() # H_L_EMA=EMA(HIGH-LOW,N)
        df['CV'] = (df['H_L_ema'] - df['H_L_ema'].shift(n)) / (df['H_L_ema'].shift(n) + eps) * 100 # CV=(H_L_EMA-REF(H_L_EMA,N))/REF(H_L_EMA,N)*100
        df[f'CV_bh_{n}'] = df['CV'].shift(1)
        extra_agg_dict[f'CV_bh_{n}'] = 'first'
        # 删除中间数据
        del df['H_L_ema']
        del df['CV']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CV_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CV', 'CV_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CV']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_RMI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # RMI 指标
    for n in back_hour_list:
        """
        N=7
        RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        RMI 与 RSI 的计算方式类似，将 RSI 中的动量与前一天价格之差
        CLOSE-REF(CLOSE,1)项改为了与前四天价格之差 CLOSEREF(CLOSE,4)
        """
        # MAX(CLOSE-REF(CLOSE,4),0)
        df['max_close'] = np.where(df['close'] > df['close'].shift(4), df['close'] - df['close'].shift(4), 0)
        # ABS(CLOSE-REF(CLOSE,1)
        df['abs_close'] = df['close'] - df['close'].shift(1)

        # df['sma_1'] = df['max_close'].rolling(n, min_periods=1).mean()
        df['sma_1'] = df['max_close'].ewm(span=n).mean() # SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)
        # df['sma_2'] = df['abs_close'].rolling(n, min_periods=1).mean()
        df['sma_2'] = df['abs_close'].ewm(span=n).mean() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['RMI'] = df['sma_1'] / (df['sma_2'] + eps) * 100 #  RMI=SMA(MAX(CLOSE-REF(CLOSE,4),0),N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        df[f'RMI_bh_{n}'] = df['RMI'].shift(1)
        extra_agg_dict[f'RMI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['max_close']
        del df['abs_close']
        del df['sma_1']
        del df['sma_2']
        del df['RMI']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RMI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RMI', 'RMI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RMI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_SKDJ(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # SKDJ 指标
    for n in back_hour_list:
        """
        N=60
        M=5
        RSV=(CLOSE-MIN(LOW,N))/(MAX(HIGH,N)-MIN(LOW,N))*100
        MARSV=SMA(RSV,3,1)
        K=SMA(MARSV,3,1)
        D=MA(K,3)
        SKDJ 为慢速随机波动（即慢速 KDJ）。SKDJ 中的 K 即 KDJ 中的 D，
        SKJ 中的 D 即 KDJ 中的 D 取移动平均。其用法与 KDJ 相同。
        当 D<40(处于超卖状态)且 K 上穿 D 时买入，当 D>60（处于超买状
        态）K 下穿 D 时卖出。
        """
        # RSV=(CLOSE-MIN(LOW,N))/(MAX(HIGH,N)-MIN(LOW,N))*100
        df['RSV'] = (df['close'] - df['low'].rolling(n, min_periods=1).min()) / (
                df['high'].rolling(n, min_periods=1).max() - df['low'].rolling(n, min_periods=1).min() + eps) * 100
        # MARSV=SMA(RSV,3,1)
        df['MARSV'] = df['RSV'].ewm(com=2).mean()
        # K=SMA(MARSV,3,1)
        df['K'] = df['MARSV'].ewm(com=2).mean()
        # D=MA(K,3)
        df['D'] = df['K'].rolling(3, min_periods=1).mean()
        df[f'SKDJ_bh_{n}'] = df['D'].shift(1)
        extra_agg_dict[f'SKDJ_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['RSV']
        del df['MARSV']
        del df['K']
        del df['D']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'SKDJ_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['SKDJ', 'SKDJ_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['SKDJ']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_ROC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # ROC 指标
    for n in back_hour_list:
        """
        ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
        ROC 衡量价格的涨跌幅。ROC 可以用来反映市场的超买超卖状态。
        当 ROC 过高时，市场处于超买状态；当 ROC 过低时，市场处于超
        卖状态。这些情况下，可能会发生反转。
        如果 ROC 上穿 5%，则产生买入信号；
        如果 ROC 下穿-5%，则产生卖出信号。
        """
        # ROC=(CLOSE-REF(CLOSE,100))/REF(CLOSE,100)
        df['ROC'] = df['close'] / (df['close'].shift(n) + eps) - 1

        df[f'ROC_bh_{n}'] = df['ROC'].shift(1)
        extra_agg_dict[f'ROC_bh_{n}'] = 'first'
        del df['ROC']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ROC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ROC', 'ROC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ROC']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_WR(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # WR 指标
    for n in back_hour_list:
        """
        HIGH(N)=MAX(HIGH,N)
        LOW(N)=MIN(LOW,N)
        WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
        WR 指标事实上就是 100-KDJ 指标计算过程中的 Stochastics。WR
        指标用来衡量市场的强弱和超买超卖状态。一般认为，当 WR 小于
        20 时，市场处于超买状态；当 WR 大于 80 时，市场处于超卖状态；
        当 WR 处于 20 到 80 之间时，多空较为平衡。
        如果 WR 上穿 80，则产生买入信号；
        如果 WR 下穿 20，则产生卖出信号。
        """
        df['max_high'] = df['high'].rolling(n, min_periods=1).max() # HIGH(N)=MAX(HIGH,N)
        df['min_low'] = df['low'].rolling(n, min_periods=1).min() # LOW(N)=MIN(LOW,N)
        # WR=100*(HIGH(N)-CLOSE)/(HIGH(N)-LOW(N))
        df['WR'] = (df['max_high'] - df['close']) / (df['max_high'] - df['min_low'] + eps) * 100
        df[f'WR_bh_{n}'] = df['WR'].shift(1)
        extra_agg_dict[f'WR_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['max_high']
        del df['min_low']
        del df['WR']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'WR_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['WR', 'WR_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['WR']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_STC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # STC 指标
    for n in back_hour_list:
        """
        N1=23
        N2=50
        N=40
        MACDX=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        V1=MIN(MACDX,N)
        V2=MAX(MACDX,N)-V1
        FK=IF(V2>0,(MACDX-V1)/V2*100,REF(FK,1))
        FD=SMA(FK,N,1)
        V3=MIN(FD,N)
        V4=MAX(FD,N)-V3
        SK=IF(V4>0,(FD-V3)/V4*100,REF(SK,1))
        STC=SD=SMA(SK,N,1) 
        STC 指标结合了 MACD 指标和 KDJ 指标的算法。首先用短期均线与
        长期均线之差算出 MACD，再求 MACD 的随机快速随机指标 FK 和
        FD，最后求 MACD 的慢速随机指标 SK 和 SD。其中慢速随机指标就
        是 STC 指标。STC 指标可以用来反映市场的超买超卖状态。一般认
        为 STC 指标超过 75 为超买，STC 指标低于 25 为超卖。
        如果 STC 上穿 25，则产生买入信号；
        如果 STC 下穿 75，则产生卖出信号。
        """
        N1 = n
        N2 = int(N1 * 1.5)  # 大约值
        N = 2 * n
        df['ema1'] = df['close'].ewm(N1, adjust=False).mean() # EMA(CLOSE,N1)
        df['ema2'] = df['close'].ewm(N, adjust=False).mean() # EMA(CLOSE,N2)
        df['MACDX'] = df['ema1'] - df['ema2'] # MACDX=EMA(CLOSE,N1)-EMA(CLOSE,N2)
        df['V1'] = df['MACDX'].rolling(N2, min_periods=1).min() # V1=MIN(MACDX,N)
        df['V2'] = df['MACDX'].rolling(N2, min_periods=1).max() - df['V1'] # V2=MAX(MACDX,N)-V1
        # FK=IF(V2>0,(MACDX-V1)/V2*100,REF(FK,1))
        df['FK'] = (df['MACDX'] - df['V1']) / (df['V2'] + eps) * 100
        df['FK'] = np.where(df['V2'] > 0, (df['MACDX'] - df['V1']) / (df['V2'] + eps) * 100, df['FK'].shift(1))

        df['FD'] = df['FK'].rolling(N2, min_periods=1).mean()# FD=SMA(FK,N,1)  直接使用均线代替sma
        df['V3'] = df['FD'].rolling(N2, min_periods=1).min() # V3=MIN(FD,N)
        df['V4'] = df['FD'].rolling(N2, min_periods=1).max() - df['V3'] # V4=MAX(FD,N)-V3
        # SK=IF(V4>0,(FD-V3)/V4*100,REF(SK,1))
        df['SK'] = (df['FD'] - df['V3']) / (df['V4'] + eps) * 100
        df['SK'] = np.where(df['V4'] > 0, (df['FD'] - df['V3']) / (df['V4'] + eps) * 100, df['SK'].shift(1))
        # STC = SD = SMA(SK, N, 1)
        df['STC'] = df['SK'].rolling(N1, min_periods=1).mean()
        df[f'STC_bh_{n}'] = df['STC'].shift(1)
        extra_agg_dict[f'STC_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['ema1']
        del df['ema2']
        del df['MACDX']
        del df['V1']
        del df['V2']
        del df['V3']
        del df['V4']
        del df['FK']
        del df['FD']
        del df['SK']
        del df['STC']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'STC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['STC', 'STC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['STC']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_RVI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # RVI 指标
    for n in back_hour_list:
        """
        N1=10
        N2=20
        STD=STD(CLOSE,N)
        USTD=SUM(IF(CLOSE>REF(CLOSE,1),STD,0),N2)
        DSTD=SUM(IF(CLOSE<REF(CLOSE,1),STD,0),N2)
        RVI=100*USTD/(USTD+DSTD)
        RVI 的计算方式与 RSI 一样，不同的是将 RSI 计算中的收盘价变化值
        替换为收盘价在过去一段时间的标准差，用来反映一段时间内上升
        的波动率和下降的波动率的对比。我们也可以像计算 RSI 指标时一样
        先对公式中的 USTD 和 DSTD 作移动平均得到 USTD_MA 和
        DSTD_MA 再求出 RVI=100*USTD_MV/(USTD_MV+DSTD_MV)。
        RVI 的用法与 RSI 一样。通常认为当 RVI 大于 70，市场处于强势上
        涨甚至达到超买的状态；当 RVI 小于 30，市场处于强势下跌甚至达
        到超卖的状态。当 RVI 跌到 30 以下又上穿 30 时，通常认为股价要
        从超卖的状态反弹；当 RVI 超过 70 又下穿 70 时，通常认为市场要
        从超买的状态回落了。
        如果 RVI 上穿 30，则产生买入信号；
        如果 RVI 下穿 70，则产生卖出信号。
        """
        df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0) # STD=STD(CLOSE,N)
        df['ustd'] = np.where(df['close'] > df['close'].shift(1), df['std'], 0) # IF(CLOSE>REF(CLOSE,1),STD,0)
        df['sum_ustd'] = df['ustd'].rolling(2 * n, min_periods=1).sum() #  USTD=SUM(IF(CLOSE>REF(CLOSE,1),STD,0),N2)

        df['dstd'] = np.where(df['close'] < df['close'].shift(1), df['std'], 0) # IF(CLOSE<REF(CLOSE,1),STD,0)
        df['sum_dstd'] = df['dstd'].rolling(2 * n, min_periods=1).sum() # DSTD=SUM(IF(CLOSE<REF(CLOSE,1),STD,0),N2)

        df['RVI'] = df['sum_ustd'] / (df['sum_ustd'] + df['sum_dstd'] + eps) * 100 # RVI=100*USTD/(USTD+DSTD)
        df[f'RVI_bh_{n}'] = df['RVI'].shift(1)
        extra_agg_dict[f'RVI_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['std']
        del df['ustd']
        del df['sum_ustd']
        del df['dstd']
        del df['sum_dstd']
        del df['RVI']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RVI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RVI', 'RVI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RVI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_UOS(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # UOS 指标
    for n in back_hour_list:
        """
        M=7
        N=14
        O=28
        TH=MAX(HIGH,REF(CLOSE,1))
        TL=MIN(LOW,REF(CLOSE,1))
        TR=TH-TL
        XR=CLOSE-TL
        XRM=SUM(XR,M)/SUM(TR,M)
        XRN=SUM(XR,N)/SUM(TR,N)
        XRO=SUM(XR,O)/SUM(TR,O)
        UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
        UOS 的用法与 RSI 指标类似，可以用来反映市场的超买超卖状态。
        一般来说，UOS 低于 30 市场处于超卖状态；UOS 高于 30 市场处于
        超买状态。
        如果 UOS 上穿 30，则产生买入信号；
        如果 UOS 下穿 70，则产生卖出信号。
        """
        # 固定多参数比例倍数
        M = n
        N = 2 * n
        O = 4 * n
        df['ref_close'] = df['close'].shift(1) # REF(CLOSE,1)
        df['TH'] = df[['high', 'ref_close']].max(axis=1) #  TH=MAX(HIGH,REF(CLOSE,1))
        df['TL'] = df[['low', 'ref_close']].min(axis=1) # TL=MIN(LOW,REF(CLOSE,1))
        df['TR'] = df['TH'] - df['TL']  # TR=TH-TL
        df['XR'] = df['close'] - df['TL'] # XR=CLOSE-TL
        df['XRM'] = df['XR'].rolling(M, min_periods=1).sum() / (eps + df['TR'].rolling(M, min_periods=1).sum()) # XRM=SUM(XR,M)/SUM(TR,M)
        df['XRN'] = df['XR'].rolling(N, min_periods=1).sum() / (eps + df['TR'].rolling(N, min_periods=1).sum()) # XRN=SUM(XR,N)/SUM(TR,N)
        df['XRO'] = df['XR'].rolling(O, min_periods=1).sum() / (eps + df['TR'].rolling(O, min_periods=1).sum()) # XRO=SUM(XR,O)/SUM(TR,O)
        # UOS=100*(XRM*N*O+XRN*M*O+XRO*M*N)/(M*N+M*O+N*O)
        df['UOS'] = 100 * (df['XRM'] * N * O + df['XRN'] * M * O + df['XRO'] * M * N) / (M * N + M * O + N * O + eps)
        df[f'UOS_bh_{n}'] = df['UOS'].shift(1)
        extra_agg_dict[f'UOS_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['ref_close']
        del df['TH']
        del df['TL']
        del df['TR']
        del df['XR']
        del df['XRM']
        del df['XRN']
        del df['XRO']
        del df['UOS']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'UOS_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['UOS', 'UOS_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['UOS']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_RSIS(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # RSIS 指标
    for n in back_hour_list:
        """
        N=120
        M=20
        CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CL
        OSE,1),0)
        RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOS
        E,1)),N,1)*100
        RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
        RSISMA=EMA(RSIS,M)
        RSIS 反映当前 RSI 在最近 N 天的 RSI 最大值和最小值之间的位置，
        与 KDJ 指标的构造思想类似。由于 RSIS 波动性比较大，我们先取移
        动平均再用其产生信号。其用法与 RSI 指标的用法类似。
        RSISMA 上穿 40 则产生买入信号；
        RSISMA 下穿 60 则产生卖出信号。
        """
        N = 6 * n
        M = n
        # CLOSE_DIFF_POS=IF(CLOSE>REF(CLOSE,1),CLOSE-REF(CLOSE,1),0)
        df['close_diff_pos'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['close'].shift(1), 0)
        # df['sma_1'] = df['close_diff_pos'].rolling(N).sum() # SMA(CLOSE_DIFF_POS,N,1)
        df['sma_1'] = df['close_diff_pos'].ewm(span=N).mean() # SMA(CLOSE_DIFF_POS,N,1)
        # df['sma_2'] = abs(df['close'] - df['close'].shift(1)).rolling(N).sum() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['sma_2'] = abs(df['close'] - df['close'].shift(1)).ewm(span=N).mean() # SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)
        df['RSI'] = df['sma_1'] / df['sma_2'] * 100 # RSI=SMA(CLOSE_DIFF_POS,N,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),N,1)*100
        # RSIS=(RSI-MIN(RSI,N))/(MAX(RSI,N)-MIN(RSI,N))*100
        df['RSIS'] = (df['RSI'] - df['RSI'].rolling(N, min_periods=1).min()) / (
                df['RSI'].rolling(N, min_periods=1).max() - df['RSI'].rolling(N, min_periods=1).min() + eps) * 100
        # RSISMA=EMA(RSIS,M)
        df['RSISMA'] = df['RSIS'].ewm(M, adjust=False).mean()

        df[f'RSIS_bh_{n}'] = df['RSISMA'].shift(1)
        extra_agg_dict[f'RSIS_bh_{n}'] = 'first'

        del df['close_diff_pos']
        del df['sma_1']
        del df['sma_2']
        del df['RSI']
        del df['RSIS']
        del df['RSISMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RSIS_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RSIS', 'RSIS_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RSIS']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_MAAMT(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # MAAMT 指标
    for n in back_hour_list:
        """
        N=40
        MAAMT=MA(AMOUNT,N)
        MAAMT 是成交额的移动平均线。当成交额上穿/下穿移动平均线时产
        生买入/卖出信号。
        """
        df['MAAMT'] = df['volume'].rolling(n, min_periods=1).mean() #MAAMT=MA(AMOUNT,N)
        df[f'MAAMT_bh_{n}'] = df['MAAMT'].shift(1)
        extra_agg_dict[f'MAAMT_bh_{n}'] = 'first'
        del df['MAAMT']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MAAMT_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MAAMT', 'MAAMT_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MAAMT']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_SROCVOL(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # SROCVOL 指标
    for n in back_hour_list:
        """
        N=20
        M=10
        EMAP=EMA(VOLUME,N)
        SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        SROCVOL 与 ROCVOL 类似，但是会先对成交量进行移动平均平滑
        处理之后再取其变化率。（SROCVOL 是 SROC 的成交量版本。）
        SROCVOL 上穿 0 买入，下穿 0 卖出。
        """
        df['emap'] = df['volume'].ewm(2 * n, adjust=False).mean() # EMAP=EMA(VOLUME,N)
        # SROCVOL=(EMAP-REF(EMAP,M))/REF(EMAP,M)
        df['SROCVOL'] = (df['emap'] - df['emap'].shift(n)) / (df['emap'].shift(n) + eps)
        df[f'SROCVOL_bh_{n}'] = df['SROCVOL'].shift(1)
        extra_agg_dict[f'SROCVOL_bh_{n}'] = 'first'
        del df['emap']
        del df['SROCVOL']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'SROCVOL_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['SROCVOL', 'SROCVOL_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['SROCVOL']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_PVO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # PVO 指标
    for n in back_hour_list:
        """
        N1=12
        N2=26
        PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
        PVO 用成交量的指数移动平均来反应成交量的变化。PVO 上穿 0 线
        买入；PVO 下穿 0 线卖出。
        """
        df['emap_1'] = df['volume'].ewm(n, min_periods=1).mean() # EMA(VOLUME,N1)
        df['emap_2'] = df['volume'].ewm(n * 2, min_periods=1).mean() # EMA(VOLUME,N2)
        df['PVO'] = (df['emap_1'] - df['emap_2']) / (df['emap_2'] + eps)# PVO=(EMA(VOLUME,N1)-EMA(VOLUME,N2))/EMA(VOLUME,N2)
        df[f'PVO_bh_{n}'] = df['PVO'].shift(1)
        extra_agg_dict[f'PVO_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['emap_1']
        del df['emap_2']
        del df['PVO']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PVO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PVO', 'PVO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PVO']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_BIASVOL(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # BIASVOL 指标
    for n in back_hour_list:
        """
        N=6，12，24
        BIASVOL(N)=(VOLUME-MA(VOLUME,N))/MA(VOLUME,N)
        BIASVOL 是乖离率 BIAS 指标的成交量版本。如果 BIASVOL6 大于
        5 且 BIASVOL12 大于 7 且 BIASVOL24 大于 11，则产生买入信号；
        如果 BIASVOL6 小于-5 且 BIASVOL12 小于-7 且 BIASVOL24 小于
        -11，则产生卖出信号。
        """
        df['ma_volume'] = df['volume'].rolling(n, min_periods=1).mean() # MA(VOLUME,N)
        df['BIASVOL'] = (df['volume'] - df['ma_volume']) / (df['ma_volume'] + eps) # BIASVOL(N)=(VOLUME-MA(VOLUME,N))/MA(VOLUME,N)
        df[f'BIASVOL_bh_{n}'] = df['BIASVOL'].shift(1)
        extra_agg_dict[f'BIASVOL_bh_{n}'] = 'first'
        del df['ma_volume']
        del df['BIASVOL']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'BIASVOL_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['BIASVOL', 'BIASVOL_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['BIASVOL']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_MACDVOL(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # MACDVOL 指标
    for n in back_hour_list:
        """
        N1=20
        N2=40
        N3=10
        MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
        SIGNAL=MA(MACDVOL,N3)
        MACDVOL 是 MACD 的成交量版本。如果 MACDVOL 上穿 SIGNAL，
        则买入；下穿 SIGNAL 则卖出。
        """
        N1 = 2 * n
        N2 = 4 * n
        N3 = n
        df['ema_volume_1'] = df['volume'].ewm(N1, adjust=False).mean() # EMA(VOLUME,N1)
        df['ema_volume_2'] = df['volume'].ewm(N2, adjust=False).mean() # EMA(VOLUME,N2)
        df['MACDV'] = df['ema_volume_1'] - df['ema_volume_2'] # MACDVOL=EMA(VOLUME,N1)-EMA(VOLUME,N2)
        df['SIGNAL'] = df['MACDV'].rolling(N3, min_periods=1).mean() # SIGNAL=MA(MACDVOL,N3)
        # 去量纲
        df['MACDVOL'] = df['MACDV'] / (df['SIGNAL'] + eps) - 1
        df[f'MACDVOL_bh_{n}'] = df['MACDVOL'].shift(1)
        extra_agg_dict[f'MACDVOL_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ema_volume_1']
        del df['ema_volume_2']
        del df['MACDV']
        del df['SIGNAL']
        del df['MACDVOL']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MACDVOL_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MACDVOL', 'MACDVOL_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MACDVOL']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_ROCVOL(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # ROCVOL 指标
    for n in back_hour_list:
        """
        N = 80
        ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)
        ROCVOL 是 ROC 的成交量版本。如果 ROCVOL 上穿 0 则买入，下
        穿 0 则卖出。
        """
        df['ROCVOL'] = df['volume'] / (df['volume'].shift(n) + eps) - 1 # ROCVOL=(VOLUME-REF(VOLUME,N))/REF(VOLUME,N)

        df[f'ROCVOL_bh_{n}'] = df['ROCVOL'].shift(1)
        extra_agg_dict[f'ROCVOL_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ROCVOL_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ROCVOL', 'ROCVOL_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ROCVOL']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_FI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # FI 指标
    for n in back_hour_list:
        """
        N=13
        FI=(CLOSE-REF(CLOSE,1))*VOLUME
        FIMA=EMA(FI,N)
        FI 用价格的变化来衡量价格的趋势，用成交量大小来衡量趋势的强
        弱。我们先对 FI 取移动平均，当均线上穿 0 线时产生买入信号，下
        穿 0 线时产生卖出信号。
        """
        df['FI'] = (df['close'] - df['close'].shift(1)) * df['volume'] # FI=(CLOSE-REF(CLOSE,1))*VOLUME
        df['FIMA'] = df['FI'].ewm(n, adjust=False).mean() # FIMA=EMA(FI,N)
        # 去量纲
        df[f'FI_bh_{n}'] = df['FI'] / (df['FIMA'] + eps) - 1
        df[f'FI_bh_{n}'] = df[f'FI_bh_{n}'].shift(1)
        extra_agg_dict[f'FI_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['FI']
        del df['FIMA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'FI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['FI', 'FI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['FI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_PVT(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # PVT 指标
    for n in back_hour_list:
        """
        PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
        PVT_MA1=MA(PVT,N1)
        PVT_MA2=MA(PVT,N2)
        PVT 指标用价格的变化率作为权重求成交量的移动平均。PVT 指标
        与 OBV 指标的思想类似，但与 OBV 指标相比，PVT 考虑了价格不
        同涨跌幅的影响，而 OBV 只考虑了价格的变化方向。我们这里用 PVT
        短期和长期均线的交叉来产生交易信号。
        如果 PVT_MA1 上穿 PVT_MA2，则产生买入信号；
        如果 PVT_MA1 下穿 PVT_MA2，则产生卖出信号。
        """
        # PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME
        df['PVT'] = (df['close'] - df['close'].shift(1)) / (df['close'].shift(1) + eps) * df['volume']
        df['PVT_MA1'] = df['PVT'].rolling(n, min_periods=1).mean() # PVT_MA1=MA(PVT,N1)
        # df['PVT_MA2'] = df['PVT'].rolling(2 * n, min_periods=1).mean()

        # 去量纲  只引入一个ma做因子
        df[f'PVT_bh_{n}'] = df['PVT'] / df['PVT_MA1'] - 1
        df[f'PVT_bh_{n}'] = df[f'PVT_bh_{n}'].shift(1)
        extra_agg_dict[f'PVT_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['PVT']
        del df['PVT_MA1']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PVT_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PVT', 'PVT_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PVT']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_RSIV(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # RSIV 指标
    for n in back_hour_list:
        """
        N=20
        VOLUP=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        VOLDOWN=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        SUMUP=SUM(VOLUP,N)
        SUMDOWN=SUM(VOLDOWN,N)
        RSIV=100*SUMUP/(SUMUP+SUMDOWN)
        RSIV 的计算方式与 RSI 相同，只是把其中的价格变化 CLOSEREF(CLOSE,1)替换成了成交量 VOLUME。用法与 RSI 类似。我们
        这里将其用作动量指标，上穿 60 买入，下穿 40 卖出。
        """
        df['VOLUP'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0) # VOLUP=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        df['VOLDOWN'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0) #  VOLDOWN=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['SUMUP'] = df['VOLUP'].rolling(n, min_periods=1).sum() # SUMUP=SUM(VOLUP,N)
        df['SUMDOWN'] = df['VOLDOWN'].rolling(n, min_periods=1).sum() # SUMDOWN=SUM(VOLDOWN,N)
        df['RSIV'] = df['SUMUP'] / (df['SUMUP'] + df['SUMDOWN'] + eps) * 100 # RSIV=100*SUMUP/(SUMUP+SUMDOWN)

        df[f'RSIV_bh_{n}'] = df['RSIV'].shift(1)
        extra_agg_dict[f'RSIV_bh_{n}'] = 'first'
        # 删除中间过渡数据
        del df['VOLUP']
        del df['VOLDOWN']
        del df['SUMUP']
        del df['SUMDOWN']
        del df['RSIV']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'RSIV_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['RSIV', 'RSIV_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['RSIV']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_AMV(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # AMV 指标
    for n in back_hour_list:
        """
        N1=13
        N2=34
        AMOV=VOLUME*(OPEN+CLOSE)/2
        AMV1=SUM(AMOV,N1)/SUM(VOLUME,N1)
        AMV2=SUM(AMOV,N2)/SUM(VOLUME,N2)
        AMV 指标用成交量作为权重对开盘价和收盘价的均值进行加权移动
        平均。成交量越大的价格对移动平均结果的影响越大，AMV 指标减
        小了成交量小的价格波动的影响。当短期 AMV 线上穿/下穿长期 AMV
        线时，产生买入/卖出信号。
        """
        df['AMOV'] = df['volume'] * (df['open'] + df['close']) / 2 # AMOV=VOLUME*(OPEN+CLOSE)/2
        df['AMV1'] = df['AMOV'].rolling(n, min_periods=1).sum() / (eps + df['volume'].rolling(n, min_periods=1).sum()) # AMV1=SUM(AMOV,N1)/SUM(VOLUME,N1)
        # df['AMV2'] = df['AMOV'].rolling(n * 3).sum() / df['volume'].rolling(n * 3).sum()
        # 去量纲
        df['AMV'] = (df['AMV1'] - df['AMV1'].rolling(n).min()) / (
                df['AMV1'].rolling(n).max() - df['AMV1'].rolling(n).min() +eps)  # 标准化
        df[f'AMV_bh_{n}'] = df['AMV'].shift(1)
        extra_agg_dict[f'AMV_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['AMOV']
        del df['AMV1']
        del df['AMV']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'AMV_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['AMV', 'AMV_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['AMV']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_VRAMT(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # VRAMT 指标
    for n in back_hour_list:
        """
        N=40
        AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
        BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
        CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
        AVS=SUM(AV,N)
        BVS=SUM(BV,N)
        CVS=SUM(CV,N)
        VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
        VRAMT 的计算与 VR 指标（Volume Ratio）一样，只是把其中的成
        交量替换成了成交额。
        如果 VRAMT 上穿 180，则产生买入信号；
        如果 VRAMT 下穿 70，则产生卖出信号。
        """
        df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0) # AV=IF(CLOSE>REF(CLOSE,1),AMOUNT,0)
        df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0) # BV=IF(CLOSE<REF(CLOSE,1),AMOUNT,0)
        df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0) # CV=IF(CLOSE=REF(CLOSE,1),AMOUNT,0)
        df['AVS'] = df['AV'].rolling(n, min_periods=1).sum() # AVS=SUM(AV,N)
        df['BVS'] = df['BV'].rolling(n, min_periods=1).sum() # BVS=SUM(BV,N)
        df['CVS'] = df['CV'].rolling(n, min_periods=1).sum() # CVS=SUM(CV,N)
        df['VRAMT'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2 + eps) # VRAMT=(AVS+CVS/2)/(BVS+CVS/2)
        df[f'VRAMT_bh_{n}'] = df['VRAMT'].shift(1)
        extra_agg_dict[f'VRAMT_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['AV']
        del df['BV']
        del df['CV']
        del df['AVS']
        del df['BVS']
        del df['CVS']
        del df['VRAMT']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'VRAMT_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['VRAMT', 'VRAMT_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['VRAMT']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_WVAD(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # WVAD 指标
    for n in back_hour_list:
        """
        N=20
        WVAD=SUM(((CLOSE-OPEN)/(HIGH-LOW)*VOLUME),N)
        WVAD 是用价格信息对成交量加权的价量指标，用来比较开盘到收盘
        期间多空双方的力量。WVAD 的构造与 CMF 类似，但是 CMF 的权
        值用的是 CLV(反映收盘价在最高价、最低价之间的位置)，而 WVAD
        用的是收盘价与开盘价的距离（即蜡烛图的实体部分的长度）占最高
        价与最低价的距离的比例，且没有再除以成交量之和。
        WVAD 上穿 0 线，代表买方力量强；
        WVAD 下穿 0 线，代表卖方力量强。
        """
        # ((CLOSE-OPEN)/(HIGH-LOW)*VOLUME)
        df['VAD'] = (df['close'] - df['open']) / (df['high'] - df['low'] + eps) * df['volume']
        df['WVAD'] = df['VAD'].rolling(n, min_periods=1).sum() # WVAD=SUM(((CLOSE-OPEN)/(HIGH-LOW)*VOLUME),N)

        # 标准化
        df[f'WVAD_bh_{n}'] = (df['WVAD'] - df['WVAD'].rolling(n).min()) / (
                df['WVAD'].rolling(n).max() - df['WVAD'].rolling(n).min() + eps)
        df[f'WVAD_bh_{n}'] = df[f'WVAD_bh_{n}'].shift(1)
        extra_agg_dict[f'WVAD_bh_{n}'] = 'first'
        del df['VAD']
        del df['WVAD']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'WVAD_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['WVAD', 'WVAD_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['WVAD']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_OBV(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # OBV 指标
    for n in back_hour_list:
        """
        N1=10
        N2=30
        VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
        VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
        OBV=REF(OBV,1)+VOL
        OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
        OBV 指标把成交量分为正的成交量（价格上升时的成交量）和负的
        成交量（价格下降时）的成交量。OBV 就是分了正负之后的成交量
        的累计和。如果 OBV 和价格的均线一起上涨（下跌），则上涨（下
        跌）趋势被确认。如果 OBV 上升（下降）而价格的均线下降（上升），
        说明价格可能要反转，可能要开始新的下跌（上涨）行情。
        如果 OBV_HISTOGRAM 上穿 0 则买入，下穿 0 则卖出。
        """
        # VOL=IF(CLOSE>REF(CLOSE,1),VOLUME,-VOLUME)
        df['VOL'] = np.where(df['close'] > df['close'].shift(1), df['volume'], -df['volume'])
        # VOL=IF(CLOSE != REF(CLOSE,1),VOL,0)
        df['VOL'] = np.where(df['close'] != df['close'].shift(1), df['VOL'], 0)
        # OBV=REF(OBV,1)+VOL
        df['OBV'] = df['VOL']
        df['OBV'] = df['VOL'] + df['OBV'].shift(1)
        # OBV_HISTOGRAM=EMA(OBV,N1)-EMA(OBV,N2)
        df['OBV_HISTOGRAM'] = df['OBV'].ewm(n, adjust=False).mean() - df['OBV'].ewm(3 * n, adjust=False).mean()
        df[f'OBV_bh_{n}'] = df['OBV_HISTOGRAM'].shift(1)
        extra_agg_dict[f'OBV_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['VOL']
        del df['OBV']
        del df['OBV_HISTOGRAM']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'OBV_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['OBV', 'OBV_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['OBV']  # 由于有些时候返回计算的因子有多个，所以使用列表



def signal_factor_CMF(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # CMF 指标
    for n in back_hour_list:
        """
        N=60
        CMF=SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW),N)/SUM(VOLUME,N)
        CMF 用 CLV 对成交量进行加权，如果收盘价在高低价的中点之上，
        则为正的成交量（买方力量占优势）；若收盘价在高低价的中点之下，
        则为负的成交量（卖方力量占优势）。
        如果 CMF 上穿 0，则产生买入信号；
        如果 CMF 下穿 0，则产生卖出信号。
        """
        # ((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW)
        A = ((df['close'] - df['low']) - (df['high'] - df['close'])) * df['volume'] / (df['high'] - df['low'] + eps)
        # CMF=SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW),N)/SUM(VOLUME,N)
        df['CMF'] = A.rolling(n, min_periods=1).sum() / df['volume'].rolling(n, min_periods=1).sum()

        df[f'CMF_bh_{n}'] = df['CMF'].shift(1)
        extra_agg_dict[f'CMF_bh_{n}'] = 'first'
        del df['CMF']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'CMF_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['CMF', 'CMF_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['CMF']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_PVI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # PVI 指标
    for n in back_hour_list:
        """
        N=40
        PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/ CLOSE,0)
        PVI=CUM_SUM(PVI_INC)
        PVI_MA=MA(PVI,N)
        PVI 是成交量升高的交易日的价格变化百分比的累积。
        PVI 相关理论认为，如果当前价涨量增，则说明散户主导市场，PVI
        可以用来识别价涨量增的市场（散户主导的市场）。
        如果 PVI 上穿 PVI_MA，则产生买入信号；
        如果 PVI 下穿 PVI_MA，则产生卖出信号。
        """
        df['ref_close'] = (df['close'] - df['close'].shift(1)) / (df['close'] + eps) # (CLOSE-REF(CLOSE))/ CLOSE
        df['PVI_INC'] = np.where(df['volume'] > df['volume'].shift(1), df['ref_close'], 0) # PVI_INC=IF(VOLUME>REF(VOLUME,1),(CLOSE-REF(CLOSE))/ CLOSE,0)
        df['PVI'] = df['PVI_INC'].cumsum() #  PVI=CUM_SUM(PVI_INC)
        df['PVI_INC_MA'] = df['PVI'].rolling(n, min_periods=1).mean() # PVI_MA=MA(PVI,N)

        df[f'PVI_bh_{n}'] = df['PVI_INC_MA'].shift(1)
        extra_agg_dict[f'PVI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['ref_close']
        del df['PVI_INC']
        del df['PVI']
        del df['PVI_INC_MA']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'PVI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['PVI', 'PVI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['PVI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_TMF(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # TMF 指标
    for n in back_hour_list:
        """
        N=80
        HIGH_TRUE=MAX(HIGH,REF(CLOSE,1))
        LOW_TRUE=MIN(LOW,REF(CLOSE,1))
        TMF=EMA(VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TR
        UE-LOW_TRUE),N)/EMA(VOL,N)
        TMF 指标和 CMF 指标类似，都是用价格对成交量加权。但是 CMF
        指标用 CLV 做权重，而 TMF 指标用的是真实最低价和真实最高价，
        且取的是移动平均而不是求和。如果 TMF 上穿 0，则产生买入信号；
        如果 TMF 下穿 0，则产生卖出信号。
        """
        df['ref'] = df['close'].shift(1) # REF(CLOSE,1)
        df['max_high'] = df[['high', 'ref']].max(axis=1) # HIGH_TRUE=MAX(HIGH,REF(CLOSE,1))
        df['min_low'] = df[['low', 'ref']].min(axis=1) # LOW_TRUE=MIN(LOW,REF(CLOSE,1))
        # VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TRUE-LOW_TRUE)
        T = df['volume'] * (2 * df['close'] - df['max_high'] - df['min_low']) / (df['max_high'] - df['min_low'] + eps)
        # TMF=EMA(VOL*(2*CLOSE-HIGH_TRUE-LOW_TRUE)/(HIGH_TRUE-LOW_TRUE),N)/EMA(VOL,N)
        df['TMF'] = T.ewm(n, adjust=False).mean() / (df['volume'].ewm(n, adjust=False).mean() + eps)
        df[f'TMF_bh_{n}'] = df['TMF'].shift(1)
        extra_agg_dict[f'TMF_bh_{n}'] = 'first'
        # 删除中间数据
        del df['ref']
        del df['max_high']
        del df['min_low']
        del df['TMF']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TMF_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TMF', 'TMF_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TMF']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_MFI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # MFI 指标
    for n in back_hour_list:
        """
        N=14
        TYPICAL_PRICE=(HIGH+LOW+CLOSE)/3
        MF=TYPICAL_PRICE*VOLUME
        MF_POS=SUM(IF(TYPICAL_PRICE>=REF(TYPICAL_PRICE,1),M
        F,0),N)
        MF_NEG=SUM(IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),
        MF,0),N)
        MFI=100-100/(1+MF_POS/MF_NEG)
        MFI 指标的计算与 RSI 指标类似，不同的是，其在上升和下跌的条件
        判断用的是典型价格而不是收盘价，且其是对 MF 求和而不是收盘价
        的变化值。MFI 同样可以用来判断市场的超买超卖状态。
        如果 MFI 上穿 80，则产生买入信号；
        如果 MFI 下穿 20，则产生卖出信号。
        """
        df['price'] = (df['high'] + df['low'] + df['close']) / 3 # TYPICAL_PRICE=(HIGH+LOW+CLOSE)/3
        df['MF'] = df['price'] * df['volume'] # MF=TYPICAL_PRICE*VOLUME
        df['pos'] = np.where(df['price'] >= df['price'].shift(1), df['MF'], 0) # IF(TYPICAL_PRICE>=REF(TYPICAL_PRICE,1),MF,0)MF,0),N)
        df['MF_POS'] = df['pos'].rolling(n, min_periods=1).sum()
        df['neg'] = np.where(df['price'] <= df['price'].shift(1), df['MF'], 0) # IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),MF,0)
        df['MF_NEG'] = df['neg'].rolling(n, min_periods=1).sum() # MF_NEG=SUM(IF(TYPICAL_PRICE<=REF(TYPICAL_PRICE,1),MF,0),N)

        df['MFI'] = 100 - 100 / (1 + df['MF_POS'] / (df['MF_NEG'] + eps)) # MFI=100-100/(1+MF_POS/MF_NEG)

        df[f'MFI_bh_{n}'] = df['MFI'].shift(1)
        extra_agg_dict[f'MFI_bh_{n}'] = 'first'
        # 删除中间数据
        del df['price']
        del df['MF']
        del df['pos']
        del df['MF_POS']
        del df['neg']
        del df['MF_NEG']
        del df['MFI']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'MFI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['MFI', 'MFI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['MFI']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_ADOSC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # ADOSC 指标
    for n in back_hour_list:
        """
        AD=CUM_SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW))
        AD_EMA1=EMA(AD,N1)
        AD_EMA2=EMA(AD,N2) 
        ADOSC=AD_EMA1-AD_EMA2
        ADL（收集派发线）指标是成交量的加权累计求和，其中权重为 CLV
        指标。ADL 指标可以与 OBV 指标进行类比。不同的是 OBV 指标只
        根据价格的变化方向把成交量分为正、负成交量再累加，而 ADL 是 用 CLV 指标作为权重进行成交量的累加。我们知道，CLV 指标衡量
        收盘价在最低价和最高价之间的位置，CLV>0(<0),则收盘价更靠近最
        高（低）价。CLV 越靠近 1(-1)，则收盘价越靠近最高（低）价。如
        果当天的 CLV>0，则 ADL 会加上成交量*CLV（收集）；如果当天的
        CLV<0，则 ADL 会减去成交量*CLV（派发）。
        ADOSC 指标是 ADL（收集派发线）指标的短期移动平均与长期移动
        平均之差。如果 ADOSC 上穿 0，则产生买入信号；如果 ADOSC 下 穿 0，则产生卖出信号。
        """
        # ((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW)
        df['AD'] = ((df['close'] - df['low']) - (df['high'] - df['close'])) * df['volume'] / (
                df['high'] - df['low'] + eps)
        df['AD_sum'] = df['AD'].cumsum() # AD=CUM_SUM(((CLOSE-LOW)-(HIGH-CLOSE))*VOLUME/(HIGH-LOW))
        df['AD_EMA1'] = df['AD_sum'].ewm(n, adjust=False).mean() # AD_EMA1=EMA(AD,N1)
        df['AD_EMA2'] = df['AD_sum'].ewm(n * 2, adjust=False).mean() # AD_EMA2=EMA(AD,N2)
        df['ADOSC'] = df['AD_EMA1'] - df['AD_EMA2'] # ADOSC=AD_EMA1-AD_EMA2

        # 标准化
        df[f'ADOSC_bh_{n}'] = (df['ADOSC'] - df['ADOSC'].rolling(n).min()) / (
                df['ADOSC'].rolling(n).max() - df['ADOSC'].rolling(n).min() + eps)
        df[f'ADOSC_bh_{n}'] = df[f'ADOSC_bh_{n}'].shift(1)
        extra_agg_dict[f'ADOSC_bh_{n}'] = 'first'
        # 删除中间数据
        del df['AD']
        del df['AD_sum']
        del df['AD_EMA2']
        del df['AD_EMA1']
        del df['ADOSC']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'ADOSC_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['ADOSC', 'ADOSC_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['ADOSC']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_VR(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # VR 指标
    for n in back_hour_list:
        """
        N=40
        AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        AVS=SUM(AV,N)
        BVS=SUM(BV,N)
        CVS=SUM(CV,N)
        VR=(AVS+CVS/2)/(BVS+CVS/2)

        VR 用过去 N 日股价上升日成交量与下降日成交量的比值来衡量多空
        力量对比。当 VR 小于 70 时，表示市场较为低迷；上穿 70 时表示市
        场可能有好转；上穿 250 时表示多方力量压倒空方力量。当 VR>300
        时，市场可能过热、买方力量过强，下穿 300 表明市场可能要反转。
        如果 VR 上穿 250，则产生买入信号；
        如果 VR 下穿 300，则产生卖出信号。
        """
        df['AV'] = np.where(df['close'] > df['close'].shift(1), df['volume'], 0) # AV=IF(CLOSE>REF(CLOSE,1),VOLUME,0)
        df['BV'] = np.where(df['close'] < df['close'].shift(1), df['volume'], 0) # BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['CV'] = np.where(df['close'] == df['close'].shift(1), df['volume'], 0) # BV=IF(CLOSE<REF(CLOSE,1),VOLUME,0)
        df['AVS'] = df['AV'].rolling(n, min_periods=1).sum() # AVS=SUM(AV,N)
        df['BVS'] = df['BV'].rolling(n, min_periods=1).sum() # BVS=SUM(BV,N)
        df['CVS'] = df['CV'].rolling(n, min_periods=1).sum() # CVS=SUM(CV,N)
        df['VR'] = (df['AVS'] + df['CVS'] / 2) / (df['BVS'] + df['CVS'] / 2 + eps) # VR=(AVS+CVS/2)/(BVS+CVS/2)
        df[f'VR_bh_{n}'] = df['VR'].shift(1)
        extra_agg_dict[f'VR_bh_{n}'] = 'first'
        # 删除中间数据
        del df['AV']
        del df['BV']
        del df['CV']
        del df['AVS']
        del df['BVS']
        del df['CVS']
        del df['VR']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'VR_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['VR', 'VR_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['VR']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_KO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # KO 指标
    for n in back_hour_list:
        """
        N1=34
        N2=55
        TYPICAL=(HIGH+LOW+CLOSE)/3
        VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
        VOLUME_EMA1=EMA(VOLUME,N1)
        VOLUME_EMA2=EMA(VOLUME,N2)
        KO=VOLUME_EMA1-VOLUME_EMA2
        这个技术指标的目的是为了观察短期和长期股票资金的流入和流出
        的情况。它的主要用途是确认股票价格趋势的方向和强度。KO 与
        OBV,VPT 等指标类似，都是用价格对成交量进行加权。KO 用的是典
        型价格的变化（只考虑变化方向，不考虑变化量），OBV 用的是收
        盘价的变化（只考虑变化方向，不考虑变化量），VPT 用的是价格的
        变化率（即考虑方向又考虑变化幅度）。
        如果 KO 上穿 0，则产生买入信号；
        如果 KO 下穿 0，则产生卖出信号。
        """
        df['price'] = (df['high'] + df['low'] + df['close']) / 3 # TYPICAL=(HIGH+LOW+CLOSE)/3
        df['V'] = np.where(df['price'] > df['price'].shift(1), df['volume'], -df['volume']) # VOLUME=IF(TYPICAL-REF(TYPICAL,1)>=0,VOLUME,-VOLUME)
        df['V_ema1'] = df['V'].ewm(n, adjust=False).mean() # VOLUME_EMA1=EMA(VOLUME,N1)
        df['V_ema2'] = df['V'].ewm(int(n * 1.618), adjust=False).mean() # VOLUME_EMA2=EMA(VOLUME,N2)
        df['KO'] = df['V_ema1'] - df['V_ema2'] # KO=VOLUME_EMA1-VOLUME_EMA2
        # 标准化
        df[f'KO_bh_{n}'] = (df['KO'] - df['KO'].rolling(n).min()) / (
                df['KO'].rolling(n).max() - df['KO'].rolling(n).min() + eps)
        df[f'KO_bh_{n}'] = df[f'KO_bh_{n}'].shift(1)
        extra_agg_dict[f'KO_bh_{n}'] = 'first'
        # 删除中间数据
        del df['price']
        del df['V']
        del df['V_ema1']
        del df['V_ema2']
        del df['KO']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'KO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['KO', 'KO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['KO']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_gap(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    """
    基于分享会策略指标 参考马老板https://forum.quantclass.cn/d/1515-xjbg-gaptrue
    :param df:
    :param extra_agg_dict:
    :param back_hour_list:
    :return:
    """
    # gap指标
    for n in back_hour_list:
        ma = df['close'].rolling(window=n, min_periods=1).mean()
        wma = talib.WMA(df['close'], n)
        gap = wma - ma
        df[f'gap_bh_{n}'] = (gap / abs(gap).rolling(window=n, min_periods=1).sum())
        df[f'gap_bh_{n}'] = df[f'gap_bh_{n}'].shift(1)
        extra_agg_dict[f'gap_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'gap_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['gap', 'gap_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['gap']  # 由于有些时候返回计算的因子有多个，所以使用列表


##############################添加冒险侠扩展指标#################################


def signal_factor_TMA2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # TMA 指标
    for n in back_hour_list:
        """
        N=20
        CLOSE_MA=MA(CLOSE,N)
        TMA=MA(CLOSE_MA,N)
        TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
        越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
        靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
        卖出信号。
        """
        price = (df['high'] + df['low']) /2
        ma = price.rolling(n, min_periods=1).mean() # CLOSE_MA=MA(CLOSE,N)
        tma = ma.rolling(n, min_periods=1).mean() # TMA=MA(CLOSE_MA,N)
        df[f'TMA2_bh_{n}'] = price / (tma + eps) - 1
        df[f'TMA2_bh_{n}'] = df[f'TMA2_bh_{n}'].shift(1)
        extra_agg_dict[f'TMA2_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TMA2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TMA2', 'TMA2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TMA2']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_TMA3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # TMA 指标
    for n in back_hour_list:
        """
        N=20
        CLOSE_MA=MA(CLOSE,N)
        TMA=MA(CLOSE_MA,N)
        TMA 均线与其他的均线类似，不同的是，像 EMA 这类的均线会赋予
        越靠近当天的价格越高的权重，而 TMA 则赋予考虑的时间段内时间
        靠中间的价格更高的权重。如果收盘价上穿/下穿 TMA 则产生买入/
        卖出信号。
        """
        price = (df['high'].rolling(n, min_periods=1).max() + df['low'].rolling(n, min_periods=1).min()) / 2.
        ma = price.rolling(n, min_periods=1).mean() # CLOSE_MA=MA(CLOSE,N)
        tma = ma.rolling(n, min_periods=1).mean() # TMA=MA(CLOSE_MA,N)
        df[f'TMA3_bh_{n}'] = price / (tma + eps) - 1
        df[f'TMA3_bh_{n}'] = df[f'TMA3_bh_{n}'].shift(1)
        extra_agg_dict[f'TMA3_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'TMA3_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['TMA3', 'TMA3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['TMA3']  # 由于有些

def signal_factor_bias2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bias # 涨跌幅更好的表达方式：bias，币价偏离均线的比例。
    for n in back_hour_list:
        price = df[['high', 'low']].sum(axis=1) / 2.
        ma = price.rolling(n, min_periods=1).mean()
        df[f'bias2_bh_{n}'] = price / (ma + eps) - 1
        df[f'bias2_bh_{n}'] = df[f'bias2_bh_{n}'].shift(1)
        extra_agg_dict[f'bias2_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bias2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bias2', 'bias2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bias2']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_bias3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bias # 涨跌幅更好的表达方式：bias，币价偏离均线的比例。
    for n in back_hour_list:
        price = df[['high', 'low', 'close']].sum(axis=1) / 3.
        ma = price.rolling(n, min_periods=1).mean()
        df[f'bias3_bh_{n}'] = price / (ma + eps) - 1
        df[f'bias3_bh_{n}'] = df[f'bias3_bh_{n}'].shift(1)
        extra_agg_dict[f'bias3_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bias3_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bias3', 'bias3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bias3']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_VIDYA2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # VIDYA
    for n in back_hour_list:
        """
        N=10
        VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER
        （EfficiencyRatio）指标。在当前趋势较强时，ER 值较大，VIDYA
        会赋予当前价格更大的权重，使得 VIDYA 紧随价格变动，减小其滞
        后性；在当前趋势较弱（比如振荡市中）,ER 值较小，VIDYA 会赋予
        当前价格较小的权重，增大 VIDYA 的滞后性，使其更加平滑，避免
        产生过多的交易信号。
        当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
        """
        price = df[['high', 'low']].sum(axis=1) / 2.
        df['abs_diff_close'] = abs(price - price.shift(n)) # ABS(CLOSE-REF(CLOSE,N))
        df['abs_diff_close_sum'] = df['abs_diff_close'].rolling(n, min_periods=1).sum() # SUM(ABS(CLOSE-REF(CLOSE,1))
        VI = df['abs_diff_close'] / (df['abs_diff_close_sum'] + eps )# VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
        VIDYA = VI * price + (1 - VI) * price.shift(1) # VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
        # 进行无量纲处理
        df[f'VIDYA2_bh_{n}'] = VIDYA / (price + eps) - 1
        df[f'VIDYA2_bh_{n}'] = df[f'VIDYA2_bh_{n}'].shift(1)
        extra_agg_dict[f'VIDYA2_bh_{n}'] = 'first'
        # 删除中间数据
        del df['abs_diff_close']
        del df['abs_diff_close_sum']
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'VIDYA2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['VIDYA2', 'VIDYA2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['VIDYA2']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_aroon(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # AroonOs
    for n in back_hour_list:
        """
        费时
        AroonUp = (N - HIGH_LEN) / N * 100
        AroonDown = (N - LOW_LEN) / N * 100
        AroonOs = AroonUp - AroonDown
        其中 HIGH_LEN，LOW_LEN 分别为过去N天最高/最低价距离当前日的天数
        AroonUp、AroonDown指标分别为考虑的时间段内最高价、最低价出现时间与当前时间的距离占时间段长度的百分比。
        如果价格当天创新高，则AroonUp等于100；创新低，则AroonDown等于100。Aroon指标为两者之差，
        变化范围为-100到100。Aroon指标大于0表示股价呈上升趋势，Aroon指标小于0表示股价呈下降趋势。
        距离0点越远则趋势越强。我们这里以20/-20为阈值构造交易信号。如果AroonOs上穿20/下穿-20则产生买入/卖出信号
        """
        # 求列的 rolling 窗口内的最大值对于的 index
        high_len = df['high'].rolling(n, min_periods=1).apply(lambda x: pd.Series(x).idxmax())
        # 当前日距离过去N天最高价的天数
        high_len = df.index - high_len
        aroon_up = 100 * (n - high_len) / n

        low_len = df['low'].rolling(n, min_periods=1).apply(lambda x: pd.Series(x).idxmin())
        low_len = df.index - low_len
        aroon_down = 100 * (n - low_len) / n
        # 进行无量纲处理
        df[f'aroon_bh_{n}'] = aroon_up - aroon_down
        df[f'aroon_bh_{n}'] = df[f'aroon_bh_{n}'].shift(1)
        extra_agg_dict[f'aroon_bh_{n}'] = 'first'
        # 删除中间数据

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'aroon_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['aroon', 'aroon_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['aroon']

def signal_factor_KC(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # KC
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-REF(LOW,1)))
        ATR=MA(TR,N)
        Middle=EMA(CLOSE,20)
        UPPER=MIDDLE+2*ATR
        LOWER=MIDDLE-2*ATR
        KC指标（KeltnerChannel）与布林带类似，都是用价格的移动平均构造中轨，不同的是表示波幅的方法，
        这里用ATR来作为波幅构造上下轨。价格突破上轨，可看成新的上升趋势，买入；价格突破下轨，可看成新的下降趋势，卖出。
        """
        df['c1'] = df['high'] - df['low'] # HIGH-LOW
        df['c2'] = abs(df['high'] - df['close'].shift(1)) # ABS(HIGH-REF(CLOSE,1)
        df['c3'] = abs(df['low'] - df['close'].shift(1)) # ABS(LOW-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1) # TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean() # ATR=MA(TR,N)
        df['middle'] = df['close'].ewm(n, adjust=False).mean() # MIDDLE=EMA(CLOSE,N)
        
        df['KC_upper'] = df['middle'] + 2 * df['ATR']
        df['KC_lower'] = df['middle'] - 2 * df['ATR']
        # 标准化
        df[f'KC_upper_bh_{n}'] = (df['KC_upper'] - df['KC_upper'].rolling(n, min_periods=1).mean()) / (df['KC_upper'].rolling(n, min_periods=1).max() - df['KC_upper'].rolling(n, min_periods=1).min() + eps)
        df[f'KC_lower_bh_{n}'] = (df['KC_lower'] - df['KC_lower'].rolling(n, min_periods=1).mean()) / (df['KC_lower'].rolling(n, min_periods=1).max() - df['KC_lower'].rolling(n, min_periods=1).min() + eps)
        df[f'KC_upper_bh_{n}'] = df[f'KC_upper_bh_{n}'].shift(1)
        df[f'KC_lower_bh_{n}'] = df[f'KC_lower_bh_{n}'].shift(1)
        extra_agg_dict[f'KC_upper_bh_{n}'] = 'first'
        extra_agg_dict[f'KC_lower_bh_{n}'] = 'first'
        # 删除中间数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['middle']

        if add:
            for _ in [f'KC_upper_bh_{n}', f'KC_lower_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['KC_upper', 'KC_lower', 'KC_upper_diff', 'KC_lower_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['KC_upper', 'KC_lower']


def signal_factor_KC2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # KC
    for n in back_hour_list:
        """
        N=14
        TR=MAX(ABS(HIGH-LOW),ABS(HIGH-REF(CLOSE,1)),ABS(REF(CLOSE,1)-REF(LOW,1)))
        ATR=MA(TR,N)
        Middle=EMA(CLOSE,20)
        UPPER=MIDDLE+2*ATR
        LOWER=MIDDLE-2*ATR
        KC指标（KeltnerChannel）与布林带类似，都是用价格的移动平均构造中轨，不同的是表示波幅的方法，
        这里用ATR来作为波幅构造上下轨。价格突破上轨，可看成新的上升趋势，买入；价格突破下轨，可看成新的下降趋势，卖出。
        """
        df['c1'] = df['high'] - df['low']  # HIGH-LOW
        df['c2'] = abs(df['high'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1)
        df['c3'] = abs(df['low'] - df['close'].shift(1))  # ABS(LOW-REF(CLOSE,1))
        df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)  # TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
        df['ATR'] = df['TR'].rolling(n, min_periods=1).mean()  # ATR=MA(TR,N)
        df['middle'] = df['close'].ewm(n, adjust=False).mean()  # MIDDLE=EMA(CLOSE,N)

        df['KC_upper'] = df['middle'] + 2 * df['ATR']
        df['KC_lower'] = df['middle'] - 2 * df['ATR']
        # 标准化
        df[f'KC2_upper_bh_{n}'] = (df['close'] - df['KC_upper']) / (4 * df['ATR'] + eps)
        df[f'KC2_lower_bh_{n}'] = (df['close'] - df['KC_lower']) / (4 * df['ATR'] + eps)
        df[f'KC2_upper_bh_{n}'] = df[f'KC2_upper_bh_{n}'].shift(1)
        df[f'KC2_lower_bh_{n}'] = df[f'KC2_lower_bh_{n}'].shift(1)
        extra_agg_dict[f'KC2_upper_bh_{n}'] = 'first'
        extra_agg_dict[f'KC2_lower_bh_{n}'] = 'first'
        # 删除中间数据
        del df['c1']
        del df['c2']
        del df['c3']
        del df['TR']
        del df['ATR']
        del df['middle']

        if add:
            for _ in [f'KC2_upper_bh_{n}', f'KC2_lower_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['KC2_upper', 'KC2_lower', 'KC2_upper_diff', 'KC2_lower_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['KC2_upper', 'KC2_lower']

def signal_factor_IC2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # IC 指标
    for n in back_hour_list:
        """
        N1=9
        N2=26
        N3=52
        TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        SPAN_A=(TS+KS)/2
        SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2
        在 IC 指标中，SPAN_A 与 SPAN_B 之间的部分称为云。如果价格在
        云上，则说明是上涨趋势（如果 SPAN_A>SPAN_B，则上涨趋势强
        烈；否则上涨趋势较弱）；如果价格在云下，则为下跌趋势（如果
        SPAN_A<SPAN_B，则下跌趋势强烈；否则下跌趋势较弱）。该指
        标的使用方式与移动平均线有许多相似之处，比如较快的线（TS）突
        破较慢的线（KS），价格突破 KS,价格突破云，SPAN_A 突破 SPAN_B
        等。我们产生信号的方式是：如果价格在云上方 SPAN_A>SPAN_B，
        则当价格上穿 KS 时买入；如果价格在云下方且 SPAN_A<SPAN_B，
        则当价格下穿 KS 时卖出。
        """
        n2 = 3 * n
        n3 = 2 * n2
        df['max_high_1'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N1)
        df['min_low_1'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N1)
        df['TS'] = (df['max_high_1'] + df['min_low_1']) / 2 # TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['max_high_2'] = df['high'].rolling(n2, min_periods=1).max() # MAX(HIGH,N2)
        df['min_low_2'] = df['low'].rolling(n2, min_periods=1).min() # MIN(LOW,N2)
        df['KS'] = (df['max_high_2'] + df['min_low_2']) / 2 # KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        df['span_A'] = (df['TS'] + df['KS']) / 2 # SPAN_A=(TS+KS)/2
        df['max_high_3'] = df['high'].rolling(n3, min_periods=1).max() # MAX(HIGH,N3)
        df['min_low_3'] = df['low'].rolling(n3, min_periods=1).min() # MIN(LOW,N3)
        df['span_B'] = (df['max_high_3'] + df['min_low_3']) / 2 # SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2

        # 去量纲
        df[f'IC2_spanA_bh_{n}'] = df['close'] / (df['span_A'] + eps)
        df[f'IC2_spanB_bh_{n}'] = df['close'] / (df['span_B'] + eps)
        df[f'IC2_spanA_bh_{n}'] = df[f'IC2_spanA_bh_{n}'].shift(1)
        df[f'IC2_spanB_bh_{n}'] = df[f'IC2_spanB_bh_{n}'].shift(1)
        extra_agg_dict[f'IC2_spanA_bh_{n}'] = 'first'
        extra_agg_dict[f'IC2_spanB_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['max_high_1']
        del df['max_high_2']
        del df['max_high_3']
        del df['min_low_1']
        del df['min_low_2']
        del df['min_low_3']
        del df['TS']
        del df['KS']
        del df['span_A']
        del df['span_B']
        if add:
            for _ in [f'IC2_spanA_bh_{n}', f'IC2_spanB_bh_{n}']:
                # 差分
                add_diff(_df=df, _d_list=diff_d, _name=_, _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['IC2_spanA', 'IC2_spanB', 'IC2_spanA_diff', 'IC2_spanB_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['IC2_spanA', 'IC2_spanB']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_IC3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # IC 指标
    for n in back_hour_list:
        """
        N1=9
        N2=26
        N3=52
        TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        SPAN_A=(TS+KS)/2
        SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2
        在 IC 指标中，SPAN_A 与 SPAN_B 之间的部分称为云。如果价格在
        云上，则说明是上涨趋势（如果 SPAN_A>SPAN_B，则上涨趋势强
        烈；否则上涨趋势较弱）；如果价格在云下，则为下跌趋势（如果
        SPAN_A<SPAN_B，则下跌趋势强烈；否则下跌趋势较弱）。该指
        标的使用方式与移动平均线有许多相似之处，比如较快的线（TS）突
        破较慢的线（KS），价格突破 KS,价格突破云，SPAN_A 突破 SPAN_B
        等。我们产生信号的方式是：如果价格在云上方 SPAN_A>SPAN_B，
        则当价格上穿 KS 时买入；如果价格在云下方且 SPAN_A<SPAN_B，
        则当价格下穿 KS 时卖出。
        """
        n2 = 3 * n
        n3 = 2 * n2
        df['max_high_1'] = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N1)
        df['min_low_1'] = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N1)
        df['TS'] = (df['max_high_1'] + df['min_low_1']) / 2 # TS=(MAX(HIGH,N1)+MIN(LOW,N1))/2
        df['max_high_2'] = df['high'].rolling(n2, min_periods=1).max() # MAX(HIGH,N2)
        df['min_low_2'] = df['low'].rolling(n2, min_periods=1).min() # MIN(LOW,N2)
        df['KS'] = (df['max_high_2'] + df['min_low_2']) / 2 # KS=(MAX(HIGH,N2)+MIN(LOW,N2))/2
        df['span_A'] = (df['TS'] + df['KS']) / 2 # SPAN_A=(TS+KS)/2
        df['max_high_3'] = df['high'].rolling(n3, min_periods=1).max() # MAX(HIGH,N3)
        df['min_low_3'] = df['low'].rolling(n3, min_periods=1).min() # MIN(LOW,N3)
        df['span_B'] = (df['max_high_3'] + df['min_low_3']) / 2 # SPAN_B=(MAX(HIGH,N3)+MIN(LOW,N3))/2

        # 去量纲
        df[f'IC3_bh_{n}'] = (df['span_A'] - df['span_B']) / (df['span_A'] + df['span_B'] + eps)
        df[f'IC3_bh_{n}'] = df[f'IC3_bh_{n}'].shift(1)
        extra_agg_dict[f'IC3_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['max_high_1']
        del df['max_high_2']
        del df['max_high_3']
        del df['min_low_1']
        del df['min_low_2']
        del df['min_low_3']
        del df['TS']
        del df['KS']
        del df['span_A']
        del df['span_B']
        if add:
                # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'IC3_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['IC3', 'IC3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['IC3']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_NVI(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # NVI 指标
    for n in back_hour_list:
        """
        N=144
        NVI_INC=IF(VOLUME<REF(VOLUME,1),1+(CLOSE-REF(CLOSE,1))/CLOSE,1)
        NVI_INC[0]=100
        NVI=CUM_PROD(NVI_INC)
        NVI_MA=MA(NVI,N)
        NVI是成交量降低的交易日的价格变化百分比的累积。NVI相关理论认为，如果当前价涨量缩，
        则说明大户主导市场，NVI可以用来识别价涨量缩的市场（大户主导的市场）。
        如果NVI上穿NVI_MA，则产生买入信号；
        如果NVI下穿NVI_MA，则产生卖出信号。
        """
        nvi_inc = np.where(df['volume'] < df['volume'].shift(1),
                           1 + (df['close'] - df['close'].shift(1)) / (eps + df['close']), 1)
        nvi_inc[0] = 100
        nvi = pd.Series(nvi_inc).cumprod()
        nvi_ma = nvi.rolling(n, min_periods=1).mean()
        # 去量纲
        df[f'NVI_bh_{n}'] = nvi - nvi_ma
        df[f'NVI_bh_{n}'] = df[f'NVI_bh_{n}'].shift(1)
        extra_agg_dict[f'NVI_bh_{n}'] = 'first'
        # 删除中间过程数据

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'NVI_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['NVI', 'NVI_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['NVI']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_VAO(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

    # VAO 指标
    for n in back_hour_list:
        """
        WEIGHTED_VOLUME=VOLUME*(CLOSE-(HIGH+LOW)/2)
        VAO=REF(VAO,1)+WEIGHTED_VOLUME
        N1=10
        N2=30
        VAO_MA1=MA(VAO,N1)
        VAO_MA2=MA(VAO,N2)
        VAO与PVT类似，都综合考虑成交量和价格,以价格的变化为权重对成交量进行加权。
        但是PVT考虑的是两天的价格变化率，而VAO考虑的是日内的价格。
        当VAO的短期均线上穿VAO的长期均线时，做多；反之做空。
        """
        wv = df['volume'] * (df['close'] - 0.5 * df['high'] - 0.5 * df['low'])
        vao = wv + wv.shift(1)
        vao_ma1 = vao.rolling(n, min_periods=1).mean()
        vao_ma2 = vao.rolling(int(3 * n), min_periods=1).mean()
        # 去量纲
        df[f'VAO_bh_{n}'] = (vao_ma1 - vao_ma2) / (vao_ma1 + vao_ma2 + eps)
        df[f'VAO_bh_{n}'] = df[f'VAO_bh_{n}'].shift(1)
        extra_agg_dict[f'VAO_bh_{n}'] = 'first'
        # 删除中间过程数据
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'VAO_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['VAO', 'VAO_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['VAO']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_mtm2(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    """
    自适用布林mtm
    :param df:
    :param extra_agg_dict:
    :param back_hour_list:
    :return:
    """
    # mtm
    for n in back_hour_list:
        df['mtm'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_mean'] = df['mtm'].rolling(n, min_periods=1).mean()
        df['mtm_std'] = df['mtm'].rolling(n, min_periods=1).std(ddof=0)
        df[f'mtm2_bh_{n}'] = (df['mtm'] - df['mtm_mean']) / (df['mtm'].rolling(n, min_periods=1).max() - df['mtm'].rolling(n, min_periods=1).min() + eps)
        df[f'mtm2_bh_{n}'] = df[f'mtm2_bh_{n}'].shift(1)
        extra_agg_dict[f'mtm2_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'mtm2_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['mtm2', 'mtm2_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['mtm']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_mtm(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    """
    自适用布林mtm
    :param df:
    :param extra_agg_dict:
    :param back_hour_list:
    :return:
    """
    # mtm
    for n in back_hour_list:
        df['mtm'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_mean'] = df['mtm'].rolling(n, min_periods=1).mean()
        df['mtm_std'] = df['mtm'].rolling(n, min_periods=1).std(ddof=0)
        # 进行标准化
        df[f'mtm_bh_{n}'] = (df['mtm'] - df['mtm_mean']) / (df['mtm_std'] + eps)
        df[f'mtm_bh_{n}'] = df[f'mtm_bh_{n}'].shift(1)
        extra_agg_dict[f'mtm_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'mtm_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['mtm', 'mtm_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['mtm']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_v1_up(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # v1_up
    for n in back_hour_list:
        n1 = n
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
        indicator *= wd_atr * mtm_atr * mtm_atr_mean
        indicator = pd.Series(indicator)

        # 对新策略因子计算自适应布林
        median = indicator.rolling(window=n1).mean()
        std = indicator.rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        z_score = abs(indicator - median) / (std + eps)
        m1 = pd.Series(z_score).rolling(window=n1, min_periods=1).max()
        up1 = median + std * m1
        factor1 = up1 - indicator

        df[f'v1_up_bh_{n}'] = factor1.shift(1)
        extra_agg_dict[f'v1_up_bh_{n}'] = 'first'

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'v1_up_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['v1_up', 'v1_up_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['v1_up']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_force(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    """
    丁老板新增force
    :param df:
    :param extra_agg_dict:
    :param back_hour_list:
    :return:
    """
    # https://forum.quantclass.cn/d/3626-biascci12878459605936-72625
    for n in back_hour_list:
        df['force'] = df['quote_volume'] * (df['close'] - df['close'].shift(1))
        df[f'force_bh_{n}'] = df['force'].rolling(n, min_periods=1).mean()
        df[f'force_bh_{n}'] = df[f'force_bh_{n}'].shift(1)
        extra_agg_dict[f'force_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'force_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['force', 'force_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['force']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_dif_mean(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    """
    计算dif 均线
    :param df:
    :param extra_agg_dict:
    :param back_hour_list:
    :return:
    """
    for n in back_hour_list:
        # 计算均线
        df['median'] = df['close'].rolling(n, min_periods=1).mean()
        # 计算上轨、下轨道
        # 计算每根k线收盘价和均线的差值，取绝对数
        df['dif'] = abs(df['close'] - df['median'])
        # 计算平均差
        df[f'dif_mean_bh_{n}'] = (df['dif'] - df['dif'].rolling(n).min()) / (
                df['dif'].rolling(n).max() - df['dif'].rolling(n).min() + eps)
        df[f'dif_mean_bh_{n}'] = df[f'dif_mean_bh_{n}'].shift(1)
        extra_agg_dict[f'dif_mean_bh_{n}'] = 'first'
        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'dif_mean_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['dif_mean', 'dif_mean_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['dif_mean']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bolling(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling  基于布林带宽带求宽度对其寻求因子
    for n in back_hour_list:
        # 计算布林上下轨
        df['std'] = df['close'].rolling(n, min_periods=1).std()
        df['ma'] = df['close'].rolling(n, min_periods=1).mean()
        df['upper'] = df['ma'] + 1.0 * df['std']
        df['lower'] = df['ma'] - 1.0 * df['std']
        # 将上下轨中间的部分设为0
        condition_0 = (df['close'] <= df['upper']) & (df['close'] >= df['lower'])
        condition_1 = df['close'] > df['upper']
        condition_2 = df['close'] < df['lower']
        df.loc[condition_0, 'distance'] = 0
        df.loc[condition_1, 'distance'] = df['close'] - df['upper']
        df.loc[condition_2, 'distance'] = df['close'] - df['lower']
        df['bolling'] = df['distance'] / (df['std'] + eps)
        del df['std']
        del df['ma']
        del df['upper']
        del df['lower']
        del df['distance']

        df[f'bolling_bh_{n}'] = df['bolling'].shift(1)
        extra_agg_dict[f'bolling_bh_{n}'] = 'first'

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling', 'bolling_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bolling_v2_min_bak(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling v2  参考J神V2布林
    for n in back_hour_list:
        n1 = n

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
        indicator *= wd_atr * mtm_atr_mean
        indicator = pd.Series(indicator)

        # 对新策略因子计算自适应布林
        median = indicator.rolling(window=n1).mean()
        std = indicator.rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        z_score = abs(indicator - median) / (std + eps)
        # m1 = pd.Series(z_score).rolling(window=n1, min_periods=1).max()
        m1 = pd.Series(z_score).rolling(window=n1, min_periods=1).min()
        up1 = median + std * m1
        down1 = median - std * m1
        v2_up = up1 - indicator
        v2_down = down1 - indicator
        df[f'bolling_v2_up_min_bh_{n}'] = v2_up.shift(1)
        df[f'bolling_v2_down_min_bh_{n}'] = v2_down.shift(1)
        extra_agg_dict[f'bolling_v2_up_min_bh_{n}'] = 'first'
        extra_agg_dict[f'bolling_v2_down_min_bh_{n}'] = 'first'

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_up_min_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_down_min_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling_v2_up_min', 'bolling_v2_down_min', 'bolling_v2_up_min_diff', 'bolling_v2_down_min_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling_v2_up_min', 'bolling_v2_down_min']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bolling_v2_min(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling v2  参考J神V2布林
    for n in back_hour_list:
        # 计算动量因子
        df['mtm'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_mean'] = df['mtm'].rolling(window=n, min_periods=1).mean()

        # 基于价格atr，计算波动率因子wd_atr
        df['c1'] = df['high'] - df['low']
        df['c2'] = abs(df['high'] - df['close'].shift(1))
        df['c3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=n, min_periods=1).mean()
        df['avgPrice'] = df['close'].rolling(window=n, min_periods=1).mean()
        df['wd_atr'] = df['atr'] / df['avgPrice']  # === 波动率因子

        # 参考ATR，对MTM指标，计算波动率因子
        df['mtm_l'] = df['low'] / df['low'].shift(n) - 1
        df['mtm_h'] = df['high'] / df['high'].shift(n) - 1
        df['mtm_c'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
        df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr'] = df['mtm_tr'].rolling(window=n, min_periods=1).mean()

        # 参考ATR，对MTM mean指标，计算波动率因子

        # 参考ATR，对MTM mean指标，计算波动率因子
        df['mtm_l_mean'] = df['mtm_l'].rolling(window=n, min_periods=1).mean()
        df['mtm_h_mean'] = df['mtm_h'].rolling(window=n, min_periods=1).mean()
        df['mtm_c_mean'] = df['mtm_c'].rolling(window=n, min_periods=1).mean()
        df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
        df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n, min_periods=1).mean()

        indicator = 'mtm_mean'
        # mtm_mean指标分别乘以三个波动率因子
        df[indicator] = df[indicator] * df['mtm_atr_mean']
        df[indicator] = df[indicator] * df['wd_atr']
        # 对新策略因子计算自适应布林
        df['median'] = df[indicator].rolling(window=n, min_periods=1).mean()
        df['std'] = df[indicator].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        df['z_score'] = abs(df[indicator] - df['median']) / (df['std'] + eps)
        df['m'] = df['z_score'].rolling(window=n, min_periods=1).min()
        df['up'] = df['median'] + df['std'] * df['m']
        df['down'] = df['median'] - df['std'] * df['m']
        df['v2_up'] = df['up'] - df[indicator]
        df['v2_down'] = df['down'] - df[indicator]
        df[f'bolling_v2_up_min_bh_{n}'] = df['v2_up'].shift(1)
        df[f'bolling_v2_down_min_bh_{n}'] = df['v2_down'].shift(1)
        extra_agg_dict[f'bolling_v2_up_min_bh_{n}'] = 'first'
        extra_agg_dict[f'bolling_v2_down_min_bh_{n}'] = 'first'

        del df['mtm']
        del df['mtm_mean']
        del df['c1']
        del df['c2']
        del df['c3']
        del df['tr']
        del df['atr']
        del df['avgPrice']
        del df['wd_atr']
        del df['mtm_l']
        del df['mtm_h']
        del df['mtm_c']
        del df['mtm_c1']
        del df['mtm_c2']
        del df['mtm_c3']
        del df['mtm_tr']
        del df['mtm_atr']
        del df['mtm_l_mean']
        del df['mtm_h_mean']
        del df['mtm_c_mean']
        del df['mtm_atr_mean']
        del df['median']
        del df['std']
        del df['z_score']
        del df['m']
        del df['up']
        del df['down']
        del df['v2_up']
        del df['v2_down']

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_up_min_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_down_min_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling_v2_up_min', 'bolling_v2_down_min', 'bolling_v2_up_min_diff',
                                    'bolling_v2_down_min_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling_v2_up_min', 'bolling_v2_down_min']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bolling_v2_max(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling v2  参考J神V2布林
    for n in back_hour_list:
        # 计算动量因子
        df['mtm'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_mean'] = df['mtm'].rolling(window=n, min_periods=1).mean()

        # 基于价格atr，计算波动率因子wd_atr
        df['c1'] = df['high'] - df['low']
        df['c2'] = abs(df['high'] - df['close'].shift(1))
        df['c3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=n, min_periods=1).mean()
        df['avgPrice'] = df['close'].rolling(window=n, min_periods=1).mean()
        df['wd_atr'] = df['atr'] / df['avgPrice']  # === 波动率因子

        # 参考ATR，对MTM指标，计算波动率因子
        df['mtm_l'] = df['low'] / df['low'].shift(n) - 1
        df['mtm_h'] = df['high'] / df['high'].shift(n) - 1
        df['mtm_c'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
        df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr'] = df['mtm_tr'].rolling(window=n, min_periods=1).mean()
        
        # 参考ATR，对MTM mean指标，计算波动率因子

        # 参考ATR，对MTM mean指标，计算波动率因子
        df['mtm_l_mean'] = df['mtm_l'].rolling(window=n, min_periods=1).mean()
        df['mtm_h_mean'] = df['mtm_h'].rolling(window=n, min_periods=1).mean()
        df['mtm_c_mean'] = df['mtm_c'].rolling(window=n, min_periods=1).mean()
        df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
        df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n, min_periods=1).mean()
        
        indicator = 'mtm_mean'
        # mtm_mean指标分别乘以三个波动率因子
        df[indicator] = df[indicator] * df['mtm_atr_mean']
        df[indicator] = df[indicator] * df['wd_atr']
        # 对新策略因子计算自适应布林
        df['median'] = df[indicator].rolling(window=n, min_periods=1).mean()
        df['std'] = df[indicator].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        df['z_score'] = abs(df[indicator] - df['median']) / (df['std'] + eps)
        df['m'] = df['z_score'].rolling(window=n, min_periods=1).max()
        df['up'] = df['median'] + df['std'] * df['m']
        df['down'] = df['median'] - df['std'] * df['m']
        df['v2_up'] = df['up'] - df[indicator]
        df['v2_down'] = df['down'] - df[indicator]
        df[f'bolling_v2_up_max_bh_{n}'] = df['v2_up'].shift(1)
        df[f'bolling_v2_down_max_bh_{n}'] = df['v2_down'].shift(1)
        extra_agg_dict[f'bolling_v2_up_max_bh_{n}'] = 'first'
        extra_agg_dict[f'bolling_v2_down_max_bh_{n}'] = 'first'

        del df['mtm']
        del df['mtm_mean']
        del df['c1']
        del df['c2']
        del df['c3']
        del df['tr']
        del df['atr']
        del df['avgPrice']
        del df['wd_atr']
        del df['mtm_l']
        del df['mtm_h']
        del df['mtm_c']
        del df['mtm_c1']
        del df['mtm_c2']
        del df['mtm_c3']
        del df['mtm_tr']
        del df['mtm_atr']
        del df['mtm_l_mean']
        del df['mtm_h_mean']
        del df['mtm_c_mean']
        del df['mtm_atr_mean']
        del df['median']
        del df['std']
        del df['z_score']
        del df['m']
        del df['up']
        del df['down']
        del df['v2_up']
        del df['v2_down']

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_up_max_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_down_max_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling_v2_up_max', 'bolling_v2_down_max', 'bolling_v2_up_max_diff', 'bolling_v2_down_max_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling_v2_up_max', 'bolling_v2_down_max']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_bolling_v2_max_bak(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling v2  参考J神V2布林
    for n in back_hour_list:
        n1 = n

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
        indicator *= wd_atr * mtm_atr_mean
        indicator = pd.Series(indicator)

        # 对新策略因子计算自适应布林
        median = indicator.rolling(window=n1).mean()
        std = indicator.rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        z_score = abs(indicator - median) / (std + eps)
        m1 = pd.Series(z_score).rolling(window=n1, min_periods=1).max()
        up1 = median + std * m1
        down1 = median - std * m1
        v2_up = up1 - indicator
        v2_down = down1 - indicator
        df[f'bolling_v2_up_max_bh_{n}'] = v2_up.shift(1)
        df[f'bolling_v2_down_max_bh_{n}'] = v2_down.shift(1)
        extra_agg_dict[f'bolling_v2_up_max_bh_{n}'] = 'first'
        extra_agg_dict[f'bolling_v2_down_max_bh_{n}'] = 'first'

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_up_max_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_down_max_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling_v2_up_max', 'bolling_v2_down_max', 'bolling_v2_up_max_diff', 'bolling_v2_down_max_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling_v2_up_max', 'bolling_v2_down_max']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bolling_v2_mean_bak(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling v2  参考J神V2布林
    for n in back_hour_list:
        n1 = n

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
        indicator *= wd_atr * mtm_atr_mean
        indicator = pd.Series(indicator)

        # 对新策略因子计算自适应布林
        median = indicator.rolling(window=n1).mean()
        std = indicator.rolling(n1, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        z_score = abs(indicator - median) / (std + eps)
        m1 = pd.Series(z_score).rolling(window=n1, min_periods=1).mean()
        up1 = median + std * m1
        down1 = median - std * m1
        v2_up = up1 - indicator
        v2_down = down1 - indicator
        df[f'bolling_v2_up_mean_bh_{n}'] = v2_up.shift(1)
        df[f'bolling_v2_down_mean_bh_{n}'] = v2_down.shift(1)
        extra_agg_dict[f'bolling_v2_up_mean_bh_{n}'] = 'first'
        extra_agg_dict[f'bolling_v2_down_mean_bh_{n}'] = 'first'

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_up_mean_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_down_mean_bh_{n}', _agg_dict=extra_agg_dict, _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling_v2_up_mean', 'bolling_v2_down_mean', 'bolling_v2_up_mean_diff', 'bolling_v2_down_mean_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling_v2_up_mean', 'bolling_v2_down_mean']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_bolling_v2_mean(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # bolling v2  参考J神V2布林
    for n in back_hour_list:
        # 计算动量因子
        df['mtm'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_mean'] = df['mtm'].rolling(window=n, min_periods=1).mean()

        # 基于价格atr，计算波动率因子wd_atr
        df['c1'] = df['high'] - df['low']
        df['c2'] = abs(df['high'] - df['close'].shift(1))
        df['c3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=n, min_periods=1).mean()
        df['avgPrice'] = df['close'].rolling(window=n, min_periods=1).mean()
        df['wd_atr'] = df['atr'] / df['avgPrice']  # === 波动率因子

        # 参考ATR，对MTM指标，计算波动率因子
        df['mtm_l'] = df['low'] / df['low'].shift(n) - 1
        df['mtm_h'] = df['high'] / df['high'].shift(n) - 1
        df['mtm_c'] = df['close'] / df['close'].shift(n) - 1
        df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
        df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr'] = df['mtm_tr'].rolling(window=n, min_periods=1).mean()

        # 参考ATR，对MTM mean指标，计算波动率因子

        # 参考ATR，对MTM mean指标，计算波动率因子
        df['mtm_l_mean'] = df['mtm_l'].rolling(window=n, min_periods=1).mean()
        df['mtm_h_mean'] = df['mtm_h'].rolling(window=n, min_periods=1).mean()
        df['mtm_c_mean'] = df['mtm_c'].rolling(window=n, min_periods=1).mean()
        df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
        df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
        df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
        df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n, min_periods=1).mean()

        indicator = 'mtm_mean'
        # mtm_mean指标分别乘以三个波动率因子
        df[indicator] = df[indicator] * df['mtm_atr_mean']
        df[indicator] = df[indicator] * df['wd_atr']
        # 对新策略因子计算自适应布林
        df['median'] = df[indicator].rolling(window=n, min_periods=1).mean()
        df['std'] = df[indicator].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
        df['z_score'] = abs(df[indicator] - df['median']) / (df['std'] + eps)
        df['m'] = df['z_score'].rolling(window=n, min_periods=1).mean()
        df['up'] = df['median'] + df['std'] * df['m']
        df['down'] = df['median'] - df['std'] * df['m']
        df['v2_up'] = df['up'] - df[indicator]
        df['v2_down'] = df['down'] - df[indicator]
        df[f'bolling_v2_up_mean_bh_{n}'] = df['v2_up'].shift(1)
        df[f'bolling_v2_down_mean_bh_{n}'] = df['v2_down'].shift(1)
        extra_agg_dict[f'bolling_v2_up_mean_bh_{n}'] = 'first'
        extra_agg_dict[f'bolling_v2_down_mean_bh_{n}'] = 'first'

        del df['mtm']
        del df['mtm_mean']
        del df['c1']
        del df['c2']
        del df['c3']
        del df['tr']
        del df['atr']
        del df['avgPrice']
        del df['wd_atr']
        del df['mtm_l']
        del df['mtm_h']
        del df['mtm_c']
        del df['mtm_c1']
        del df['mtm_c2']
        del df['mtm_c3']
        del df['mtm_tr']
        del df['mtm_atr']
        del df['mtm_l_mean']
        del df['mtm_h_mean']
        del df['mtm_c_mean']
        del df['mtm_atr_mean']
        del df['median']
        del df['std']
        del df['z_score']
        del df['m']
        del df['up']
        del df['down']
        del df['v2_up']
        del df['v2_down']

        if add:
            # 差分
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_up_mean_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
            add_diff(_df=df, _d_list=diff_d, _name=f'bolling_v2_down_mean_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
    if add:
        return df, extra_agg_dict, ['bolling_v2_up_mean', 'bolling_v2_down_mean', 'bolling_v2_up_mean_diff',
                                    'bolling_v2_down_mean_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['bolling_v2_up_mean', 'bolling_v2_down_mean']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_lcsd(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    # 计算lcsd代码---丁老板自创
    for n in back_hour_list:
        df['ma'] = df['close'].rolling(n, min_periods=1).mean()
        df[f'lcsd_bh_{n}'] = (df['low'] - df['ma']) / df['low']
        df[f'lcsd_bh_{n}'] = df[f'lcsd_bh_{n}'].shift(1)
        extra_agg_dict[f'lcsd_bh_{n}'] = 'first'
        # 删除中间过程数据
        del df['ma']

        if add:
            add_diff(_df=df, _d_list=diff_d, _name=f'lcsd_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
    if add:
        return df, extra_agg_dict, ['lcsd', 'lcsd_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['lcsd']  # 由于有些时候返回计算的因子有多个，所以使用列表

def signal_factor_market_profit_loss(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):

   # 市场盈亏---  源自 松
    for n in back_hour_list:
        quote_volume_ema = df['quote_volume'].ewm(span=n, adjust=False).mean()
        volume_ema = df['volume'].ewm(span=n, adjust=False).mean()
        df[f'平均持仓成本_bh_{n}'] = quote_volume_ema / volume_ema
        df[f'market_profit_loss_bh_{n}'] = df['close'] / df[f'平均持仓成本_bh_{n}'] - 1
        df[f'market_profit_loss_bh_{n}'] = df[f'market_profit_loss_bh_{n}'].shift(1)
        extra_agg_dict[f'market_profit_loss_bh_{n}'] = 'first'
        del df[f'平均持仓成本_bh_{n}']

        if add:
            add_diff(_df=df, _d_list=diff_d, _name=f'market_profit_loss_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
    if add:
        return df, extra_agg_dict, ['market_profit_loss', 'market_profit_loss_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['market_profit_loss']  # 由于有些时候返回计算的因子有多个，所以使用列表


def signal_factor_zhenfu3(df, extra_agg_dict={}, back_hour_list=[2, 4, 6, 10, 12, 24], add=True):
    """
    振幅比率 abs(close-open) / (high-low)
    :param df: 
    :param extra_agg_dict: 
    :param back_hour_list: 
    :param add: 
    :return: 
    """
    for n in back_hour_list:
        df['zhenfu3'] = abs(df['close'] - df['open']) / (df['high'] - df['low'])
        df[f'zhenfu3_bh_{n}'] = df['zhenfu3'].shift(1)
        extra_agg_dict[f'zhenfu3_bh_{n}'] = 'first'

        if add:
            add_diff(_df=df, _d_list=diff_d, _name=f'zhenfu3_bh_{n}', _agg_dict=extra_agg_dict,
                     _agg_type='first')
    if add:
        return df, extra_agg_dict, ['zhenfu3', 'zhenfu3_diff']  # 由于有些时候返回计算的因子有多个，所以使用列表
    else:
        return df, extra_agg_dict, ['zhenfu3']  # 由于有些时候返回计算的因子有多个，所以使用列表
