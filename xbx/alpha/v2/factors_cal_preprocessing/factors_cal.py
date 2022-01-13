import pandas as pd
import numpy as np
import statsmodels.api as sm
import scipy.stats as st
import talib # talib版本 0.4.19
import math
from sklearn.linear_model import LinearRegression #
# from fracdiff import fdiff  # https://github.com/simaki/fracdiff  pip install fracdiff

eps = 1e-8
# eps = 0

def signal_factor_K(df,back_hour_list=[2, 4, 6, 10, 12, 24]):
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
        
def signal_factor_D(df,back_hour_list=[2, 4, 6, 10, 12, 24]):
    # KDJ 指标
    for n in back_hour_list:
        low_list = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N) 求周期内low的最小值
        high_list = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N) 求周期内high 的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list + eps) * 100 # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100 计算一个随机值
        # K D J的值在固定的范围内
        df[f'K_bh_{n}'] = rsv.ewm(com=2).mean() # K=SMA(Stochastics,3,1) 计算k
        df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D=SMA(K,3,1)  计算D
        
def signal_factor_J(df,back_hour_list=[2, 4, 6, 10, 12, 24]):
    # KDJ 指标
    for n in back_hour_list:
        low_list = df['low'].rolling(n, min_periods=1).min() # MIN(LOW,N) 求周期内low的最小值
        high_list = df['high'].rolling(n, min_periods=1).max() # MAX(HIGH,N) 求周期内high 的最大值
        rsv = (df['close'] - low_list) / (high_list - low_list + eps) * 100 # Stochastics=(CLOSE-LOW_N)/(HIGH_N-LOW_N)*100 计算一个随机值
        # K D J的值在固定的范围内
        df[f'K_bh_{n}'] = rsv.ewm(com=2).mean() # K=SMA(Stochastics,3,1) 计算k
        df[f'D_bh_{n}'] = df[f'K_bh_{n}'].ewm(com=2).mean()  # D=SMA(K,3,1)  计算D
        df[f'J_bh_{n}'] = 3 * df[f'K_bh_{n}'] - 2 * df[f'D_bh_{n}'] # 计算J

def signal_factor_RSI(df,back_hour_list=[2, 4, 6, 10, 12, 24]):
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
        
def signal_factor_avg_price(df,back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 均价
    for n in back_hour_list:
        df['price'] = df['quote_volume'].rolling(n, min_periods=1).sum() / df['volume'].rolling(n, min_periods=1).sum()

        df[f'avg_price_bh_{n}'] = (df['price'] - df['price'].rolling(n, min_periods=1).min()) / (
                    df['price'].rolling(n, min_periods=1).max() - df['price'].rolling(n, min_periods=1).min() + eps)

def signal_factor_zhang_die_fu (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 涨跌幅
    for n in back_hour_list:
        df[f'zhang_die_fu_bh_{n}'] = df['close'].pct_change(n)
    

def signal_factor_bias (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # bias # 涨跌幅更好的表达方式：bias，币价偏离均线的比例。
    for n in back_hour_list:
        ma = df['close'].rolling(n, min_periods=1).mean()
        df[f'bias_bh_{n}'] = df['close'] / ma - 1
        
def signal_factor_zhenfu (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 振幅：最高价最低价
    for n in back_hour_list:
        high = df['high'].rolling(n, min_periods=1).max()
        low = df['low'].rolling(n, min_periods=1).min()
        df[f'zhenfu_bh_{n}'] = high / low - 1
        

def signal_factor_zhenfu2 (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 振幅：收盘价、开盘价
    high = df[['close', 'open']].max(axis=1)
    low = df[['close', 'open']].min(axis=1)
    for n in back_hour_list:
        high = high.rolling(n, min_periods=1).max()
        low = low.rolling(n, min_periods=1).min()
        df[f'zhenfu2_bh_{n}'] = high / low - 1
        

def signal_factor_zhang_die_fu_std (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 涨跌幅std，振幅的另外一种形式
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'zhang_die_fu_std_bh_{n}'] = change.rolling(n).std()
        

def signal_factor_zhang_die_fu_skew (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 涨跌幅偏度：在商品期货市场有效
    change = df['close'].pct_change()
    for n in back_hour_list:
        df[f'zhang_die_fu_skew_bh_{n}'] = change.rolling(n).skew()

def signal_factor_volume (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 成交额：对应小市值概念
    for n in back_hour_list:
        df[f'volume_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=1).sum()

def signal_factor_volume_std (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 成交额：对应小市值概念
    for n in back_hour_list:
        df[f'volume_std_bh_{n}'] = df['quote_volume'].rolling(n, min_periods=2).std()


def signal_factor_taker_buy_ratio (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 资金流入比例，币安独有的数据
    for n in back_hour_list:
        volume = df['quote_volume'].rolling(n, min_periods=1).sum()
        buy_volume = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum()
        df[f'taker_buy_ratio_bh_{n}'] = buy_volume / volume

def signal_factor_quote_volume_ratio (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 量比
    for n in back_hour_list:
        df[f'quote_volume_ratio_bh_{n}'] = df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()
        
def signal_factor_trade_num (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 成交笔数
    for n in back_hour_list:
        df[f'trade_num_bh_{n}'] = df['trade_num'].rolling(n, min_periods=1).sum()
        
def signal_factor_quanlity_price_corr (df, back_hour_list=[2, 4, 6, 10, 12, 24]):    # 量价相关系数：量价相关选股策略
    for n in back_hour_list:
        df[f'quanlity_price_corr_bh_{n}'] = df['close'].rolling(n).corr(df['quote_volume'].rolling(n))

def signal_factor_PSY (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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
        df[f'PSY_bh_{n}'] = df['PSY'] 

def signal_factor_CMO (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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

        df[f'CMO_bh_{n}'] = df['CMO'] 
        
def signal_factor_TRIX (df, back_hour_list=[2, 4, 6, 10, 12, 24]):

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

        df[f'TRIX_bh_{n}'] = df['TRIX'] 
        
def signal_factor_REG (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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

        df['reg_close'] = talib.LINEARREG(df['close'], timeperiod=n) # 该部分为talib内置求线性回归
        df['reg'] = df['close'] / df['reg_close'] - 1

        # sklearn 线性回归
#         def reg_ols(_y):
#             _x = np.arange(n) + 1
#             model = LinearRegression().fit(_x.reshape(-1, 1), _y)  # 线性回归训练
#             y_pred = model.coef_ * _x + model.intercept_  # y = ax + b
#             return y_pred[-1]

#         df['reg_close'] = df['close'].rolling(n).apply(lambda y: reg_ols(y)) # 求数据拟合的线性回归
#         df['reg'] = df['close'] / (df['reg_close'] + eps) - 1

        df[f'REG_bh_{n}'] = df['reg']
    
def signal_factor_cci (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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

        df[f'cci_bh_{n}'] = (df['tp'] - df['ma']) / (eps + df['md']) # CCI=(TP-MA)/(0.015MD)  CCI在一定范围内
    
def signal_factor_vwap_bias (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # bias因子以均价表示
    for n in back_hour_list:
        """
        将bias 的close替换成vwap
        """
        df['vwap'] = df['volume'] / df['quote_volume']  # 在周期内成交额除以成交量等于成交均价
        ma = df['vwap'].rolling(n, min_periods=1).mean() # 求移动平均线
        df[f'vwap_bias_bh_{n}'] = df['vwap'] / (ma + eps) - 1  # 去量纲
        
def signal_factor_ADM (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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
        df[f'ADM_bh_{n}'] = ADTM 
        
def signal_factor_POS (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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
        df[f'POS_bh_{n}'] = pos 
        
def signal_factor_STC (df, back_hour_list=[2, 4, 6, 10, 12, 24]):

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
        df[f'STC_bh_{n}'] = df['STC']
        
def signal_factor_ER_bull (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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
        
def signal_factor_ER_bear (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
    # 计算 ER
    for n in back_hour_list:
     
        ema = df['close'].ewm(n, adjust=False).mean() # EMA(CLOSE,N)
        bull_power = df['high'] - ema  # 越高表示上涨 牛市 BullPower=HIGH-EMA(CLOSE,N)
        bear_power = df['low'] - ema  # 越低表示下降越厉害  熊市 BearPower=LOW-EMA(CLOSE,N)
        df[f'ER_bear_bh_{n}'] = bear_power / (ema + eps)  # 去量纲
        

def signal_factor_RCCD (df, back_hour_list=[2, 4, 6, 10, 12, 24]):
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

        df[f'RCCD_bh_{n}'] = df['RCCD']
        
def signal_factor_PMO (df, back_hour_list=[2, 4, 6, 10, 12, 24]):

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

        df[f'PMO_bh_{n}'] = df['PMO_SIGNAL']
        
def signal_factor_VRAMT (df, back_hour_list=[2, 4, 6, 10, 12, 24]):

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
        df[f'VRAMT_bh_{n}'] = df['VRAMT']