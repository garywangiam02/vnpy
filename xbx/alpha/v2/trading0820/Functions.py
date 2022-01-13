"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import ccxt
# import pandas as pd
import pandas as pd
import numpy as np
import time
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
from Signals import *
from Config import *
from Utility import robust,send_dingding_msg
from functools import partial
import talib as ta
from fracdiff import fdiff
import os
import configparser
config = configparser.ConfigParser()

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数
pd.set_option('use_inf_as_na', True)


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
    # kline = exchange.fapiPublic_get_klines({'symbol': symbol, 'interval': '1h', 'limit': limit})
    kline = robust(exchange.fapiPublic_get_klines,{'symbol': symbol, 'interval': '1h', 'limit': limit})

    # 将数据转换为DataFrame
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(kline, columns=columns, dtype='float')

    # 整理数据
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
    df['symbol'] = symbol  # 添加symbol列
    columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    df = df[columns]

    # 删除runtime那行的数据，如果有的话
    df = df[df['candle_begin_time'] != run_time]

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
    config.read('config.ini')
    debug   = int(config['default']['debug'])

    if debug == 1:    
        # 调试模式下，循环获取数据
        result = []
        for arg in arg_list:
            (exchange, symbol, run_time) = arg
            res = fetch_binance_swap_candle_data(exchange, symbol, run_time)
            result.append(res)

    else:
        # 多进程获取数据
        with Pool(processes=20) as pl:
            # 利用starmap启用多进程信息
            result = pl.starmap(fetch_binance_swap_candle_data, arg_list)

    df = dict(result)

    for symbol in symbol_list:
        if len(df[symbol]) < 100:
            print('no enough data', symbol, f'{len(df[symbol])}')
            del df[symbol]

    print('获取所有币种K线数据完成，花费时间：', time.time() - s_time, '\n')
    return df


# 获取币安的ticker数据
def fetch_binance_ticker_data(binance):
    """
    使用ccxt的接口fapiPublic_get_ticker_24hr()获取ticker数据
                       priceChange  priceChangePercent  weightedAvgPrice     lastPrice    lastQty  ...      openTime     closeTime      firstId       lastId      count
    symbol                                                                                 ...
    BTCUSDT     377.720000               3.517      10964.340000  11118.710000      0.039  ...  1.595927e+12  1.596013e+12  169966030.0  171208339.0  1242251.0
    ETHUSDT       9.840000               3.131        316.970000    324.140000      4.380  ...  1.595927e+12  1.596013e+12   72997450.0   73586755.0   589302.0
    ...
    XLMUSDT       0.002720               2.838          0.096520      0.098570    203.000  ...  1.595927e+12  1.596013e+12   12193167.0   12314848.0   121682.0
    ADAUSDT       0.002610               1.863          0.143840      0.142680   1056.000  ...  1.595927e+12  1.596013e+12   17919791.0   18260724.0   340914.0
    XMRUSDT       2.420000               3.013         81.780000     82.740000      0.797  ...  1.595927e+12  1.596013e+12    4974234.0    5029877.0    55644.0
    :param binance:
    :return:
    """
    tickers = binance.fapiPublic_get_ticker_24hr()
    tickers = pd.DataFrame(tickers, dtype=float)
    tickers.set_index('symbol', inplace=True)

    return tickers['lastPrice']


# =====获取持仓
# 获取币安账户的实际持仓
# @robust
def update_symbol_info(exchange, symbol_list):
    """
    使用ccxt接口：fapiPrivate_get_positionrisk，获取账户持仓
    返回值案例
                   positionAmt  entryPrice  markPrice  unRealizedProfit  liquidationPrice  ...  maxNotionalValue  marginType isolatedMargin  isAutoAddMargin
    positionSide
    symbol                                                                            ...
    XMRUSDT         0.003    63.86333  63.877630          0.000043             0.000  ...            250000       cross            0.0            false         LONG
    ATOMUSDT       -0.030     2.61000   2.600252          0.000292           447.424  ...             25000       cross            0.0            false        SHORT
    :param exchange:
    :param symbol_list:
    :return:
    """
    # 获取原始数据
    position_risk = robust(exchange.fapiPrivate_get_positionrisk)

    # 将原始数据转化为dataframe
    position_risk = pd.DataFrame(position_risk, dtype='float')

    # 整理数据
    position_risk.rename(columns={'positionAmt': '当前持仓量'}, inplace=True)
    position_risk = position_risk[position_risk['当前持仓量'] != 0]  # 只保留有仓位的币种
    position_risk.set_index('symbol', inplace=True)  # 将symbol设置为index

    # 创建symbol_info
    symbol_info = pd.DataFrame(index=symbol_list, columns=['当前持仓量'])
    symbol_info['当前持仓量'] = position_risk['当前持仓量']
    symbol_info['当前持仓量'].fillna(value=0, inplace=True)

    return symbol_info



# =====策略相关函数
# 选币数据整理 & 选币 Lasso 版
# 横截面因子参数组合, 载入训练好的数据
import pickle
def load_pkl(name):
    with open(name, 'rb') as f:
        return pickle.load(f)

regr_coef = pd.read_pickle('./regr_coef.pkl') # 因子的系数
factors = regr_coef.index # 用到的因子

train_mean = load_pkl('./train_mean.pkl') # 因子的均值
train_std = load_pkl('./train_std.pkl') # 因子的方差


def cal_factor_and_select_coin_lasso(stratagy_list, symbol_candle_data, run_time):
    '''
    :param stratagy_list:
    :param symbol_candle_data:
    :param run_time:
    :return:
    '''
    s_time = time.time()
    config.read('config.ini')

    # ===逐个遍历每一个策略
    select_coin_list = []
    for strategy in stratagy_list:
        # 获取策略参数
        c_factor = strategy['c_factor']
        hold_period = strategy['hold_period']
        selected_coin_num = strategy['selected_coin_num']
        # factors = strategy['factors']

        # ===逐个遍历每一个币种，计算其因子，并且转化周期
        # period_df_list = []

        symbol_list = symbol_candle_data.keys()
        print('symbol #', len(symbol_list))
        print('Calculating factors...')

        # for factor_dict in factors:
        # factor = strategy['factor']
        # para = strategy['para']
        if_reverse = strategy['if_reverse']

        f = partial(deal_file_name, symbol_candle_data, hold_period, run_time, c_factor)

        with Pool(processes=os.cpu_count(), maxtasksperchild=1) as pl:
            all_coin_data_list = pl.map(f, symbol_list)

        # ===将不同的币种数据合并到一张表，并且存储
        df = pd.concat(all_coin_data_list)
        df = df.sort_values(['s_time', 'symbol'])

        # 填充因子中 NA 值, 用因子的均值填充
        print('test null rows:', df.isna().any(axis=1).sum())
        for f in factors:
            df[f].fillna(train_mean[f], inplace=True)
        print('test null rows(after fillna):', df.isna().any(axis=1).sum())

        # 计算因子导入
        test_factors = (df[factors] - train_mean) / train_std
        df['linregr'] = test_factors.dot(regr_coef)

        # 用于保存数据核对实盘框架
        # print(df[['symbol', 'close', 's_time', 'e_time', 'offset', 'linregr', 'RSI_bh_4']].tail(20))
        # print(df.columns)
        # df.to_csv('./trade_btc-usdt.csv')
        # exit()

        if if_reverse:
            df['因子'] = -df['linregr']
        else:
            df['因子'] = df['linregr']

        # ===选币数据整理完成，接下来开始选币
        # 多空双向rank
        df['币总数'] = df.groupby(df.index).size()
        df['rank'] = df.groupby('s_time')['因子'].rank(method='first')

        # 删除不要的币
        df['方向'] = 0
        # 做多的币
        df.loc[(df['rank'] <= selected_coin_num), '方向'] = 1
        # 做空的币
        df.loc[((df['币总数'] - df['rank']) < selected_coin_num), '方向'] = -1
        df = df[df['方向'] != 0]

        # ===将每个币种的数据保存到dict中
        # 删除不需要的列
        # df.drop([factor, '币总数', 'rank'], axis=1, inplace=True)
        # df.drop(['币总数', 'rank'], axis=1, inplace=True)
        df.reset_index(inplace=True)
        select_coin_list.append(df)

    select_coin = pd.concat(select_coin_list)
    print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)

    debug   = int(config['default']['debug'])
    if debug == 1:
        # 调试模式下，保存选币信息
        print(select_coin[['symbol', 's_time', 'e_time', 'offset', 'linregr', '因子', '币总数', 'rank', '方向']])
        select_coin.to_csv('select_coin.csv')

    return select_coin


# =====策略相关函数
# 选币数据整理 & 选币
def cal_factor_and_select_coin(stratagy_list, symbol_candle_data, run_time):
    """
    :param stratagy_list:
    :param symbol_candle_data:
    :param run_time:
    :return:
    """
    s_time = time.time()
    config.read('config.ini')

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
        print(_symbol_list)
        print('symbol #', len(_symbol_list))

        symbol_list = [symbol for symbol in _symbol_list if 'USDT' in symbol]
        print(symbol_list)
        print('symbol #', len(symbol_list))

        for symbol in symbol_list:
            if symbol in no_enough_data_symbol:
                continue

            # =获取相应币种1h的k线，深度拷贝
            df = symbol_candle_data[symbol].copy()

            # =空数据
            if df.empty:
                print('no data',symbol)
                if symbol not in no_enough_data_symbol:
                    no_enough_data_symbol.append(symbol)                
                continue
                
            if len(df) < 100:
                print('no enough data',symbol)
                if symbol not in no_enough_data_symbol:
                    no_enough_data_symbol.append(symbol)
                continue

            df[c_factor] = 0

            for factor_dict in factors:
                factor          = factor_dict['factor']
                para            = factor_dict['para']
                if_reverse      = factor_dict['if_reverse']
    
                df = eval(f'signal_{factor}')(df, int(para))  # 计算信号

                # 初始化
                df[factor + '_因子'] = np.nan 

                # =空计算
                if np.isnan(df.iloc[-1][factor]):
                    continue

                if if_reverse:              
                    df[factor + '_因子'] = - df[factor]  
                else:
                    df[factor + '_因子'] = df[factor]  


            # =将数据转化为需要的周期
            df['s_time'] = df['candle_begin_time']
            df['e_time'] = df['candle_begin_time']
            df.set_index('candle_begin_time', inplace=True)

            agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'close': 'last', c_factor: 'last'}

            for factor_dict in factors:
                factor          = factor_dict['factor']
                agg_dict[factor + '_因子'] = 'last'

            # 转换生成每个策略所有offset的因子
            for offset in range(int(hold_period[:-1])):
                # 转换周期
                period_df = df.resample(hold_period, base=offset).agg(agg_dict)
                period_df['offset'] = offset
                # 保存策略信息到结果当中
                period_df['key'] = f'{c_factor}_{hold_period}_{offset}H'  # 创建主键值

                # 截取指定周期的数据
                period_df = period_df[
                    (period_df['s_time'] <= run_time - timedelta(hours=int(hold_period[:-1]))) &
                    (period_df['s_time'] > run_time - 2 * timedelta(hours=int(hold_period[:-1])))
                ]
                # 合并数据
                period_df_list.append(period_df)

        print("period_df_list", period_df_list[:10])

        # ===将不同offset的数据，合并到一张表
        df = pd.concat(period_df_list)
        df = df.sort_values(['s_time', 'symbol'])

        df[c_factor] = 0

        for factor_dict in factors:
            factor          = factor_dict['factor']
            weight          = factor_dict['weight']
            df[factor + '_排名'] = df.groupby('s_time')[factor + '_因子'].rank()
            df[c_factor] += df[factor + '_排名']*weight


        # ===选币数据整理完成，接下来开始选币
        # 多空双向rank
        df['币总数'] = df.groupby(df.index).size()
        df['rank'] = df.groupby('s_time')[c_factor].rank(method='first')

        # 删除不要的币
        df['方向'] = 0
        df.loc[(df['rank'] <= selected_coin_num), '方向'] = 1
        df.loc[((df['币总数'] - df['rank']) < selected_coin_num), '方向'] = -1
        df = df[df['方向'] != 0]

        # print(df)
        '''
                      symbol    s_time              e_time                close      c001   bias_因子  cci_因子    offset  key         bias_排名  cci_排名  币总数  rank  方向
candle_begin_time                                                                                                                                                    
2021-08-05 01:00:00     BELUSDT 2021-08-05 01:00:00 2021-08-05 06:00:00     1.81890  134.2  0.003725  -22.632367       1  c001_6H_1H    100.0   114.0  117  117.0  -1
2021-08-05 01:00:00    RUNEUSDT 2021-08-05 01:00:00 2021-08-05 06:00:00     7.98000   15.4 -0.009003 -139.022187       1  c001_6H_1H     10.0    18.0  117    1.0   1
2021-08-05 02:00:00    RUNEUSDT 2021-08-05 02:00:00 2021-08-05 07:00:00     7.90700   11.8 -0.014765 -109.194221       2  c001_6H_2H      1.0    36.0  117    1.0   1
2021-08-05 02:00:00     RVNUSDT 2021-08-05 02:00:00 2021-08-05 07:00:00     0.07385  139.6  0.010502  -34.538231       2  c001_6H_2H    109.0   102.0  117  117.0  -1
2021-08-05 03:00:00  BTCDOMUSDT 2021-08-05 03:00:00 2021-08-05 08:00:00  1062.80000  141.1  0.001413  102.626211       3  c001_6H_3H    106.0   117.0  117  117.0  -1
2021-08-05 03:00:00     OGNUSDT 2021-08-05 03:00:00 2021-08-05 08:00:00     0.79760   11.6 -0.026219  -95.881899       3  c001_6H_3H      2.0    32.0  117    1.0   1
2021-08-05 04:00:00     DGBUSDT 2021-08-05 04:00:00 2021-08-05 09:00:00     0.05129   18.4 -0.009415  -76.040147       4  c001_6H_4H     10.0    28.0  117    1.0   1
2021-08-05 04:00:00    LUNAUSDT 2021-08-05 04:00:00 2021-08-05 09:00:00    14.69100  135.3  0.011603  -11.638536       4  c001_6H_4H    108.0    91.0  117  117.0  -1
2021-08-05 05:00:00    BAKEUSDT 2021-08-05 05:00:00 2021-08-05 10:00:00     2.02940   15.8 -0.008113  -76.513137       5  c001_6H_5H      5.0    36.0  117    1.0   1
2021-08-05 05:00:00    LUNAUSDT 2021-08-05 05:00:00 2021-08-05 10:00:00    14.88700  139.1  0.019431  -26.094267       5  c001_6H_5H    113.0    87.0  117  117.0  -1
2021-08-05 06:00:00    LUNAUSDT 2021-08-05 06:00:00 2021-08-05 11:00:00    15.02000  140.7  0.019878  -31.771689       0  c001_6H_0H    114.0    89.0  117  117.0  -1
2021-08-05 06:00:00   OCEANUSDT 2021-08-05 06:00:00 2021-08-05 11:00:00     0.59448    6.4 -0.007036 -109.992568       0  c001_6H_0H      1.0    18.0  117    1.0   1

        '''

        # ===将每个币种的数据保存到dict中
        # 删除不需要的列
        # df.drop([factor, '币总数', 'rank'], axis=1, inplace=True)
        df.drop(['币总数', 'rank'], axis=1, inplace=True)
        df.reset_index(inplace=True)
        select_coin_list.append(df)

    select_coin = pd.concat(select_coin_list)
    print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)
    print(select_coin)

    debug   = int(config['default']['debug'])
    if debug == 1:
        # 调试模式下，保存选币信息
        print(select_coin)
        select_coin.to_csv('select_coin.csv')
    return select_coin


# 计算每个策略分配的资金
def cal_strategy_trade_usdt(stratagy_list, trade_usdt):
    """
    计算每个策略分配的资金
    """
    df = pd.DataFrame()
    # 策略的个数
    strategy_num = len(stratagy_list)
    # 遍历策略
    for strategy in stratagy_list:
        c_factor = strategy['c_factor']
        hold_period = strategy['hold_period']
        selected_coin_num = strategy['selected_coin_num']

        offset_num = int(hold_period[:-1])
        for offset in range(offset_num):
            df.loc[
                f'{c_factor}_{hold_period}_{offset}H', '策略分配资金'] = trade_usdt / strategy_num / 2 / offset_num \
                                                                        / selected_coin_num

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'key'}, inplace=True)

    return df


# 计算实际下单量
def cal_order_amount(symbol_info, select_coin, strategy_trade_usdt):
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left')
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']

    # 对下单量进行汇总
    symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum()
    symbol_info['目标下单量'].fillna(value=0, inplace=True)
    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']

    # 历史回溯
    print(select_coin[['key', 's_time', 'symbol', '方向', '策略分配资金', 'close']], '\n')

    # 删除实际下单量为0的币种
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    return symbol_info

# 下单
# @robust
def place_order(symbol_info, symbol_last_price, min_qty , price_precision):

    for symbol, row in symbol_info.dropna(subset=['实际下单量']).iterrows():

        if symbol not in min_qty:
            continue

        # 计算下单量：按照最小下单量向下取整
        quantity = row['实际下单量']
        quantity = float(f'{quantity:.{min_qty[symbol]}f}')
        reduce_only = np.isnan(row['目标下单份数']) or row['目标下单量'] * quantity < 0

        quantity = abs(quantity)  # 下单量取正数
        if quantity == 0:
            print(symbol, quantity, '实际下单量为0，不下单')
            continue

        # 计算下单方向、价格
        if row['实际下单量'] > 0:
            side = 'BUY'
            price = symbol_last_price[symbol] * 1.02
        else:
            side = 'SELL'
            price = symbol_last_price[symbol] * 0.98

        # 对下单价格这种最小下单精度
        price = float(f'{price:.{price_precision[symbol]}f}')

        if symbol not in price_precision:
            continue

        if (quantity * price < 5) and not reduce_only:
            print('quantity * price < 5')
            quantity = 0
            continue


        # 下单参数
        params = {'symbol': symbol, 'side': side, 'type': 'LIMIT', 'price': price, 'quantity': quantity,
                  'clientOrderId': str(time.time()), 'timeInForce': 'GTC', 'reduceOnly': reduce_only}
        # 下单
        print('下单参数：', params)

        open_order = robust(exchange.fapiPrivate_post_order,params)
        print('下单完成，下单信息：', open_order, '\n')
        

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
    return run_time
