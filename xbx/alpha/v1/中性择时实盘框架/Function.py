"""
《邢不行-2020新版|Python数字货币量化投资课程》
无需编程基础，助教答疑服务，专属策略网站，一旦加入，永续更新。
课程详细介绍：https://quantclass.cn/crypto/class
邢不行微信: xbx9025
本程序作者: 邢不行

# 课程内容
币安u本位择时策略实盘框架相关函数
"""
import ccxt
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import hmac
import hashlib
import base64
from urllib import parse
from Config import *
import Signals
import requests
import json
import os

import configparser
config = configparser.ConfigParser()
config.read('config.ini')
# debug模式
debug = config['default']['debug']
# 获取本程序标识，以便发送报错时知道是由哪个程序发出的
name = config['default']['name']



# ==========辅助功能函数==========
# ===下次运行时间，和课程里面讲的函数是一样的
def next_run_time(time_interval, ahead_seconds=5):
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
    5m  当前时间为：12:34:51  返回时间为：12:35:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00
    2h  当前时间为：00:37:51  返回时间为：02:00:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00
    5m  当前时间为：21日的23:57:51  返回时间为：22日的00:00:00

    ahead_seconds = 5
    15m  当前时间为：12:59:57  返回时间为：13:15:00，而不是 13:00:00
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
    # now_time = datetime(2019, 5, 9, 23, 50, 30)  # 指定now_time，可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)

    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break

    print('\n程序下次运行的时间：', target_time, '\n')
    return target_time


# ===依据时间间隔, 自动计算并休眠到指定时间
def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param time_interval:
    :param ahead_time:
    :param if_sleep:
    :return:
    """

    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time)

    # sleep
    if if_sleep:
        time.sleep(max(0, (run_time - datetime.now()).seconds))
        # 可以考察：print(run_time - n)、print((run_time - n).seconds)
        while True:  # 在靠近目标时间时
            if datetime.now() > run_time:
                break

    return run_time


# ===将最新数据和历史数据合并
def symbol_candle_data_append_recent_candle_data(symbol_candle_data, recent_candle_data, symbol_list, max_candle_num):

    for symbol in symbol_list:
        df = symbol_candle_data[symbol].append(recent_candle_data[symbol], ignore_index=True)
        df.drop_duplicates(subset=['candle_begin_time_GMT8'], keep='last', inplace=True)
        df.sort_values(by='candle_begin_time_GMT8', inplace=True)  # 排序，理论上这步应该可以省略，加快速度
        df = df.iloc[-max_candle_num:]  # 保持最大K线数量不会超过max_candle_num个
        df.reset_index(drop=True, inplace=True)
        symbol_candle_data[symbol] = df

    return symbol_candle_data


# ===重试机制
def retry_wrapper(func, params={}, act_name='', sleep_seconds=3, retry_times=5):
    """
    需要在出错时不断重试的函数，例如和交易所交互，可以使用本函数调用。
    :param func: 需要重试的函数名
    :param params: func的参数
    :param act_name: 本次动作的名称
    :param sleep_seconds: 报错后的sleep时间
    :param retry_times: 为最大的出错重试次数
    :return:
    """

    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result
        except Exception as e:
            print(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            time.sleep(sleep_seconds)
    else:
        # send_dingding_and_raise_error(output_info)
        output_info = act_name + '报错重试次数超过上限，程序退出。\n'
        send_wx_error_msg(output_info)
        send_dingding_error_msg(output_info)
        raise ValueError(output_info)


# ==========交易所交互函数==========
# ===判断当前持仓模式
def if_oneway_mode(exchange):
    """
    判断当前合约持仓模式。必须得是单向模式。如果是双向模式，就报错。
    查询当前的持仓模式。使用函数：GET /fapi/v1/positionSide/dual (HMAC SHA256)
    判断持仓情况，False为单向持仓，True为单向持仓
    :param exchange:
    :return:
    """

    positionSide = retry_wrapper(exchange.fapiPrivateGetPositionSideDual, act_name='查看合约持仓模式')

    if positionSide['dualSidePosition']:
        raise ValueError("当前持仓模式为双向持仓，程序已停止运行。请去币安官网改为单向持仓。")
    else:
        print('当前持仓模式：单向持仓')


# ===获得币对精度
def usdt_future_exchange_info(exchange, symbol_config_df):
    """
    获取symbol_config中币种的最小下单价格、数量
    :param exchange:
    :return:
    使用接口：GET /fapi/v1/exchangeInfo
    文档：https://binance-docs.github.io/apidocs/futures/cn/#0f3f2d5ee7
    """

    # 获取u本为合约交易对的信息
    exchange_info = retry_wrapper(exchange.fapiPublic_get_exchangeinfo, act_name='查看合约基本信息')

    # 转化为dataframe
    df = pd.DataFrame(exchange_info['symbols'])
    # df['minPrice'] = df['filters'].apply(lambda x: x[0]['minPrice'])
    # df['minQty'] = df['filters'].apply(lambda x: x[1]['minQty'])
    df['tickSize'] = df['filters'].apply(lambda x: math.log(1/float(x[0]['tickSize']), 10))
    df['stepSize'] = df['filters'].apply(lambda x: math.log(1/float(x[1]['stepSize']), 10))
    df = df[['symbol', 'pricePrecision', 'quantityPrecision', 'tickSize', 'stepSize']]
    df.set_index('symbol', inplace=True)

    # 赋值
    symbol_config_df['symbol_1'] = symbol_config_df['symbol'].apply(lambda x: x.split('-')[0] + 'USDT')
    symbol_config_df['symbol_2'] = symbol_config_df['symbol'].apply(lambda x: x.split('-')[1] + 'USDT')

    symbol_config_df['最小下单价精度_1'] = None
    symbol_config_df['最小下单价精度_2'] = None
    symbol_config_df['最小下单量精度_1'] = None
    symbol_config_df['最小下单量精度_2'] = None



    for i, row in symbol_config_df.iterrows():
        # ETH-BTC -> ETHUSDT BTCUSDT
        symbol_1 = row['symbol_1']
        symbol_2 = row['symbol_2']

        symbol_config_df['最小下单价精度_1'].iat[i] = round(df.at[symbol_1, 'tickSize'])
        symbol_config_df['最小下单价精度_2'].iat[i] = round(df.at[symbol_2, 'tickSize'])

        p_1 = df.at[symbol_1, 'quantityPrecision']
        p_2 = df.at[symbol_2, 'quantityPrecision']
        symbol_config_df['最小下单量精度_1'].iat[i] = None if p_1 == 0 else round(p_1)
        symbol_config_df['最小下单量精度_2'].iat[i] = None if p_2 == 0 else round(p_2)

    return symbol_config_df


# ===获取当前持仓信息
def binance_update_account(exchange, symbol_list, symbol_info):
    """
    获取u本位账户的持仓信息、账户余额信息
    :param exchange:
    :param symbol_config:
    :param symbol_info:
    :return:
    接口：GET /fapi/v2/account (HMAC SHA256)
    文档：https://binance-docs.github.io/apidocs/futures/cn/#v2-user_data-2
    币安的币本位合约，不管是交割，还是永续，共享一个账户。他们的symbol不一样。比如btc的永续合约是BTCUSDT，季度合约是BTCUSDT_210625
    """
    # ===获取持仓数据===
    # 获取账户信息
    # account_info = exchange.fapiPrivateGetAccount()
    account_info = retry_wrapper(exchange.fapiPrivateGetAccount, act_name='查看合约账户信息')

    # 将持仓信息转变成dataframe格式
    positions_df = pd.DataFrame(account_info['positions'], dtype=float)
    positions_df = positions_df.set_index('symbol')
    # 筛选交易的币对
    positions_df = positions_df[positions_df.index.isin(symbol_list)]
    # 将账户信息转变成dataframe格式
    assets_df = pd.DataFrame(account_info['assets'], dtype=float)
    assets_df = assets_df.set_index('asset')

    # 根据持仓信息、账户信息中的值填充symbol_info
    balance = assets_df.loc['USDT', 'marginBalance']  # 保证金余额
    symbol_info['账户权益'] = balance

    symbol_info['持仓量'] = positions_df['positionAmt']
    symbol_info['持仓方向'] = symbol_info['持仓量'].apply(lambda x: 1 if float(x) > 0 else (-1 if float(x) < 0 else 0))

    symbol_info['持仓收益'] = positions_df['unrealizedProfit']
    symbol_info['持仓均价'] = positions_df['entryPrice']

    symbol_info['持仓金额'] = symbol_info['持仓均价'] * symbol_info['持仓量']

    # 原框架字段，暂时用不上
    # 计算每个币种的分配资金（在无平仓的情况下）
    # profit = symbol_info['持仓收益'].sum()
    # symbol_info['分配资金'] = (balance - profit) * symbol_info['分配比例']

    return symbol_info


# ===通过ccxt获取K线数据
def ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, limit):
    """
    获取指定币种的K线信息
    :param exchange:
    :param symbol:
    :param time_interval:
    :param limit:
    :return:
    """

    # 获取数据
    # data = exchange.fapiPublic_get_klines({'symbol': symbol, 'interval': time_interval, 'limit': limit})
    data = retry_wrapper(exchange.fapiPublic_get_klines, act_name='获取币种K线数据',
                         params={'symbol': symbol, 'interval': time_interval, 'limit': limit})

    # 整理数据
    df = pd.DataFrame(data, dtype=float)
    df.rename(columns={1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df[0], unit='ms')
    df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
    df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]

    return df


# ===单线程获取需要的K线数据，并检测质量。
def single_threading_get_binance_candle_data(exchange, symbol_list, symbol_info, time_interval, run_time, candle_num):
    """
    获取所有币种的k线数据，并初步处理
    :param exchange:
    :param symbol_config:
    :param symbol_info:
    :param time_interval:
    :param run_time:
    :param candle_num:
    :return:
    """

    symbol_candle_data = dict()  # 用于存储K线数据

    print('开始获取最新K线数据')
    # 遍历每一个币种
    for symbol in symbol_list:
        print(symbol, '开始时间：', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), end=' ')

        # 获取symbol该品种最新的K线数据
        df = ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, limit=candle_num)

        # 如果获取数据为空，再次获取
        # if df.empty:
            # continue

        # 获取到了最新数据
        print(symbol, '结束时间：', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # 用最新的close去更新symbol_info中的「当前价格」
        symbol_info.at[symbol, '当前价格'] = df.iloc[-1]['close']  # 该品种的最新价格
        symbol_candle_data[symbol] = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]  # 去除run_time周期的数据

    return symbol_candle_data


# ===获取需要的币种的历史K线数据。
def get_binance_history_candle_data(exchange, symbol_list, time_interval, candle_num, if_print=True):

    symbol_candle_data = dict()  # 用于存储K线数据
    print('获取交易币种的历史K线数据')
    print('需要获取的数据：', symbol_list)

    # 遍历每一个币种
    for symbol in symbol_list:

        # 获取symbol该品种最新的K线数据
        df = ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, limit=candle_num)

        # 为了保险起见，去掉最后一行最新的数据
        df = df[:-1]

        symbol_candle_data[symbol] = df  # 去除run_time周期的数据
        time.sleep(medium_sleep_time)

        if if_print:
            print(symbol, f'{candle_num}根历史k线数据获取成功')
            # print(symbol_candle_data[symbol].tail(3))

    print('成功获取交易币种的历史K线数据', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return symbol_candle_data


# ===批量下单
def place_binance_batch_order(exchange, symbol_order_params):

    all_order_info = []

    # 目前币安仅支持每次批量下5单
    num = 5  # 每个批量最多下单的数量
    for i in range(0, len(symbol_order_params), num):
        order_list = symbol_order_params[i:i + num]
        params = {'batchOrders': exchange.json(order_list),
                  'timestamp': int(time.time() * 1000)}
        # order_info = exchange.fapiPrivatePostBatchOrders(params)
        order_info = retry_wrapper(exchange.fapiPrivatePostBatchOrders, params=params, act_name='批量下单')
        all_order_info += order_info

        print('\n成交订单信息：')
        for _ in order_info:
            print(_)
        time.sleep(short_sleep_time)

    return all_order_info


# ==========趋势策略相关函数==========
def calculate_signal(symbol_config_df, complex_symbol_data):
    """
    计算交易信号：
    symbol_config_df中，每一行是一个具体策略，逐行计算策略信号
    计算完成后将整个symbol_config_df返回
    """

    print('\n')
    print('-'*30, '计算交易信号', '-'*30)
    # 初始化signal，target_pos等
    symbol_config_df['signal'] = None
    symbol_config_df['target_pos'] = pd.to_numeric(symbol_config_df['last_pos'], downcast='float')
    # 注意candle_begin_time_GMT8在时间上比当前时间滞后一根k线
    # 如1h策略，candle_begin_time_GMT8为10:00时，执行计算时其实是11:00
    symbol_config_df['candle_begin_time_GMT8'] = None
    symbol_config_df['close'] = None

    # 逐个遍历交易对
    for i, row in symbol_config_df.iterrows():

        complex_symbol = row['symbol'] #ETH-BTC

        # 赋值相关数据
        df = complex_symbol_data[complex_symbol].copy()  # 最新ETH-BTC数据
        now_pos = row['last_pos'] # 当前持仓方向，比如-0.2
        avg_price = row['last_open_price']  # 当前持仓均价

        # 根据策略计算出目标交易信号。
        if not df.empty:  # 当原始数据不为空的时候
            signal = getattr(Signals, row['strategy_name'])(df, now_pos, avg_price, row['para'])

            symbol_config_df['candle_begin_time_GMT8'].iat[i] = df['candle_begin_time_GMT8'].iat[-1]
            symbol_config_df['close'].iat[i] = df['close'].iat[-1]
            symbol_config_df['signal'].iat[i] = signal

        # 当signal为 1 0 -1时，需要重新计算目标仓位target_pos
        # 注意目标仓位target_pos包含了资金比例和杠杆信息
        # target_pos=-0.2 意味着将账户权益的0.2倍资金用于该策略的开空
        if signal in [1, 0, -1]:
            symbol_config_df['target_pos'].iat[i] = signal * row['position'] * row['leverage']

        print(i, row['symbol'], row['strategy_name'], row['para'], row['leverage']
              , row['position'], 'last_pos：',
              round(row['last_pos'], 3), '本周期signal: ', signal,
              'target_pos：', round(symbol_config_df['target_pos'].iat[i], 3))


    print('-'*30, '计算交易信号', '-'*30)

    return symbol_config_df


# 根据交易所的限制（最小下单单位、量等），修改下单的数量和价格
def modify_order_quantity_and_price(symbol, symbol_config, params):
    """
    根据交易所的限制（最小下单单位、量等），修改下单的数量和价格
    :param symbol:
    :param symbol_config:
    :param params:
    :return:
    """

    # 根据每个币种的精度，修改下单数量的精度
    params['quantity'] = round(params['quantity'], symbol_config[symbol]['最小下单量精度'])

    # 买单加价2%，卖单降价2%
    params['price'] = params['price'] * 1.02 if params['side'] == 'BUY' else params['price'] * 0.98
    # 根据每个币种的精度，修改下单价格的精度
    params['price'] = round(params['price'], symbol_config[symbol]['最小下单价精度'])

    return params


# 针对某个类型订单，计算下单参数。供cal_all_order_info函数调用
def cal_order_params(signal_type, symbol, symbol_info, symbol_config):
    """
    针对某个类型订单，计算下单参数。供cal_all_order_info函数调用
    :param signal_type:
    :param symbol:
    :param symbol_info:
    :param symbol_config:
    :return:
    """

    params = {
        'symbol': symbol,
        'side': binance_order_type[signal_type],
        'price': symbol_info.at[symbol, '当前价格'],
        'type': 'LIMIT',
        'timeInForce': 'GTC',
    }

    if signal_type in ['平空', '平多']:
        params['quantity'] = abs(symbol_info.at[symbol, '持仓量'])

    elif signal_type in ['开多', '开空']:
        params['quantity'] = symbol_info.at[symbol, '分配资金'] * symbol_config[symbol]['leverage'] / \
                   symbol_info.at[symbol, '当前价格']

    else:
        close_quantity = abs(symbol_info.at[symbol, '持仓量'])
        open_quantity = symbol_info.at[symbol, '分配资金'] * symbol_config[symbol]['leverage'] / \
                        symbol_info.at[symbol, '当前价格']
        params['quantity'] = close_quantity + open_quantity

    # 修改精度
    print(symbol, '修改精度前', params)
    params = modify_order_quantity_and_price(symbol, symbol_config, params)
    print(symbol, '修改精度后', params)

    return params


# 计算所有币种的下单参数
def cal_all_order_info(order_info_df):
    '''
    将order_info_df：

    symbol  open_pos    账户权益    当前价格        最小下单价精度 最小下单量精度      下单量
0   BTCUSDT       0.2  510.854511  36343.7              2              3  0.00281124
1  DOGEUSDT      -0.2  510.854511  0.35622              5           None     -286.82
2   ETHUSDT      -0.2  510.854511  2592.23              2              3  -0.0394143
3   LTCUSDT       0.2  510.854511   171.39              2              3    0.596131

    逐行转换为下单信息symbol_order_params：
 [
     {'symbol': 'BTCUSDT', 'side': 'BUY', 'price': '37289.39', 'quantity': '0.003', 'type': 'LIMIT', 'timeInForce': 'GTC'},
     {'symbol': 'DOGEUSDT', 'side': 'SELL', 'price': '0.35557', 'quantity': '841.0', 'type': 'LIMIT', 'timeInForce': 'GTC'},
     {'symbol': 'ETHUSDT', 'side': 'SELL', 'price': '2563.98', 'quantity': '0.039', 'type': 'LIMIT', 'timeInForce': 'GTC'},
     {'symbol': 'LTCUSDT', 'side': 'BUY', 'price': '176.63', 'quantity': '1.761', 'type': 'LIMIT', 'timeInForce': 'GTC'}
 ]

    '''

    # 每个币对的下单信息，逐个放入了该列表
    symbol_order_params = []

    for i, row in order_info_df.iterrows():
        symbol = row['symbol']
        side = 'BUY' if row['下单量'] > 0 else 'SELL'
        price = row['当前价格'] * 1.02 if side == 'BUY' else row['当前价格'] * 0.98
        quantity = abs(row['下单量'])
        # 修改精度
        price = round(price, int(row['最小下单价精度']))
        quantity = round(quantity, int(row['最小下单量精度']))

        if quantity == 0:  # 考察下单量是否为0
            print('\n', symbol, '下单量为0，忽略')
        elif price * quantity <= 5:  # 和最小下单额5美元比较
            print('\n', symbol, '下单金额小于5u，忽略')
        else:
            # 改成str
            price = str(price)
            quantity = str(quantity)
            params = {
                'symbol': symbol,
                'side': side,
                'price': price,
                'quantity': quantity,
                'type': 'LIMIT',
                'timeInForce': 'GTC',
            }
            print(symbol, '下单信息：', params)

            # 每个币对的下单信息，逐个放入了该列表
            symbol_order_params.append(params)

    return symbol_order_params


#########################################

def get_symbol_list(symbol_config_df):
    '''
    从symbol_config_df的symbol列中获取symbol，将需要抓取的数据提取出来
    :param symbol_config_df:
    :return: {'BTCUSDT', 'LTCUSDT', 'UNIUSDT', 'ETHUSDT', 'DOGEUSDT'}
    '''

    symbol_data_list = set() # 集合（set）是一个无序的不重复元素序列

    for i, row in symbol_config_df.iterrows():
        symbol_data_list.add(row['symbol_1'])
        symbol_data_list.add(row['symbol_2'])

    return symbol_data_list


def transfer_symbol_data(symbol_config_df, symbol_candle_data):

    complex_symbol_data = {}

    for complex_symbol in symbol_config_df['symbol']:
        symbol_1 = complex_symbol.split('-')[0] + 'USDT'
        symbol_2 = complex_symbol.split('-')[1] + 'USDT'
        df = pd.merge(symbol_candle_data[symbol_1],
                      symbol_candle_data[symbol_2],
                      how='inner',
                      on='candle_begin_time_GMT8')
        df['open'] = df['open_x'] / df['open_y']
        df['high'] = df['high_x'] / df['low_y']  # 注意是high/low
        df['low'] = df['low_x'] / df['high_y']  # 注意是low/high
        df['close'] = df['close_x'] / df['close_y']
        df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close']]

        complex_symbol_data[complex_symbol] = df

    return complex_symbol_data



def update_precision(exchange, symbol_info):

    # 获取u本为合约交易对的信息
    exchange_info = retry_wrapper(exchange.fapiPublic_get_exchangeinfo, act_name='查看合约基本信息')

    # 转化为dataframe
    df = pd.DataFrame(exchange_info['symbols'])
    # df['minPrice'] = df['filters'].apply(lambda x: x[0]['minPrice'])
    # df['minQty'] = df['filters'].apply(lambda x: x[1]['minQty'])
    df['tickSize'] = df['filters'].apply(lambda x: math.log(1/float(x[0]['tickSize']), 10))
    df['stepSize'] = df['filters'].apply(lambda x: math.log(1/float(x[1]['stepSize']), 10))
    df = df[['symbol', 'pricePrecision', 'quantityPrecision', 'tickSize', 'stepSize']]
    df.set_index('symbol', inplace=True)

    # 赋值
    symbol_info['最小下单价精度'] = None
    symbol_info['最小下单量精度'] = None

    for symbol, row in symbol_info.iterrows():

        symbol_info.at[symbol, '最小下单价精度'] = round(df.at[symbol, 'tickSize'])
        # p = df.at[symbol, 'quantityPrecision']
        # symbol_info.at[symbol, '最小下单量精度'] = None if p == 0 else round(p)
        symbol_info.at[symbol, '最小下单量精度'] = round(df.at[symbol, 'quantityPrecision'])

    return symbol_info




def cal_order_info_df(symbol_config_df, symbol_info):
    '''
    根据 symbol_config_df 和 symbol_info 计算出需要下单的币种与下单量
    返回数据为：
    symbol  open_pos    账户权益     当前价格           最小下单价精度  最小下单量精度      下单量
0   BTCUSDT       0.2  502.695936  36997.33000               2             3.0    0.002717
1  DOGEUSDT       0.6  502.695936      0.37782               5             NaN  798.310206
2   ETHUSDT      -0.2  502.695936   2669.44000               2             3.0   -0.037663
3   LTCUSDT      -0.6  502.695936    176.65000               2             3.0   -1.707430

    后续下单时，根据该 df 逐行下单

    '''

    # 开仓量 = 目标仓位 - 现有仓位
    _symbol_config_df = symbol_config_df.copy()
    _symbol_config_df['open_pos'] = _symbol_config_df['target_pos'] - _symbol_config_df['last_pos']
    # 如果开仓量.round(0)==0.0，则意味不需要开仓，不必保留
    _symbol_config_df = _symbol_config_df[_symbol_config_df['open_pos'].round(5) != 0.0]


    _symbol_config_df.rename({'symbol': 'complex_symbol', 'signal': 'complex_signal',
                             'open_pos': 'complex_open_pos'}, axis=1, inplace=True)
    print('\n')
    print('-'*30, '有发出信号的策略', '-'*30)
    print(_symbol_config_df[['complex_symbol', 'leverage', 'strategy_name', 'para',
                             'position', 'last_signal', 'last_pos', 'last_open_price',
                             'complex_signal', 'target_pos', 'complex_open_pos']])
    print('-'*30, '有发出信号的策略', '-'*30)
    print('\n')

    # 无信号
    if _symbol_config_df.empty:
        print('当前周期无信号', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return pd.DataFrame()

    signal_df = _symbol_config_df.copy()
    signal_df['time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    signal_df = signal_df[['time', 'complex_symbol', 'leverage',
                           'strategy_name', 'para', 'position', 'last_signal',
                           'last_pos', 'last_open_price',
                           'complex_signal', 'target_pos', 'complex_open_pos']]

    # 将signal信息保存至本地
    if not signal_df.empty:
        if os.path.exists(signal_file):
            # 若文件存在，则用追加模式mode='a'，且不写入列名header=False
            signal_df.to_csv(signal_file, mode='a', index=False, header=False)
        else:
            # 若文件本身不存在，则用写入模式mode='w'，且需要写入列名header=True
            signal_df.to_csv(signal_file, mode='w', index=False, header=True)

        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 成功本周期信号数据保存数据至：{signal_file}\n\n')


    # 将ETH-BTC一行分两行
    _df = _symbol_config_df['complex_symbol'].str.split('-', expand=True).stack()
    _df = _df.reset_index(level=1, drop=True).rename('s')
    _symbol_config_df = _symbol_config_df.join(_df)
    _symbol_config_df['symbol'] = _symbol_config_df['s'] + 'USDT'
    '''
      complex_symbol  leverage       strategy_name     para  position  last_pos  last_open_price  symbol_1 symbol_2  最小下单价精度_1  最小下单价精度_2  最小下单量精度_1  最小下单量精度_2  complex_signal  target_pos candle_begin_time_GMT8     close  complex_open_pos     s    symbol
0        ETH-BTC         1  real_signal_random  [10, 2]       0.2         0          0   ETHUSDT  BTCUSDT                 2                 2               3.0                 3            -1.0        -0.2    2021-06-04 10:55:00  0.072387              -0.2   ETH   ETHUSDT
0        ETH-BTC         1  real_signal_random  [10, 2]       0.2         0          0   ETHUSDT  BTCUSDT                 2                 2               3.0                 3            -1.0        -0.2    2021-06-04 10:55:00  0.072387              -0.2   BTC   BTCUSDT
2       DOGE-LTC         1  real_signal_random  [10, 2]       0.2         0          0  DOGEUSDT  LTCUSDT                 5                 2               NaN                 3             1.0         0.2    2021-06-04 10:55:00  0.002100               0.2  DOGE  DOGEUSDT
2       DOGE-LTC         1  real_signal_random  [10, 2]       0.2         0          0  DOGEUSDT  LTCUSDT                 5                 2               NaN                 3             1.0         0.2    2021-06-04 10:55:00  0.002100               0.2   LTC   LTCUSDT
3       DOGE-LTC         2  real_signal_random  [10, 2]       0.2         0          0  DOGEUSDT  LTCUSDT                 5                 2               NaN                 3             1.0         0.4    2021-06-04 10:55:00  0.002100               0.4  DOGE  DOGEUSDT
3       DOGE-LTC         2  real_signal_random  [10, 2]       0.2         0          0  DOGEUSDT  LTCUSDT                 5                 2               NaN                 3             1.0         0.4    2021-06-04 10:55:00  0.002100               0.4   LTC   LTCUSDT

    '''


    _temp = _symbol_config_df[['complex_symbol', 's', 'symbol', 'complex_signal', 'complex_open_pos']].copy()
    _temp.reset_index(drop=True, inplace=True)
    _temp['signal'] = None
    _temp['open_pos'] = 0.0

    for i, row in _temp.iterrows():
        # 如果ETH-BTC信号为1，则ETH信号为1，BTC信号为-1
        if row['s'] == row['complex_symbol'].split('-')[0]:
            _temp['signal'].iat[i] = row['complex_signal']
            _temp['open_pos'].iat[i] = row['complex_open_pos'] / 2 # 注意需要除2
        elif row['s'] == row['complex_symbol'].split('-')[1]:
            _temp['signal'].iat[i] = -row['complex_signal']
            _temp['open_pos'].iat[i] = -row['complex_open_pos'] / 2 # 注意需要除2

    _temp = _temp[['symbol', 'signal', 'open_pos']]
    _temp = _temp.groupby(['symbol']).sum()
    _temp = _temp[_temp['open_pos'].round(5) != 0.0]


    '''
                          open_pos
            symbol            
            BTCUSDT        0.2
            DOGEUSDT       0.6
            ETHUSDT       -0.2
            LTCUSDT       -0.6
    '''

    # 将_temp与symbol_info合并，获取账户权益、币对的下单精度信息
    _temp = pd.merge(_temp, symbol_info.reset_index().rename({'index': 'symbol'}, axis=1), on='symbol')
    _temp['下单量'] = _temp['open_pos'] * _temp['账户权益'] / _temp['当前价格']
    # 对除数为0的情况作容错处理，好像没什么必要...
    _temp['下单量'].replace([np.inf, -np.inf, "", np.nan], 0, inplace=True)
    _temp['下单金额'] = _temp['open_pos'] * _temp['账户权益']

    _temp = _temp[['symbol', 'open_pos', '账户权益', '当前价格',
                   '最小下单价精度', '最小下单量精度', '下单量', '下单金额']]


    '''
        symbol  open_pos    账户权益 当前价格 最小下单价精度 最小下单量精度      下单量
0  BTCUSDT       0.2  507.338303    36776              2              3  0.00275907
1  ETHUSDT      -0.2  507.338303  2625.58              2              3  -0.0386458
    '''

    print('-' * 30, 'order_info_df', '-' * 30)
    print(_temp[['symbol', 'open_pos', '账户权益', '当前价格', '下单量', '下单金额']])
    print('-' * 30, 'order_info_df', '-' * 30)
    print('\n')

    return _temp




def update_symbol_config_df(symbol_config_df):
    # 每次循环结束后，更新symbol_config_df

    symbol_config_df['open_pos'] = symbol_config_df['target_pos'] - symbol_config_df['last_pos']
    # change_flag标记出 open_pos 不为0的策略
    # 该类策略需要重新记录last_pos和last_open_price
    symbol_config_df['change_flag'] = symbol_config_df['open_pos'].round(5) != 0

    symbol_config_df.loc[symbol_config_df['change_flag'], 'last_pos'] = symbol_config_df['target_pos']
    symbol_config_df.loc[symbol_config_df['change_flag'], 'last_open_price'] = symbol_config_df['close']
    symbol_config_df.loc[symbol_config_df['signal'] == 0, 'last_open_price'] = 0 # 如果策略发出的是平仓信号，则last_open_price=0
    symbol_config_df.loc[symbol_config_df['change_flag'], 'last_signal'] = symbol_config_df['signal']

    symbol_config_df = symbol_config_df[['symbol', 'leverage', 'strategy_name',
                                         'para', 'position', 'last_signal', 'last_pos',
                                         'last_open_price']]

    symbol_config_df.to_csv('symbol_config_df.csv', index=False)
    print('\n已更新 symbol_config_df.csv', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return symbol_config_df


def check_palce_order(symbol_order_params, all_order_info):
    '''
    逐一对比下单参数symbol_order_params与交易所返回的all_order_info，检查下单是否成功

    :param symbol_order_params: 下单时传给交易所的参数
    :param all_order_info: 交易所返回的参数

    :return:
    '''
    # 遍历下单参数
    for i, order_param in enumerate(symbol_order_params):
        symbol = order_param.get('symbol')

        # 如果all_order_info没有orderId和clientOrderId，即下单失败
        orderId = all_order_info[i].get('orderId')
        clientOrderId = all_order_info[i].get('clientOrderId')
        price = all_order_info[i].get('price')
        origQty = all_order_info[i].get('origQty')
        side = all_order_info[i].get('side')
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if clientOrderId:
            order_info_str = f'{time}, {symbol}, {side}, {price}, {origQty}, {orderId}, {clientOrderId}\n'

            # 将下单信息记录到本地
            with open('all_order_info.txt', 'a+') as f:
                f.write(order_info_str)

        else:  # 下单失败
            err_msg = '下单失败：' + str(all_order_info[i]) + '\n下单参数为：' + str(order_param)
            # 将漏单发送至手机，后续可根据下单参数手动下单
            # 如果不手动下单或手动更改symbol_config_df.csv，会使得理论仓位与实际仓位对不上
            send_wx_error_msg(err_msg)
            send_dingding_error_msg(err_msg)
            print(err_msg)




# ===发送钉钉相关函数
# 计算钉钉时间戳
def cal_timestamp_sign(secret):
    # 根据钉钉开发文档，修改推送消息的安全设置https://ding-doc.dingtalk.com/doc#/serverapi2/qf2nxq
    # 也就是根据这个方法，不只是要有robot_id，还要有secret
    # 当前时间戳，单位是毫秒，与请求调用时间误差不能超过1小时
    # python3用int取整
    timestamp = int(round(time.time() * 1000))
    # 密钥，机器人安全设置页面，加签一栏下面显示的SEC开头的字符串
    secret_enc = bytes(secret.encode('utf-8'))
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = bytes(string_to_sign.encode('utf-8'))
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    # 得到最终的签名值
    sign = parse.quote_plus(base64.b64encode(hmac_code))
    return str(timestamp), str(sign)

def loadJson(filename):
    with open(filename, 'r', encoding='UTF-8') as f:
        data = json.load(f)
        return data


def saveJson(filename, data):
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)



def send_dingding_msg(content, dingding_api):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。
                        例如如下地址：https://oapi.dingtalk.com/robot/send?access_token=81a0e96814b4c8c3132445f529fbffd4bcce66
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """

    robot_id = dingding_api['robot_id']
    secret = dingding_api['secret']

    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}
        headers = {"Content-Type": "application/json;charset=utf-8"}
        # https://oapi.dingtalk.com/robot/send?access_token=XXXXXX&timestamp=XXX&sign=XXX
        timestamp, sign_str = cal_timestamp_sign(secret)
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id + \
              '&timestamp=' + timestamp + '&sign=' + sign_str
        body = json.dumps(msg)
        requests.post(url, data=body, headers=headers, timeout=10)
        print('成功发送钉钉', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        print("发送钉钉失败:", e, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))



# 报错机器人用专门的api，不与其他机器人混用
def send_dingding_error_msg(content):
    # print(content)
    # 报错机器人专用api
    send_dingding_msg(name + content, dingding_error_api)




# 发送微信
def send_wx_message(content, wx_api):

    corpid = wx_api['corpid']
    secret = wx_api['secret']
    agentid = wx_api['agentid']

    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    data = {
        "corpid": corpid,
        "corpsecret": secret
    }

    try:  # 容错
        r = requests.get(url=url, params=data)
    except Exception:
        print("send_message()的requests.get()失败，请检查网络连接。")
    # print(r.json())
    # exit()
    token = r.json()['access_token']
    # Token是服务端生成的一串字符串，以作客户端进行请求的一个令牌
    # 当第一次登录后，服务器生成一个Token便将此Token返回给客户端
    # 以后客户端只需带上这个Token前来请求数据即可，无需再次带上用户名和密码
    url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(token)
    data = {
        "toparty": "1",
        "msgtype": "text",
        "agentid": agentid,
        "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")},
        "safe": "0"
    }

    try:  # 容错
        result = requests.post(url=url, data=json.dumps(data))
        print('成功发送微信', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        print("send_message()的requests.post()失败，请检查网络连接。", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return result.text


# 报错机器人用专门的api，不与其他机器人混用
def send_wx_error_msg(content):
    print(content)
    # 报错机器人专用api：wx_error_api
    send_wx_message(name + content, wx_error_api)




# 保存净值数据，方便事后统计资金曲线
def store_equity_history(equity, run_time, equity_file_name):
    """
    将数据保存到本地
    """
    equity_str = f'{run_time} {equity}\n'

    # 把更新的时间及净值信息存储到文件的最后一行
    with open(equity_file_name, 'a+') as f:
        f.write(equity_str)

