# -*- coding: utf-8 -*-
import pandas as pd
import time
from multiprocessing import Pool, cpu_count
from datetime import  timedelta
from functools import partial

from icecream import ic

def Timestamp():
    return '%s |> ' % time.strftime("%Y-%m-%d %T")

# 定制输出格式
ic.configureOutput(prefix=Timestamp)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 6000)  # 最多显示数据的行数


# ===重试机制
def retry_wrapper(func, params={}, act_name='', sleep_seconds=3, retry_times=100):
    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result
        except Exception as e:
            ic(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            time.sleep(sleep_seconds)
    else:
        # send_dingding_and_raise_error(output_info)
        raise ValueError(act_name, '报错重试次数超过上限，程序退出。')


def get_history_data_more_than_1500(exchange, symbol, time_interval, run_time, candle_num):
    ic('获取交易币种的历史K线数据',symbol)
    # 将结束时间改为UTC时间
    end_time_real = pd.to_datetime(run_time)-pd.Timedelta(hours=8)

    # 用结束时间减k线时间计算开始时间
    if time_interval.find('m') >= 0:  
       start_time_real = end_time_real - timedelta(minutes=int(time_interval.split('m')[0]) * candle_num)
       min_timedelta = timedelta(minutes=int(time_interval.split('m')[0])) # 最小时间偏差
    elif(time_interval.find('h') >= 0):
       start_time_real = end_time_real - timedelta(hours=int(time_interval.split('h')[0]) * candle_num)
       min_timedelta = timedelta(hours=int(time_interval.split('h')[0]))
    else:  # 注意暂时未判断按天的策略
        ic(time_interval, '时间间隔格式错误，请修改')
        raise ValueError
                                     
    # 将时间parse，由于api参数规定，必须用parse的时间，该行代码用于测试，本代码以startTime作为
    # 每次修改的参数，如果读者想用endTime进行开发则需要parse endTime。
    end_time_real_parse = exchange.parse8601(str(end_time_real))


    df_all = []
    while pd.to_datetime(start_time_real) < pd.to_datetime(end_time_real)-min_timedelta:
        # parse startTime
        start_time_real_parse = exchange.parse8601(str(start_time_real))
        kline = retry_wrapper(exchange.fapiPublic_get_klines, act_name='获取币种K线数据',
                              params={'symbol': symbol, 'interval': time_interval, 'startTime': start_time_real_parse,
                                      'limit': 1500})
        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                   'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(kline, columns=columns, dtype='float')

        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        df = df[columns]
        if len(df) == 0:
            #start_time_real = start_time_real + timedelta(hours=time_rule * candle_num)
            continue
        # +1min 是为了从下一根k线开始
        start_time_real = df.iloc[-1]['candle_begin_time'] - pd.Timedelta(hours=8) + pd.Timedelta(minutes=1)
        df_all.append(df)

    df_all = pd.concat(df_all)
    df_all.sort_values(by='candle_begin_time', inplace=True)
    df_all.drop_duplicates(subset=['candle_begin_time'], inplace=True)
    df_all.reset_index(drop=True, inplace=True)
    # 删除runtime那行的数据，如果有的话
    df_all = df_all[df_all['candle_begin_time'] != run_time]
    # 这个函数是获取历史数据的，去掉最后一行最新的数据。如果将来需要改为实时获取，修改删除这一行
    df_all = df_all[:-1]
    return df_all


def ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, run_time, limit=1000):
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
    kline = retry_wrapper(exchange.fapiPublic_get_klines, act_name='获取币种K线数据',
                          params={'symbol': symbol, 'interval': time_interval, 'limit': limit})
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(kline, columns=columns, dtype='float')

    # 整理数据
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    df = df[columns]
    # 删除runtime那行的数据，如果有的话
    df = df[df['candle_begin_time'] != run_time]
    return df


def get_data(symbol, exchange, candle_num, time_interval, run_time):
    # 获取symbol该品种最新的K线数据
    # 当获取超过1500根k线
    if candle_num > 1500:
        df = get_history_data_more_than_1500(exchange, symbol, time_interval, run_time, candle_num)
    else:
        df = ccxt_fetch_binance_candle_data(exchange, symbol, time_interval, run_time, candle_num)

    ic(symbol,df.head(),df.tail())

    df['symbol'] = symbol
    return symbol, df


# ===获取需要的币种的历史K线数据。
def get_binance_history_candle_data(exchange, symbol_list, time_interval, run_time, candle_num):
    ic('获取交易币种的历史K线数据')

    f = partial(get_data, exchange=exchange, candle_num=candle_num, time_interval=time_interval, run_time=run_time)
    # 防止太多请求
    n_jobs = cpu_count()
    if candle_num > 1500:
        n_jobs = 2
    with Pool(n_jobs) as p:
        data = p.map(f, symbol_list)

    return dict(data)