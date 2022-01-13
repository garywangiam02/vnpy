# coding=utf-8
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, List

import ccxt
import pandas as pd
from datetime import datetime, timedelta

import kline_fetcher
import kline_pusher
from kline_fetcher import KlineWebFetcher

from config import Config, utc_offset
import robust

LIMIT = 1000
batch_save_buffer = 1000
ignore_symbols = ['BTCSTUSDT']  # 辣鸡币对, 接口问题导致不开空


def run_function_till_success(function, tryTimes=5, sleepTimes=60):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    while True:
        if retry > tryTimes:
            return False
        try:
            result = function()
            return [result, retry]
        except (Exception) as reason:
            print(reason)
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内


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
    params = {'symbol': symbol, 'interval': '1h', 'limit': limit}
    kline = robust(exchange.fapiPublicGetKlines, params)

    # 将数据转换为DataFrame
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(kline, columns=columns, dtype='float')
    df.sort_values('candle_begin_time', inplace=True)
    df['symbol'] = symbol  # 添加symbol列
    columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num']
    df = df[columns]

    # 整理数据
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + timedelta(hours=utc_offset)
    # 删除runtime那行的数据，如果有的话
    df = df[df['candle_begin_time'] < run_time]

    return symbol, df


def get_klines_from_db(db_engine, symbols: List[str], limit: int = 1000) -> Dict[str, pd.DataFrame]:
    select_sql_prefix = f"select symbol, candle_begin_time, open, high, low, close, volume, quote_volume, trade_num " \
                        f"from klines where symbol in "
    select_sql_suffix = f" order by candle_begin_time desc limit {len(symbols) * limit}"
    symbols_format = [f"'{symbol}'" for symbol in symbols]
    symbol_condition_sql = f"({','.join(symbols_format)})"
    select_sql = f'{select_sql_prefix}{symbol_condition_sql}{select_sql_suffix}'
    result = {}
    with db_engine.begin() as conn:
        records_result_set = conn.execute(select_sql).fetchall()
        if len(records_result_set) <= 0:
            return result
    columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num']
    df = pd.DataFrame(records_result_set, columns=columns, dtype='float')
    df.sort_values('candle_begin_time', inplace=True)
    for symbol, symbol_df in df.groupby('symbol'):
        result[symbol] = symbol_df
    return result


def get_klines_from_web(symbols, kline_type: str) -> Dict[str, pd.DataFrame]:
    """
    并行获取所有币种永续合约数据的1小时K线数据
    :param exchange:
    :param symbol_list:
    :param run_time:
    :return:
    """
    fetcher = KlineWebFetcher({
        'system': {
            'try_times': 3,
            'sleep_seconds': 10
        },
        'fetcher': {
            'worker': {
                'worker_count': 10
            }
        }
    })
    return fetcher.fetch_klines(symbols, kline_type)


def main(config_origin: Dict = None):
    config = Config(config_origin, False)
    pusher_type = config['kline']['pusher']['type']
    fetcher = kline_fetcher.type_fetcher['web'](config)
    pusher = kline_pusher.type_pusher[pusher_type](config)
    exchange = ccxt.binance()
    rb = robust.Robust(config_origin)

    def _get_symbols(kline_type: str):
        kline_type_api = {
            'spot': exchange.public_get_exchangeinfo,
            'swap': exchange.fapiPublic_get_exchangeinfo
        }
        exchange_info = rb.robust(kline_type_api[kline_type], )
        return [_symbol_info['symbol'] for _symbol_info in exchange_info['symbols']]

    with ThreadPoolExecutor(max_workers=2) as tp:
        spot_symbols_query_future = tp.submit(_get_symbols, 'spot')
        swap_symbols_query_future = tp.submit(_get_symbols, 'swap')
        spot_symbols = spot_symbols_query_future.result()
        swap_symbols = swap_symbols_query_future.result()
        symbols = [symbol for symbol in spot_symbols if symbol in swap_symbols]

        def _call(kline_type):
            klines = fetcher.fetch_klines(symbols, kline_type)
            pusher.push_klines(kline_type, klines)

        futures = [tp.submit(_call, kline_type) for kline_type in ['spot', 'swap']]
        [future.result() for future in futures]


if __name__ == '__main__':
    config = {
        "system": {
            "debug": True,
            "loop": False,
            "cheat_seconds": 0,
            "long_sleep_time": 20,
            "web_query_kline_size": 1000
        },
        "robust": {
            "sleep_seconds": 20,
            "try_times": 10
        },
        "kline": {
            "type": "swap",
            "fetcher": {
                "kline_per_limit": 1000,
                "worker": {
                    "worker_count": 10
                },
                "config": {
                    "web": {
                        "fix_time": True
                    }
                }
            },
            "pusher": {
                "type": "s3",
                "worker": {
                    "worker_count": 3
                },
                "config": {
                    "s3": {
                        "region_name": "zxczxc",
                        "bucket_name": "czxczxc"
                    },
                    "db": {
                        "driver": "driver",
                        "host": "xxxx.rds.amazonaws.com",
                        "port": 3306,
                        "username": "asdasd",
                        "password": "dfgdfgdfg",
                        "db_name": "asdasdasd",
                        "batch_save_buffer": 1000,
                        "conn_pool_size": 3
                    }
                }
            }
        }
    }
    main(config)
