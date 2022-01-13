"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import datetime
from concurrent.futures.thread import ThreadPoolExecutor

from functions import Function
from typing import Dict
from config import Config, pre_cal_key
from utility import Utility
import pandas as pd
import time
import kline_fetcher

pd.options.mode.chained_assignment = None
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

ignore_symbols = []  # 辣鸡币对, 接口问题导致不开空


def main(config_origin: Dict = None):
    print(f'pandas version: {pd.__version__}')
    while True:
        config = Config(config_origin)
        loop = config['system']['loop']
        exchange = config[pre_cal_key]['exchange']
        fetcher_type = config['kline']['fetcher']['type']
        kline_type = config['kline']['type']
        trade_ratio = config['trade']['trade_ratio']
        strategy_list = config['strategy_list']
        debug = config['system']['debug']
        cheat_seconds = config['system']['cheat_seconds']
        long_sleep_time = config['system']['long_sleep_time']
        fetcher = kline_fetcher.type_fetcher[fetcher_type](config)

        utility = Utility(config)
        function = Function(config)

        with ThreadPoolExecutor(max_workers=3) as tp:
            def _get_balance():
                return utility.robust(exchange.fapiPrivate_get_balance, )  # 获取账户净值

            def _get_exchange_spot_info():
                return utility.robust(exchange.public_get_exchangeinfo, )  # 获取交易所现货信息

            def _get_exchange_swap_info():
                return utility.robust(exchange.fapiPublic_get_exchangeinfo, )  # 获取交易所合约信息

            def _get_available_symbols():
                if kline_type == 'spot':
                    exchange_info_swap_future = tp.submit(_get_exchange_swap_info)
                    exchange_info_spot_future = tp.submit(_get_exchange_spot_info)
                    exchange_swap_info = exchange_info_swap_future.result()
                    exchange_spot_info = exchange_info_spot_future.result()
                    swap_symbols = [_symbol_info['symbol'] for _symbol_info in exchange_swap_info['symbols']]
                    spot_symbols = [_symbol_info['symbol'] for _symbol_info in exchange_spot_info['symbols']]
                    skipped_symbols = [symbol for symbol in swap_symbols if symbol not in set(spot_symbols)]
                    _available_symbols = [symbol for symbol in swap_symbols if symbol in set(spot_symbols)]
                    print(f'skipped_symbols: {skipped_symbols}')
                    print(f'available symbols: {_available_symbols}')
                    return _available_symbols
                else:
                    exchange_info_swap_future = tp.submit(_get_exchange_swap_info)
                    exchange_swap_info = exchange_info_swap_future.result()
                    swap_symbols = [_symbol_info['symbol'] for _symbol_info in exchange_swap_info['symbols']]
                    return swap_symbols

            balance_future = tp.submit(_get_balance)
            available_symbols = _get_available_symbols()

        balance = balance_future.result()
        balance = pd.DataFrame(balance)
        equity = float(balance[balance['asset'] == 'USDT']['balance'])
        trade_usdt = equity * trade_ratio
        print('trade_usdt', trade_usdt)

        # =====获取每个策略分配的资金：固定资金，之后的版本会改成浮动
        strategy_trade_usdt = function.cal_strategy_trade_usdt(strategy_list, trade_usdt)
        print(strategy_trade_usdt, '\n')

        # 全部处理完，更新k线
        symbol_list = [symbol for symbol in available_symbols if symbol.endswith('USDT')]
        [symbol_list.remove(ignore_symbol) for ignore_symbol in ignore_symbols]

        # =====获取账户的实际持仓
        symbol_info = function.update_symbol_info(exchange, symbol_list)
        print(symbol_info.T, '\n')

        # =====sleep直到下一个整点小时
        run_time = datetime.datetime.now().replace(minute=0, second=0)

        if loop:
            if debug:
                pass
            else:
                run_time = run_time + datetime.timedelta(hours=1)
                while datetime.datetime.now() < run_time:
                    time.sleep(1)

        utc_now = datetime.datetime.utcnow()
        if utc_now.hour % 8 == 0:
            print('\n', '每日结算时间', '\n\n')
            if utc_now.minute < 1:
                sleep_seconds = 60 - utc_now.second
                print(f'睡眠{sleep_seconds}秒')
                time.sleep(sleep_seconds)

        # =====并行获取所有币种的1小时K线
        symbol_candle_data = fetcher.fetch_klines(symbol_list, kline_type)
        # symbol_candle_data = function.fetch_all_binance_swap_candle_data(exchange, symbol_list, kline_type)
        print(symbol_candle_data['BTCUSDT'].tail(2), '\n')

        # =====选币数据整理 & 选币
        select_coin = function.cal_factor_and_select_coin(strategy_list, symbol_candle_data, run_time)
        print(select_coin[['key', 's_time', 'symbol', '方向']].T, '\n')

        # =====计算实际下单量
        symbol_info = function.cal_order_amount(symbol_info, select_coin, strategy_trade_usdt)
        print('实际下单量：\n', symbol_info, '\n')

        # =====获取币种的最新价格
        symbol_last_price = function.fetch_binance_ticker_data(exchange)

        # =====逐个下单
        function.place_order(symbol_info, symbol_last_price)

        if not loop:
            print('非循环调用, 结束程序')
            return

        # 本次循环结束
        # 时间补偿
        print('\n', '-' * 20, '本次循环结束，%f秒后进入下一次循环' % (long_sleep_time + int(cheat_seconds)), '-' * 20, '\n\n')
        time.sleep(long_sleep_time + int(cheat_seconds))

        if debug:
            # 调试模式，直接退出本次循环
            return


if __name__ == '__main__':
    tn = datetime.datetime.now()
    main()
    print(datetime.datetime.now() - tn)
