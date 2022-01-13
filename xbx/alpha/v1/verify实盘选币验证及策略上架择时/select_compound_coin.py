from Functions import *
from Config import *
from Utility import *
import configparser

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


def main():
    c_rate = 4 / 10000
    start_time = '2021-04-10 00:00:00'
    black_symbol_list = []
    # 全部处理完，更新k线
    exchange_info = robust(exchange.fapiPublic_get_exchangeinfo)
    _symbol_list = [x['symbol'] for x in exchange_info['symbols'] if
                    x['status'] == 'TRADING']  # 即将下架的币如BTCST的status是PENDING_TRADING
    symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT') and symbol not in black_symbol_list]

    # =====sleep直到下一个整点小时
    run_time = sleep_until_run_time('1h', if_sleep=False, cheat_seconds=cheat_seconds)

    # =====并行获取所有币种的1小时K线
    symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time)
    print(symbol_candle_data['BTCUSDT'].head(2))

    # =====选币数据整理 & 选币
    cal_factor_and_select_coin(stratagy_list, symbol_candle_data, c_rate, start_time)


if __name__ == '__main__':
    main()
