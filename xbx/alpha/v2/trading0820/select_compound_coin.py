"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
from Signals import *
from Functions import *
from Config import *
from Utility import *
import configparser
config = configparser.ConfigParser()


pd.options.mode.chained_assignment = None
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def main():

    while True:
        
        config.read('config.ini')
        trade_ratio = float(config['trade']['trade_ratio'])
        debug = int(config['default']['debug'])

        balance = robust(exchange.fapiPrivate_get_balance,)  # 获取账户净值
        balance = pd.DataFrame(balance)
        equity = float(balance[balance['asset'] == 'USDT']['balance'])
        trade_usdt = equity*trade_ratio
        print('trade_usdt',trade_usdt)

        # =====获取每个策略分配的资金：固定资金，之后的版本会改成浮动
        strategy_trade_usdt = cal_strategy_trade_usdt(stratagy_list, trade_usdt)
        print(strategy_trade_usdt, '\n')

        # 全部处理完，更新k线
        # https://binance-docs.github.io/apidocs/futures/cn/#0f3f2d5ee7
        exchange_info = robust(exchange.fapiPublic_get_exchangeinfo,)  # 获取账户净值

        _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
        _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')] #过滤usdt合约
        symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list] # 过滤黑名单

        # symbol_list = symbol_list[:5]
        # symbol_list = ['BTCUSDT']

        # ===从exchange_info中获取每个币种最小交易量
        min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}
        # 案例：{'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0}
        # 表示小数点后多少位
        # minQty: 数量下限, 最小数量

        # ===从exchange_info中获取每个币种下单精度
        price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in exchange_info['symbols']}
        # 案例：{'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3}
        # 表示小数点后多少位
        # tickSize: 订单最小价格间隔

        # =====获取账户的实际持仓
        symbol_info = update_symbol_info(exchange, symbol_list)
        print(symbol_info.T, '\n')

        print('持仓 >0 的币:\n', symbol_info[abs(symbol_info['当前持仓量']) > 0])

        # =====sleep直到下一个整点小时
        if debug == 1:
            # 调试模式，直接进入下一个循环
            run_time = sleep_until_run_time('1h', if_sleep=False, cheat_seconds=cheat_seconds)
        else:
            run_time = sleep_until_run_time('1h', if_sleep=True, cheat_seconds=cheat_seconds)

        if (run_time.hour % 8 == 0):
            print('\n','每日结算时间','\n\n')
            if not debug:
                time.sleep(60)

        # =====并行获取所有币种的1小时K线
        symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time)
        print(symbol_candle_data['BTCUSDT'].tail(2), '\n')
        print(symbol_candle_data['BTCUSDT'].shape, '\n')

        # tmp = pd.read_csv('./test_btc-usdt.csv', parse_dates=['candle_begin_time'])
        # symbol_candle_data['BTCUSDT'] = tmp

        # =====选币数据整理 & 选币
        # select_coin = cal_factor_and_select_coin(stratagy_list, symbol_candle_data, run_time)
        select_coin = cal_factor_and_select_coin_lasso(stratagy_list, symbol_candle_data, run_time)
        print(select_coin[['key', 's_time', 'symbol', '方向']].T, '\n')

        # =====计算实际下单量
        symbol_info = cal_order_amount(symbol_info, select_coin, strategy_trade_usdt)
        print('实际下单量：\n', symbol_info, '\n')

        # =====获取币种的最新价格
        symbol_last_price = fetch_binance_ticker_data(exchange)

        # =====逐个下单
        place_order(symbol_info, symbol_last_price, min_qty , price_precision)

        # 本次循环结束
        # 时间补偿
        print('\n', '-' * 20, '本次循环结束，%f秒后进入下一次循环' % (long_sleep_time+int(cheat_seconds)), '-' * 20, '\n\n')
        time.sleep(long_sleep_time+int(cheat_seconds))

        if debug == 1:
            # 调试模式，直接退出本次循环
            exit()


if __name__ == '__main__':
    main()
