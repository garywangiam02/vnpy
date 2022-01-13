from coin_alpha_kopo_V1.comon.TWAP_FREESTEP import *
import pickle
import math
import os

pd.options.mode.chained_assignment = None
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
import warnings

warnings.filterwarnings('ignore')

import random


def select_coin(exchange, black_symbol_list, stratagy_list, name, dingding_id, dingding_secret, Percent, debug):
    while True:
        # =====开始每小时的循环
        try:

            # ===获取交易所相关数据
            exchange_info = robust(exchange.fapiPublic_get_exchangeinfo, )

            _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
            _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # 过滤usdt合约
            symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名

            # 设置杠杆

            # 设置杠杆为2
            for symbol in symbol_list:
                params = {'symbol': symbol, 'leverage': 2}
                a = robust(exchange.fapiPrivatePostLeverage, params)
                # print(a)

            # 检查有木有多出来的挂单，主要防止流动性不足
            order_info = robust(exchange.fapiPrivateGetOpenOrders, )
            if order_info:
                for order in order_info:
                    orderId = order['orderId']
                    symbol = order['symbol']
                    print(symbol, orderId)
                    params = {'symbol': symbol, 'orderId': orderId}
                    delete_order = robust(exchange.fapiPrivateDeleteOrder, params)
                    print(delete_order)

            # 从exchange_info中获取每个币种最小交易量
            min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in
                       exchange_info['symbols']}
            # 案例：{'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0}

            # 从exchange_info中获取每个币种下单精度
            price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in
                               exchange_info['symbols']}
            # 案例：{'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3}

            # =====获取账户信息
            # 获取账户的实际持仓
            symbol_info = update_symbol_info(exchange, symbol_list)
            symbol_last_price = fetch_binance_ticker_data(exchange)  # 获取币种的最新价格
            # 获取多头仓位
            long_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] > 0]['当前持仓量']).dropna()
            # 获取空头仓位
            short_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] < 0]['当前持仓量']).dropna()
            print('当前持仓量\n', symbol_info[symbol_info['利润'] != 0], '\n')
            print(f'多仓净值: {sum(long_position_equity):8.2f}\n')
            print(f'空仓净值: {sum(short_position_equity):8.2f}\n')
            if sum(short_position_equity) != 0:
                print(f'多空比: {abs((sum(long_position_equity) / sum(short_position_equity))):8.2f}\n')

            # 计算新的和旧的trade_usdt
            trade_usdt_old = cal_old_and_new_trade_usdt(exchange, Percent)

            if debug == 1:
                trade_usdt_old = 100

            strategy_trade_usdt = cal_strategy_trade_usdt(stratagy_list, trade_usdt_old)
            print(strategy_trade_usdt)

            if debug == 1:
                # 调试模式，直接进入下一个循环
                run_time = sleep_until_run_time('1h', if_sleep=False)
            else:
                run_time = sleep_until_run_time('1h', if_sleep=True)

            # 生成data_ready文件
            current_file = __file__
            ready_path_data = os.path.join(os.path.abspath(os.path.join(current_file, os.pardir, os.pardir)), 'data', 'k_line', '_'.join(
                ['binance_select_coin_kline_all', str(run_time).replace(':', '-'), '.csv']))
            time.sleep(2)

            for i in range(30):
                if debug == 1:
                    ready_path_data = os.path.join(os.path.abspath(os.path.join(current_file, os.pardir, os.pardir)))

                if os.path.exists(ready_path_data):
                    print('存在最新数据')
                    break
                else:
                    time.sleep(random.uniform(1, 5))
                    continue

            allsave_path = os.path.join(os.path.abspath(os.path.join(current_file, os.pardir, os.pardir)), 'data', 'k_line', '_'.join(['binance_select_coin_kline_all', '.pkl']))
            with open(allsave_path, 'rb') as f:
                symbol_candle_data = pickle.load(f)
            symbol_candle_data = symbol_candle_data.copy()
            print(len(symbol_candle_data))
            print(symbol_candle_data['BTCUSDT'].tail(2), '\n')

            select_coin = cal_factor_and_select_coin(stratagy_list, symbol_candle_data, run_time)

            twap_freestep(exchange, trade_usdt_old, symbol_info, select_coin, strategy_trade_usdt, min_qty, price_precision)

            # =====数据更新、整理、监测
            # 更新账户的实际持仓
            time.sleep(random.uniform(1, 5))
            symbol_info = update_symbol_info(exchange, symbol_list)

            # 数据监测
            equity = fetch_binance_swap_equity(exchange)  # 更新账户净值

            strategy_trade_usdt = cal_strategy_trade_usdt(stratagy_list, trade_usdt_old)
            symbol_last_price = fetch_binance_ticker_data(exchange)  # 获取币种的最新价格

            symbol_info = cal_order_amount(symbol_info, select_coin, strategy_trade_usdt)
            symbol_amount = symbol_info

            symbol_info = update_symbol_info(exchange, symbol_list)
            # 账户净值、持仓、下单信息等发送钉钉
            send_dingding_msg_every_loop(name, equity, select_coin, symbol_info, symbol_amount, symbol_last_price, dingding_id, dingding_secret)

            balance = robust(exchange.fapiPrivate_get_balance, )  # 获取账户净值
            balance = pd.DataFrame(balance)
            replenish_bnb(exchange, balance)

            if debug == 1:
                exit()

        except Exception as e:
            print(name, '程序报错中断，报错内容：', str(e), '暂停一段时间后重启，sleep时间：', '3')
            send_dingding_msg(name + '程序报错中断，报错内容：' + str(e), dingding_id, dingding_secret)
            time.sleep(3)
            continue
