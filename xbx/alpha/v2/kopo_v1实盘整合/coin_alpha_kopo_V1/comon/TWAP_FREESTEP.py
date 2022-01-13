from coin_alpha_kopo_V1.comon.Functions import *


def twap_freestep(exchange, trade_usdt_old, symbol_info, select_coin, strategy_trade_usdt, min_qty, price_precision):
    # 新增参数
    # 单次最大下单金额
    Max_one_order_amount = 800
    # 订单间隔时间
    Twap_interval = 3

    # 这里使用本文件内的cal_order_amount函数
    symbol_info = cal_order_amount_twap_freestep(symbol_info, select_coin, strategy_trade_usdt)

    # 补全历史持仓的最新价格信息
    if symbol_info['实际下单资金'].isnull().any():
        symbol_last_price = fetch_binance_ticker_data(exchange)
        nan_symbol = symbol_info.loc[symbol_info['实际下单资金'].isnull(), '实际下单资金'].index
        symbol_info.loc[nan_symbol, '实际下单资金'] = symbol_info.loc[nan_symbol, '实际下单量'] * symbol_last_price[nan_symbol]

    # 使用twap算法拆分订单
    twap_symbol_info_list = get_twap_symbol_info_list(symbol_info, Max_one_order_amount)

    for i in range(len(twap_symbol_info_list)):

        # =====获取币种的最新价格
        symbol_last_price = fetch_binance_ticker_data(exchange)

        # =====逐批下单
        place_order_reduce_only(exchange, twap_symbol_info_list[i], symbol_last_price, min_qty, price_precision)

        if i < len(twap_symbol_info_list) - 1:
            print(f'Twap {Twap_interval} s 等待')
            time.sleep(Twap_interval)
