import math

from manager.Functions import *


def clear_pos(exchange,percent = 100):
    """
    一键清仓
    percent 清仓百分比
    """
    exchange_info = robust(exchange.fapiPublic_get_exchangeinfo,)  # 获取账户净值    
    _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
    symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')] #过滤usdt合约

    # ===从exchange_info中获取每个币种最小交易量
    min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}

    # ===从exchange_info中获取每个币种下单精度
    price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in exchange_info['symbols']}

    symbol_info = update_symbol_info(exchange, symbol_list)

    # ==== 过滤实际持仓为0的币种
    symbol_info = symbol_info[symbol_info['当前持仓量'] != 0] 

    symbol_info['目标下单份数'] = 0
    symbol_info['目标下单量'] = 0
    
    # =====计算实际下单量
    symbol_info['实际下单量'] =  - symbol_info['当前持仓量']*percent/100

    # =====获取币种的最新价格
    symbol_last_price = fetch_binance_ticker_data(exchange)

    # =====逐个下单
    place_order(exchange,symbol_info, symbol_last_price, min_qty, price_precision)
