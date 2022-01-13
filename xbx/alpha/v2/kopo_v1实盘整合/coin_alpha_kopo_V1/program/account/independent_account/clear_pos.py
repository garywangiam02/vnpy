import ccxt
from coin_alpha_kopo_V1.comon.select_coin_function_twap_freestep import *

apiKey = ''
secret = ''

# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': apiKey,
    'secret': secret,
    'timeout': 3000,
    'rateLimit': 10,
    'hostname': 'binancezh.cc',  # 无法fq的时候启用 binancezh.cc
    'enableRateLimit': False}
exchange = ccxt.binance(BINANCE_CONFIG)

if __name__ == '__main__':
    # ===获取交易所相关数据
    exchange_info = robust(exchange.fapiPublic_get_exchangeinfo, )

    _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
    _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # 过滤usdt合约
    symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名
    # 从exchange_info中获取每个币种最小交易量
    min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in
               exchange_info['symbols']}

    # 从exchange_info中获取每个币种下单精度
    price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in
                       exchange_info['symbols']}
    clear_pos(exchange, min_qty, price_precision)
