# -*- coding: utf-8 -*-
import ccxt

# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': '',
    'secret': '',
    'timeout': 30000,
    'rateLimit': 10,
    'hostname': 'binance.com',  # 无法fq的时候启用
    'enableRateLimit': False,
    'options': {
        'adjustForTimeDifference': True,  # ←---- resolves the timestamp
        'recvWindow': 10000,
    },
}
binance_exchange = ccxt.binance(BINANCE_CONFIG)