import ccxt
import numpy as np
import time
from datetime import datetime, timedelta

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

add_count = 1  # 加钱次数
sleep_time = 3600  # 间隔时间（s）

spot = exchange.private_get_account()
print(spot)
for coin in spot['balances']:
    if coin['asset'] == 'USDT':
        spot_usdt_amount = float(coin['free'])
        print(f'现货账户USDT数量：{spot_usdt_amount}')

add_amount = np.floor(spot_usdt_amount / add_count * 100) / 100
print(f'每次加钱数量：{add_amount}')

print(f'60秒后开始划转')
time.sleep(60)

for i in range(add_count):
    while True:
        try:
            info = exchange.sapiPostFuturesTransfer(
                params={
                    'type': 1,  # 1：现货至u本位合约；2：u本位合约至现货
                    'asset': 'USDT',
                    'amount': add_amount,
                }
            )
            print(f'从现货至合约账户，划转成功：{info}，划转数量：{add_amount}，时间：{datetime.now()}')
            break
        except Exception as e:
            print(f'划转出错：{e}，60秒后重试。{datetime.now()}')
            time.sleep(60)
    print(f'{sleep_time}秒后开始下次划转。')
    time.sleep(sleep_time)

print(f'划转结束，时间{datetime.now()}')
