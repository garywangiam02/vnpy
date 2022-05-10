# flake8: noqa
# 自动补全数字货币（币安合约）renko bar => Mongodb
# 下载的tick数据缓存 => tick_data/tdx/future
import sys, os, copy, csv, signal

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    print(f'append {vnpy_root} into sys.path')
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.renko.rebuild_crypto import *


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print(u'请输入三个参数 host symbol pricetick [ bar]')
        exit()
    print(sys.argv)
    host = sys.argv[1]

    setting = {
        "host": host,
        "db_name": CRYPTO_RENKO_DB_NAME,
        'bar_folder': os.path.join(vnpy_root,'bar_data')
    }
    builder = CryptoRenkoRebuilder(setting)

    symbol = sys.argv[2]
    price_tick = float(sys.argv[3])

    print(f'启动 Cryto bar=> renko补全,数据库:{host}/{CRYPTO_RENKO_DB_NAME} 合约:{symbol}')
    builder.start_with_bar(symbol=symbol, price_tick=price_tick, height=['K3', 'K5', 'K10'])

    print(f'exit refill {symbol} renkos')

