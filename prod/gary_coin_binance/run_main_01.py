# flake8: noqa

import os
import sys

# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', '..'))
sys.path.append(ROOT_PATH)
print(f'append {ROOT_PATH} into sys.path')

from vnpy.event import EventEngine                    # 事件引擎
from vnpy.trader.engine import MainEngine             # 主引擎
from vnpy.trader.ui import MainWindow, create_qapp    # 可视化界面
from vnpy.gateway.binancef import BinancefGateway     # 币安期货合约Gateway
from vnpy.app.cta_crypto import CtaCryptoApp          # 数字货币CTA引擎


def main():
    """"""
    qapp = create_qapp()
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BinancefGateway, 'binance01')  # 添加Gateway，指定名称
    main_engine.add_app(CtaCryptoApp)                      # 添加App
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    qapp.exec()


if __name__ == "__main__":
    main()
