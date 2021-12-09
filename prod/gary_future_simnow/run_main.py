# flake8: noqa
import os
import sys
# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)
    print(f'append {ROOT_PATH} into sys.path')


from vnpy.event import EventEngine

from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy.gateway.ctp import CtpGateway
from vnpy.app.cta_strategy_pro import CtaStrategyProApp
from vnpy.app.algo_trading import AlgoTradingApp

def main():
    """"""
    qapp = create_qapp()
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(CtpGateway)  # 使用指定的ctp配置文件
    main_engine.add_app(CtaStrategyProApp)           # 添加增强版cta_strategy引擎
    main_engine.add_app(AlgoTradingApp)              # 使用算法引擎辅助

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
