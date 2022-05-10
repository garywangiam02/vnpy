# flake8: noqa
"""
多周期显示K线，
时间点同步
华富资产/李来佳
"""

import sys
import os
import ctypes
import platform

system = platform.system()
os.environ["VNPY_TESTING"] = "1"

# 将repostory的目录，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_PATH)

from vnpy.trader.ui.kline.crosshair import Crosshair
from vnpy.trader.ui.kline.kline import *

if __name__ == '__main__':

    # K线界面
    try:
        kline_settings = {
            "M15": {
                # K线、主图、副图的数据文件
                "data_file": "/Users/gary/workspace/vnpy/export_BTCUSDT.BINANCE_M15.csv",
                # 主图指标
                "main_indicators": ["ma7", "ma25"],
                # 副图指标
                "sub_indicators": ["dif", "dea", "macd"],
                # 事务文件
                # "dist_file": "log/xxx/xxx_BTCUSDT.BINANCE_dist.csv",
                # 事务需要显示的标记
                # "dist_include_list": ["buy"]
                # 分笔数据文件
                "bi_file": "/Users/gary/workspace/vnpy/export_M15_bi.csv",
                # 线段数据文件
                "duan_file": "/Users/gary/workspace/vnpy/export_M15_duan.csv",
                # 中枢数据文件
                "bi_zs_file": "/Users/gary/workspace/vnpy/export_M15_zs.csv",
                # 成交记录文件
                # "trade_file": "log/xxx/xxxx_trade.csv",
            },
            # "M30": {
            #     "data_file": "export_BTCUSDT.BINANCE_M30.csv",
            #     "main_indicators": [],
            #     "sub_indicators": ["dif", "dea", "macd"],
            #     # "dist_file": "log/xxx/xxx_BTCUSDT.BINANCE_dist.csv",
            #     # "dist_include_list": ["buy"]
            # }
        }
        display_multi_grid(kline_settings)

    except Exception as ex:
        print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
