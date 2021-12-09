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
        # 1~n根K线的设置
        kline_settings = {
            # K线名称: K线回放参数设置
            "M3":
                {
                    # k线数据文件路径，包含主图数据和副图数据
                    "data_file": "log/stock_grid_0920_1942_600410.SSE/stock_grid_600410.SSE_600410.SSE.csv",
                    # 主图指标
                    "main_indicators": [],
                    # 副图指标
                    "sub_indicators": [],
                    # 交易记录文件路径
                    "trade_file": "log/stock_grid_0920_1942_600410.SSE/stock_grid_600410.SSE_trade.csv",
                    # 策略逻辑记录文件
                    "dist_file": "log/stock_grid_0920_1942_600410.SSE/stock_grid_600410.SSE_dist.csv",
                    # 策略逻辑文件要显示的operation字段内容
                    "dist_include_list": ["buy", "sell","3 sell"],

                    # 缠论分笔文件
                    "bi_file": "log/stock_grid_0920_1942_600410.SSE/stock_grid_600410.SSE_600410.SSE_bi.csv",
                    # 缠论线段文件
                    "duan_file": "log/stock_grid_0920_1942_600410.SSE/stock_grid_600410.SSE_600410.SSE_duan.csv",
                    # 缠论笔中枢文件
                    "bi_zs_file": "log/stock_grid_0920_1942_600410.SSE/stock_grid_600410.SSE_600410.SSE_zs.csv",
                }
        }
        display_multi_grid(kline_settings)

    except Exception as ex:
        print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
