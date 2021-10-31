# flake8: noqa
"""
多周期显示K线，
时间点同步
华富资产/李来佳
"""

from vnpy.trader.ui.kline.kline import *
from vnpy.trader.ui.kline.crosshair import Crosshair
import sys
import os
import ctypes
import platform

system = platform.system()
os.environ["VNPY_TESTING"] = "1"

# 将repostory的目录，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_PATH)


if __name__ == '__main__':

    # K线界面
    try:

        kline_settings = {

            "D1":
                {
                    "data_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_D1.csv",
                    "main_indicators": [
                        "ma55", "ma89"
                    ],  # ,"upper","lower"
                    "sub_indicators": [
                        "dif", 'dea'
                    ],  # "dif",'dea',, 'macd'
                    "trade_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_trade.csv",
                    # "dist_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_s144_000651.SZSE_dist.csv",
                    "bi_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_D1_bi.csv",
                    "duan_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_D1_duan.csv",
                    "bi_zs_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_D1_zs.csv",
                    # "dist_include_list": ["short_break","top_div","top_div2",
                    #                       "long_break", "rev","break_fail","break_fail2","break_fail3"
                    #                       ]

                },
            "M30":
                {
                    "data_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_M30.csv",
                    "main_indicators": [
                        "ma55", "ma89"
                    ],  # ,"upper","lower"
                    "sub_indicators": [
                        "dif", 'dea'
                    ],  # "dif",'dea',, 'macd'
                    "trade_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_trade.csv",
                    # "dist_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_s144_000651.SZSE_dist.csv",
                    "bi_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_M30_bi.csv",
                    "duan_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_M30_duan.csv",
                    "bi_zs_file": "log/stock_3rd_buy_1024_1921_300750.SZSE/stock_S153_300750.SZSE_300750.SZSE_M30_zs.csv",
                    # "dist_include_list": ["short_break","top_div","top_div2",
                    #                       "long_break", "rev","break_fail","break_fail2","break_fail3"
                    #                       ]

                }
        }
        display_multi_grid(kline_settings)

    except Exception as ex:
        print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
