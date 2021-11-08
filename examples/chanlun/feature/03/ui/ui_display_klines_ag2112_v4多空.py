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

            "M3":
                {
                    "data_file": "log/FutureGrid0529_v4多空_ag2112/Future_grid_tick_ag2112_ag2112.SHFE_M3.csv",
                    "main_indicators": [

                    ],
                    "sub_indicators": [

                    ],
                    "trade_file": "log/FutureGrid0529_v4多空_ag2112/Future_grid_tick_ag2112_trade.csv",
                    "dist_file": "log/FutureGrid0529_v4多空_ag2112/Future_grid_tick_ag2112_dist.csv",
                    "bi_file": "log/FutureGrid0529_v4多空_ag2112/Future_grid_tick_ag2112_ag2112.SHFE_M3_bi.csv",
                    "duan_file": "log/FutureGrid0529_v4多空_ag2112/Future_grid_tick_ag2112_ag2112.SHFE_M3_duan.csv",
                    "bi_zs_file": "log/FutureGrid0529_v4多空_ag2112/Future_grid_tick_ag2112_ag2112.SHFE_M3_zs.csv",
                    "dist_include_list": ["buy",
                                          "sell",'short','cover'
                                          ]

                }
        }
        display_multi_grid(kline_settings)

    except Exception as ex:
        print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
