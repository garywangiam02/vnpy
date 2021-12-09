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

            "M30":
                {
                    "data_file": "/root/workspace/vnpy/log/channel_break_1203_1839_RB99/channel_break_RB99_M30_M30.csv",
                    "main_indicators": [
                        "pre_high", "pre_low","ma21"
                    ], # pre_high,pre_low,ma21,atr
                    "sub_indicators": [
                        "atr"
                    ],  # "dif",'dea',, 'macd'
                    "trade_file": "/root/workspace/vnpy/log/channel_break_1203_1839_RB99/channel_break_RB99_M30_trade.csv",
                    "dist_file": "/root/workspace/vnpy/log/channel_break_1203_1839_RB99/channel_break_RB99_M30_dist.csv",
                    "bi_file": "/root/workspace/vnpy/log/channel_break_1203_1839_RB99.SSE/channel_break_RB99_M30_M30_bi.csv",
                    "duan_file": "/root/workspace/vnpy/log/channel_break_1203_1839_RB99/channel_break_RB99_M30_M30_duan.csv",
                    "bi_zs_file": "/root/workspace/vnpy/log/channel_break_1203_1839_RB99/channel_break_RB99_M30_M30_zs.csv",
                    # "dist_include_list": ["short_break","top_div","top_div2",
                    #                       "long_break", "rev","break_fail","break_fail2","break_fail3"
                    #                       ]

                }
        }
        display_multi_grid(kline_settings)

    except Exception as ex:
        print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
