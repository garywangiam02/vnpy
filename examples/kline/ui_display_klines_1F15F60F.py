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
            "MA99 /15F":
                {
                    "data_file": "export/MA99_M15.csv",
                    "main_indicators": [
                        "ma55", "ma89"],  # ,"upper","lower"
                    "sub_indicators": [
                        "dif", 'dea'
                    ],  # "dif",'dea',, 'macd'
                    "bi_file": "export/MA99_M15_bi.csv",
                    "duan_file": "export/MA99_M15_duan.csv",
                    "bi_zs_file": "export/MA99_M15_zs.csv",

                },
            "MA99 /60F":
                {
                    "data_file": "export/MA99_H1.csv",
                    "main_indicators": [
                        "ma55", "ma89"],  # ,"upper","lower"
                    "sub_indicators": [
                        "dif", 'dea'
                    ],  # "dif",'dea',, 'macd'
                    "bi_file": "export/MA99_H1_bi.csv",
                    "duan_file": "export/MA99_H1_duan.csv",
                    "bi_zs_file": "export/MA99_H1_zs.csv",
                }
        }
        display_multi_grid(kline_settings)

    except Exception as ex:
        print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
