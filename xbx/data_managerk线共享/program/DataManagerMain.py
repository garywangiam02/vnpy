# -*- coding: utf-8 -*-
from config.config import *
from manager.exchange import binance_exchange
from manager.multi_manager import DataManagerFather
import time


if __name__ == '__main__':
    father_manager = DataManagerFather(exchange=binance_exchange,needed_time_interval_list=needed_time_interval_list)
    while(True):
        time.sleep(60*30)