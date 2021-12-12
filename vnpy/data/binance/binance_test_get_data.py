# flake8: noqa
import os
import sys
import json

from vnpy.data.binance.binance_future_data import BinanceFutureData
from vnpy.trader.object import HistoryRequest, Exchange, Interval
from datetime import datetime, timedelta

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

# 创建接口
biandata = BinanceFutureData()

b_inteval = biandata.get_interval(interval="5h", interval_num=1)  # 获取周期


def init_data_from_tdx(self):
    """从币安合约始化数据"""
    try:
        from vnpy.data.binance.binance_future_data import BinanceFutureData

        # 优先从本地缓存文件，获取缓存
        last_bar_dt = self.load_klines_from_cache()

        # 创建接口
        biandata = BinanceFutureData(self)

        # 开始时间
        if last_bar_dt:
            start_dt = last_bar_dt - timedelta(days=2)
        else:
            start_dt = datetime.now() - timedelta(days=30)

        # 通达信返回得bar，datetime属性是bar的结束时间，所以不能使用callback函数自动推送Bar
        # 这里可以直接取5分钟，也可以取一分钟数据
        reg = HistoryRequest(
            symbol='BTCUSDT',
            exchange=Exchange.BINANCE,
            start=start_dt,
            end=end_date,
            interval=Interval.MINUTE,
            interval_num=1,
        )
        min1_bars = biandata.get_bars(req=reg)

        if not result:
            self.write_error(u'未能取回数据')
            return False

        for bar in min1_bars:
            if last_bar_dt and bar.datetime < last_bar_dt:
                continue
            self.cur_datetime = bar.datetime
            bar.datetime = bar.datetime - timedelta(minutes=1)
            bar.time = bar.datetime.strftime('%H:%M:%S')
            self.cur_99_price = bar.close_price
            self.kline_x.add_bar(bar, bar_freq=1)

        return True

    except Exception as ex:
        self.write_error(u'从币安数据接口初始化 Exception:{},{}'.format(str(ex), traceback.format_exc()))
        return False


start_date = "20180101"
start = datetime.strptime(start_date, '%Y%m%d')
reg = HistoryRequest(
    symbol='BTCUSDT',
    exchange=Exchange.BINANCE,
    start=start,
    end=datetime.now(),
    interval=Interval.MINUTE,
    interval_num=1,
)

bar = biandata.get_bars(
    reg
)
biandata.export_to(bars=bar,file_name="BTCUSDT_1m.csv")
