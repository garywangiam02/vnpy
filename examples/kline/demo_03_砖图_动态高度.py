# flake8: noqa

# 示例代码
# 从pytdx下载某合约数据，生成动态高度砖图K线，并显示

import os
import sys
import json

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_common import FakeStrategy
from vnpy.data.tdx.tdx_future_data import *
from vnpy.component.cta_renko_bar import CtaRenkoBar
from vnpy.trader.ui.kline.ui_snapshot import UiSnapshot
from vnpy.trader.ui import create_qapp
from vnpy.trader.object import BarData,RenkoBarData, TickData
from typing import Union

def on_bar(*args, **kwargs):
    pass

def bar_to_tick(bar: Union[BarData, RenkoBarData]):
    """ 通过bar计算tick数据 """

    tick = TickData(
        gateway_name='backtesting',
        symbol=bar.symbol,
        exchange=bar.exchange,
        datetime=bar.datetime
    )
    tick.date = bar.datetime.strftime("%Y-%m-%d")
    tick.time = bar.datetime.strftime("%H:%M:%S") + '.000'
    tick.trading_day = bar.trading_day
    tick.volume = bar.volume
    tick.open_interest = bar.open_interest
    tick.last_price = bar.close_price
    tick.last_volume = bar.volume
    tick.limit_up = 0
    tick.limit_down = 0
    tick.open_price = 0
    tick.high_price = 0
    tick.low_price = 0
    tick.pre_close = 0
    tick.bid_price_1 = bar.close_price
    tick.ask_price_1 = bar.close_price
    tick.bid_volume_1 = bar.volume
    tick.ask_volume_1 = bar.volume
    return tick


t1 = FakeStrategy()

# 创建API对象
api_01 = TdxFutureData(strategy=t1)

# 下载合约
symbol = 'rb2205'
# 下载周期
period = '1min'
# 一根bar代表的分钟数
bar_freq = int(period.replace('min', ''))

# 获取某个合约得的分时数据,周期是15分钟，返回数据类型是barData
ret, bars = api_01.get_bars(symbol=symbol,
                            period=period,
                            start_dt=datetime(year=2021, month=9, day=1),
                            return_bar=True)

# 创建一个千分之5波动的renko kline对象
setting = {}
setting['name'] = f'{symbol}_K5'
setting['kilo_height'] = 5    # 这里是千分之五作为一个bar高度
setting['price_tick'] = 1
#setting['height'] =  20 * setting['price_tick'] # 这里是20个跳作为一个bar高度，与千分之x不能共存哦

setting['para_boll_len'] = 22          # 布林通道线
setting['para_kdj_len'] = 9     # 激活kdj


setting['underly_symbol'] = get_underlying_symbol(symbol).upper()
kline = CtaRenkoBar(strategy=t1, cb_on_bar=on_bar, setting=setting)



# 推送bar到kline中
for bar in bars:
    tick = bar_to_tick(bar)
    kline.on_tick(tick)

# 获取kline的切片数据
data = kline.get_data()
snapshot = {
    'strategy': "demo",
    'datetime': datetime.now(),
    "kline_names":[kline.name],
    "klines": {kline.name: data}}

# 创建一个GUI界面应用app
qApp = create_qapp()

# 创建切片回放工具窗口
ui = UiSnapshot()
# 显示切片内容
ui.show(snapshot_file="",
        d=snapshot)

sys.exit(qApp.exec_())
