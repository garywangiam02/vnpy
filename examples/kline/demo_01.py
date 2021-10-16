# flake8: noqa

# 示例代码
# 从pytdx下载某合约数据，显示主图指标、副图指标、缠论

import os
import sys
import json

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_common import FakeStrategy
from vnpy.data.tdx.tdx_future_data import *
from vnpy.component.cta_line_bar import CtaMinuteBar
from vnpy.trader.ui.kline.ui_snapshot import UiSnapshot
from vnpy.trader.ui import create_qapp

t1 = FakeStrategy()

# 创建API对象
api_01 = TdxFutureData(strategy=t1)

# 下载合约
symbol = 'MA2109'
# 下载周期
period = '15min'
# 一根bar代表的分钟数
bar_freq = int(period.replace('min', ''))

# 获取某个合约得的分时数据,周期是15分钟，返回数据类型是barData
ret, bars = api_01.get_bars(symbol=symbol,
                            period=period,
                            start_dt=datetime(year=2021, month=1, day=1),
                            return_bar=True)

# 创建一个15分钟bar的 kline对象
setting = {}
setting['name'] = f'{symbol}_{period}'
setting['bar_interval'] = bar_freq
setting['para_ma1_len'] = 55
setting['para_ma2_len'] = 89
setting['para_macd_fast_len'] = 12
setting['para_macd_slow_len'] = 26
setting['para_macd_signal_len'] = 9
setting['para_active_chanlun'] = True
setting['price_tick'] = 1
setting['underly_symbol'] = get_underlying_symbol(symbol).upper()
kline = CtaMinuteBar(strategy=t1, cb_on_bar=None, setting=setting)

# 推送bar到kline中
for bar in bars:
    kline.add_bar(bar, bar_is_completed=True, bar_freq=bar_freq)

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
