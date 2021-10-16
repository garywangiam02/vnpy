# flake8: noqa

# 示例代码
# 从pytdx下载某一主力合约的周期K线，识别出其5、7、9、11、13笔得信号点，标注在图上

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
from vnpy.component.cta_utility import *
from vnpy.trader.ui.kline.ui_snapshot import UiSnapshot
from vnpy.trader.utility import append_data
from vnpy.trader.ui import create_qapp

# 本示例中，输出的dist文件,主要用于图形显示一些逻辑
demo_07_dist = 'demo_07_dist.csv'

# 三买、三卖
third_signals = [ChanSignals.LI0.value, ChanSignals.LI1.value, ChanSignals.LI2.value, ChanSignals.LI3.value,
                 ChanSignals.SI0.value,ChanSignals.SI1.value, ChanSignals.SI2.value, ChanSignals.SI3.value]


class DemoStrategy(FakeStrategy):
    # 输出至csv格式的head标题
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price', 'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个执行检查的分笔结束的位置
        self.last_check_bi = None

        # 如果之前存在，移除
        if os.path.exists(demo_07_dist):
            self.write_log(f'移除{demo_07_dist}')
            os.remove(demo_07_dist)

        self.symbol = kwargs.get('symbol', 'symbol')
        self.period = kwargs.get('period', '15min')
        self.bar_freq = kwargs.get('bar_fraq', 15)
        # 创建一个15分钟bar的 kline对象
        setting = {}
        setting['name'] = f'{self.symbol}_{self.period}'
        setting['bar_interval'] = self.bar_freq
        setting['para_ma1_len'] = 55  # 双均线
        setting['para_ma2_len'] = 89
        setting['para_macd_fast_len'] = 12  # 激活macd
        setting['para_macd_slow_len'] = 26
        setting['para_macd_signal_len'] = 9
        setting['para_active_chanlun'] = True  # 激活缠论
        setting['price_tick'] = 1
        setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()
        self.kline = CtaMinuteBar(strategy=self, cb_on_bar=self.on_bar, setting=setting)

    def on_bar(self, *args, **kwargs):
        """
        重构on_bar函数，实现demo的判断逻辑
        :param args:
        :param kwargs:
        :return:
        """
        if self.kline.cur_duan is None or self.kline.cur_bi_zs is None:
            return

        cur_fx = self.kline.fenxing_list[-1]
        # 分型未结束，不做判断
        if cur_fx.is_rt:
            return

        if len(self.kline.bi_list) < 13:
            return

        # 当前分笔与上一次检查的分笔，结束时间不同，执行检查
        if self.kline.cur_bi.end != self.last_check_bi:

            for n in [5, 7, 9, 11, 13]:
                # 通过指定n笔（不限于线段），获取其分笔方法
                signal = check_chan_xt(self.kline, self.kline.bi_list[-n:])

                if signal != ChanSignals.Other.value and signal in third_signals:
                    self.last_check_bi = self.kline.cur_bi.end
                    # 写入记录
                    append_data(file_name=demo_07_dist,
                                field_names=self.dist_fieldnames,
                                dict_data={
                                    'datetime': datetime.strptime(self.kline.cur_bi.end, '%Y-%m-%d %H:%M:%S'),
                                    'symbol': self.symbol,
                                    'volume': 0,
                                    'price': self.kline.cur_price,
                                    'operation': signal
                                })


# 下载合约
symbol = 'J99'
# 下载周期
period = '15min'
# 一根bar代表的分钟数
bar_freq = int(period.replace('min', ''))

t1 = DemoStrategy(symbol=symbol, period=period, bar_fraq=bar_freq)

# 创建API对象
api_01 = TdxFutureData(strategy=t1)

# 获取某个合约得的分时数据,周期是15分钟，返回数据类型是barData
ret, bars = api_01.get_bars(symbol=symbol,
                            period=period,
                            start_dt=datetime(year=2021, month=1, day=1),
                            return_bar=True)
display_month = None
# 推送bar到kline中
for bar in bars:
    if bar.datetime.month != display_month:
        t1.write_log(f'推送:{bar.datetime.year}年{bar.datetime.month}月数据')
        display_month = bar.datetime.month
    t1.kline.add_bar(bar, bar_is_completed=True, bar_freq=bar_freq)

# 获取kline的切片数据
data = t1.kline.get_data()

snapshot = {
    'strategy': "demo",
    'datetime': datetime.now(),
    "kline_names": [t1.kline.name],
    "klines": {t1.kline.name: data}}

# 创建一个GUI界面应用app
qApp = create_qapp()

# 创建切片回放工具窗口
ui = UiSnapshot()

# 显示切片内容
ui.show(snapshot_file="",
        d=snapshot,  # 切片数据
        dist_file=demo_07_dist,  # 本地dist csv文件
        dist_include_list=[e.value for e in ChanSignals])  # 指定输出的文字内容

sys.exit(qApp.exec_())
