# flake8: noqa

# 示例代码
# 从pytdx下载某合约数据，识别出顶、底分型，并识别出其强弱，在UI界面上展示出来

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
from vnpy.trader.utility import append_data
from vnpy.trader.ui import create_qapp

# 本示例中，输出的dist文件,主要用于图形显示一些逻辑
demo_03_dist = 'demo_03_dist.csv'


class DemoStrategy(FakeStrategy):
    # 输出csv的head
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price',
                       'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个找到的符合要求的分型index
        self.last_found_fx = None

        # 如果之前存在，移除
        if os.path.exists(demo_03_dist):
            self.write_log(f'移除{demo_03_dist}')
            os.remove(demo_03_dist)

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
        重构on_bar函数，实现demo的分型强弱判断
        :param args:
        :param kwargs:
        :return:
        """
        # 至少要有分型
        if len(self.kline.fenxing_list) == 0:
            return

        cur_fx = self.kline.fenxing_list[-1]

        # 如果的分型已经处理过了，就不再计算
        if cur_fx.index == self.last_found_fx:
            return

        # 分型是非实时的，已经走完的
        if cur_fx.is_rt:
            return

        # 分型前x根bar
        pre_bars = [bar for bar in self.kline.line_bar[-10:] if
                    bar.datetime.strftime('%Y-%m-%d %H:%M:%S') < cur_fx.index]

        if len(pre_bars) == 0:
            return
        pre_bar = pre_bars[-1]

        # 分型后x根bar
        extra_bars = \
            [bar for bar in self.kline.line_bar[-10:] if bar.datetime.strftime('%Y-%m-%d %H:%M:%S') > cur_fx.index]

        # 分型后，有三根bar
        if len(extra_bars) < 3:
            return

        # 处理顶分型
        if cur_fx.direction == 1:
            # 顶分型后第一根bar的低点，没有超过前bar的低点
            if extra_bars[0].low_price >= pre_bar.low_price:
                return

            # 找到正确形态，第二、第三根bar，都站在顶分型之下
            if pre_bar.low_price >= extra_bars[1].high_price > extra_bars[2].high_price:
                self.last_found_fx = cur_fx.index
                append_data(file_name=demo_03_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': extra_bars[-1].datetime,
                                'symbol': self.symbol,
                                'volume': 0,
                                'price': extra_bars[-1].high_price,
                                'operation': '强顶分'
                            })

        # 处理底分型
        if cur_fx.direction == -1:
            # 底分型后第一根bar的高点，没有超过前bar的高点
            if extra_bars[0].high_price <= pre_bar.high_price:
                return

            # 找到正确形态，第二、第三根bar，都站在底分型之上
            if pre_bar.high_price <= extra_bars[1].low_price < extra_bars[2].low_price:
                self.last_found_fx = cur_fx.index
                append_data(file_name=demo_03_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': extra_bars[-1].datetime,
                                'symbol': self.symbol,
                                'volume': 0,
                                'price': extra_bars[-1].low_price,
                                'operation': '强底分'
                            })


# 下载合约
symbol = 'rb2205'
# 下载周期
period = '30min'
# 一根bar代表的分钟数
bar_freq = int(period.replace('min', ''))

t1 = DemoStrategy(symbol=symbol, period=period, bar_fraq=bar_freq)

# 创建API对象
api_01 = TdxFutureData(strategy=t1)

# 获取某个合约得的分时数据,周期是15分钟，返回数据类型是barData
ret, bars = api_01.get_bars(symbol=symbol,
                            period=period,
                            start_dt=datetime(year=2021, month=7, day=1),
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
# 暂时不显示段、中枢等
data.pop('duan_list', None)
data.pop('bi_zs_list', None)
data.pop('duan_zs_list', None)

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
        dist_file=demo_03_dist, # 本地dist csv文件
        dist_include_list=['强底分','强顶分'])  # 指定输出的文字内容

sys.exit(qApp.exec_())
