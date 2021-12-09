# flake8: noqa

# 示例代码
# 从pytdx下载某一连续合约的周期K线，识别出其中枢类型，标注在图上

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
demo_05_dist = 'demo_05_dist.csv'


class DemoStrategy(FakeStrategy):
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price',
                       'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个找到的符合要求的分笔位置
        self.last_found_bi = None

        # 最后一个处理得中枢开始位置
        self.last_found_zs = None

        # 最后一个中枢得判断类型
        self.last_found_type = None

        # 如果之前存在，移除
        if os.path.exists(demo_05_dist):
            self.write_log(f'移除{demo_05_dist}')
            os.remove(demo_05_dist)

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
        if self.kline.cur_duan is None:
            return

        if self.kline.cur_bi_zs is None:
            return

        # 当前笔的start == 上一次判断过得
        if self.kline.cur_bi.start == self.last_found_bi:
            return

        # 当前笔中枢与上一个笔中枢得开始不同，可能是个新得笔中枢
        if self.kline.cur_bi_zs.start != self.last_found_zs:
            # 设置为最新判断中枢
            self.last_found_zs = self.kline.cur_bi_zs.start
            # 设置中枢得最后一笔开始时间，为最新判断时间
            self.last_found_bi = self.kline.cur_bi_zs.bi_list[-1].start
            # 设置中枢得类型为None
            self.last_found_type = None
            return

        # K线最后一笔得开始 = 中枢最后一笔得结束
        if self.kline.cur_bi.start == self.kline.cur_bi_zs.bi_list[-1].end:
            # 获得类型
            zs_type = self.kline.cur_bi_zs.get_type()

            # 记录下，这一笔已经执行过判断了
            self.last_found_bi = self.kline.cur_bi.start

            # 不一致时，才写入
            if zs_type != self.last_found_type:
                self.last_found_type = zs_type
                append_data(file_name=demo_05_dist,
                            field_names=self.dist_fieldnames,
                            dict_data={
                                'datetime': self.kline.cur_datetime,
                                'symbol': self.symbol,
                                'volume': 0,
                                'price': self.kline.cur_bi_zs.low,
                                'operation': zs_type
                            })

# 下载合约
symbol = 'RU99'
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
                            start_dt=datetime(year=2020, month=1, day=1),
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
# 暂时不显示中枢等

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
        dist_file=demo_05_dist,  # 本地dist csv文件
        dist_include_list=['close','enlarge','balance','attact', 'defend'])  # 指定输出的文字内容

sys.exit(qApp.exec_())
