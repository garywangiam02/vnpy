# flake8: noqa

# 示例代码
# 从pytdx下载某一主力合约的1分钟K线，生成本级别和次级别K线，识别出三买后盘整背驰信号点，标注在图上

import os
import sys
import json
import pickle, bz2
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
demo_12_dist = 'demo_12_dist.csv'


class DemoStrategy(FakeStrategy):
    # 输出至csv格式的head标题
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price', 'operation']

    def __init__(self, *args, **kwargs):

        super().__init__()

        # 最后一个执行检查的分笔结束的位置
        self.last_check_bi = None

        # 如果之前存在，移除
        if os.path.exists(demo_12_dist):
            self.write_log(f'移除{demo_12_dist}')
            os.remove(demo_12_dist)

        self.symbol = kwargs.get('symbol', 'symbol')
        self.x_period = kwargs.get('x_period', '15min')
        self.x_bar_freq = kwargs.get('x_bar_freq', 15)

        self.y_period = kwargs.get('y_period', '5min')
        self.y_bar_freq = kwargs.get('y_bar_freq', 5)

        # 创建一个15分钟bar的 kline对象
        setting = {}
        setting['name'] = f'{self.symbol}_{self.x_period}'
        setting['bar_interval'] = self.x_bar_freq
        setting['para_ma1_len'] = 55  # 双均线
        setting['para_ma2_len'] = 89
        setting['para_macd_fast_len'] = 12  # 激活macd
        setting['para_macd_slow_len'] = 26
        setting['para_macd_signal_len'] = 9
        setting['para_active_chanlun'] = True  # 激活缠论
        setting['price_tick'] = 1
        setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()
        self.kline_x = CtaMinuteBar(strategy=self, cb_on_bar=self.on_bar_x, setting=setting)

        # 创建一个5分钟bar的 kline对象
        setting = {}
        setting['name'] = f'{self.symbol}_{self.y_period}'
        setting['bar_interval'] = self.y_bar_freq
        setting['para_ma1_len'] = 55  # 双均线
        setting['para_ma2_len'] = 89
        setting['para_macd_fast_len'] = 12  # 激活macd
        setting['para_macd_slow_len'] = 26
        setting['para_macd_signal_len'] = 9
        setting['para_active_chanlun'] = True  # 激活缠论
        setting['price_tick'] = 1
        setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()
        self.kline_y = CtaMinuteBar(strategy=self, cb_on_bar=self.on_bar_y, setting=setting)

    def on_bar_x(self, *args, **kwargs):
        pass

    def on_bar_y(self, *args, **kwargs):
        """
        重构on_bar函数，实现demo的判断逻辑
        :param args:
        :param kwargs:
        :return:
        """
        if self.kline_x.cur_duan is None:
            return

        # 分型未结束，不做判断
        if self.kline_x.cur_fenxing.is_rt:
            return

        # 当前分笔与上一次检查的分笔，结束时间不同，执行检查
        if self.kline_x.cur_bi.end != self.last_check_bi:
            if self.kline_x.cur_datetime.strftime('%Y-%m-%d %H:%M') > '2021-03-04 21:00':
                a = 1
            # 判断盘整背驰
            if self.kline_x.cur_duan.direction == 1:
                # 本级别和次级别一起判断
                result01 = check_pz3bc_1st(big_kline=self.kline_x,
                                        small_kline=self.kline_y,
                                        signal_direction=Direction.SHORT)
                # 本级别判断
                result02 = check_pz3bc_1st(big_kline=self.kline_x,
                                           small_kline=None,
                                           signal_direction=Direction.SHORT)
                if result01 or result02:
                    self.last_check_bi = self.kline_x.cur_bi.end
                    # 写入记录
                    append_data(file_name=demo_12_dist,
                                field_names=self.dist_fieldnames,
                                dict_data={
                                    'datetime': datetime.strptime(self.kline_x.cur_duan.end, '%Y-%m-%d %H:%M:%S'),
                                    'symbol': self.symbol,
                                    'volume': 0,
                                    'price': self.kline_x.cur_price,
                                    'operation': '三买后盘整背驰一卖'
                                })
            else:
                # 本级别和次级别一起判断
                result01 = check_pz3bc_1st(big_kline=self.kline_x,
                                        small_kline=self.kline_y,
                                        signal_direction=Direction.LONG)
                # 本级别判断
                result02 = check_pz3bc_1st(big_kline=self.kline_x,
                                         small_kline=None,
                                         signal_direction=Direction.LONG)
                if result01 or result02:
                    self.last_check_bi = self.kline_x.cur_bi.end
                    # 写入记录
                    append_data(file_name=demo_12_dist,
                                field_names=self.dist_fieldnames,
                                dict_data={
                                    'datetime': datetime.strptime(self.kline_x.cur_duan.end, '%Y-%m-%d %H:%M:%S'),
                                    'symbol': self.symbol,
                                    'volume': 0,
                                    'price': self.kline_x.cur_price,
                                    'operation': '三卖后盘整背驰一买'
                                })


# 下载合约
symbol = 'RB99'
# 下载周期
period = '1min'
# 一根bar代表的分钟数
bar_freq = int(period.replace('min', ''))
start_date = '20210101'
end_date = datetime.now().strftime('%Y%m%d')
t1 = DemoStrategy(symbol=symbol, x_period='15min', x_bar_fraq=15, y_period='5min', y_bar_freq=5)

cache_file = f'{symbol}_{start_date}_{end_date}_{period}.pkb2'
if os.path.exists(cache_file):
    t1.write_log(f'从缓存加载数据')
    with bz2.BZ2File(cache_file, 'rb') as f:
        bars = pickle.load(f)
else:
    # 创建API对象
    api_01 = TdxFutureData(strategy=t1)

    # 获取某个合约得的分时数据,周期是1分钟，返回数据类型是barData
    ret, bars = api_01.get_bars(symbol=symbol,
                                period=period,
                                start_dt=datetime.strptime(start_date,'%Y%m%d'),
                                return_bar=True)
    if len(bars)> 0:
        t1.write_log(f'保存缓存数据')
        with bz2.BZ2File(cache_file, 'wb') as f:
            pickle.dump(bars,f)

display_month = None
# 推送bar到kline中
for bar in bars:
    if bar.datetime.month != display_month:
        t1.write_log(f'推送:{bar.datetime.year}年{bar.datetime.month}月数据')
        display_month = bar.datetime.month

    t1.kline_y.add_bar(bar, bar_freq=bar_freq)
    t1.kline_x.add_bar(bar, bar_freq=bar_freq)

t1.write_log(f'推送完毕')
# 获取kline的切片数据
x_data = t1.kline_x.get_data()
y_data = t1.kline_y.get_data()
t1.write_log(f'构造切片数据dict')
snapshot = {
    'strategy': "demo",
    'datetime': datetime.now(),
    "kline_names": [t1.kline_x.name, t1.kline_y.name],
    "klines": {t1.kline_x.name: x_data,
               t1.kline_y.name: y_data
               }
}

# 创建一个GUI界面应用app
qApp = create_qapp()

# 创建切片回放工具窗口
ui = UiSnapshot()

# 显示切片内容
ui.show(snapshot_file="",
        d=snapshot,  # 切片数据
        dist_file=demo_12_dist,  # 本地dist csv文件
        dist_include_list=['三买后盘整背驰一卖', '三卖后盘整背驰一买'])  # 指定输出的文字内容

sys.exit(qApp.exec_())
