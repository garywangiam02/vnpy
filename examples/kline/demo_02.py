# flake8: noqa

# 示例代码
# 从pytdx下载某合约数据,生成多个周期K线，输出显示主图指标、副图指标、缠论到csv文件

import os
import sys
import json

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_common import FakeStrategy
from vnpy.data.tdx.tdx_future_data import *
from vnpy.component.cta_line_bar import get_cta_bar_type
from vnpy.trader.ui.kline.ui_snapshot import UiSnapshot
from vnpy.trader.ui import create_qapp

t1 = FakeStrategy()

# 创建API对象
api_01 = TdxFutureData(strategy=t1)

# 下载合约
symbol = 'MA99'
# 下载周期
period = '1min'
# 一根bar代表的分钟数
bar_freq = int(period.replace('min', ''))
# 输出周期
export_periods = ['M1', 'M15', 'H1', 'D1']
# 数据开始下载时间
start_dt = datetime(year=2021, month=1, day=1)

# 获取某个合约得的分时数据,周期是15分钟，返回数据类型是barData
ret, bars = api_01.get_bars(symbol=symbol,
                            period=period,
                            start_dt=start_dt,
                            return_bar=True)

klines = {}

for x_name in export_periods:

    t1.write_log(f'创建{x_name}的K线')
    # 获取K线的类、周期数
    # M15 => CtaMinuteBar, 15
    kline_class, interval_num = get_cta_bar_type(x_name)

    # 创建一个x分钟或x小时等bar的 kline对象
    setting = {}
    setting['name'] = f'{symbol}_{x_name}'
    setting['bar_interval'] = interval_num
    setting['para_ma1_len'] = 55
    setting['para_ma2_len'] = 89
    setting['para_macd_fast_len'] = 12
    setting['para_macd_slow_len'] = 26
    setting['para_macd_signal_len'] = 9
    setting['para_active_chanlun'] = True
    setting['price_tick'] = 1
    setting['underly_symbol'] = get_underlying_symbol(symbol).upper()

    # 创建对象
    kline = kline_class(strategy=t1, cb_on_bar=None, setting=setting)

    klines.update({kline.name: kline})

    # 设置输出目录
    export_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'export'))
    if not os.path.exists(export_folder):
        t1.write_log(f'创建输出目录{export_folder}')
        os.makedirs(export_folder)

    # 设置K线数据+主图指标+副图指标的输出路径
    kline.export_filename = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'export',
                                                u'{}.csv'.format(kline.name)))

    if os.path.exists(kline.export_filename):
        t1.write_log(f'移除{kline.export_filename}')
        os.remove(kline.export_filename)

    kline.export_fields = [
        {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
        {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
        {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
        {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
        {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
        {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
        {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
        {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
        {'name': 'pre_high', 'source': 'line_bar', 'attr': 'line_pre_high', 'type_': 'list'},
        {'name': 'pre_low', 'source': 'line_bar', 'attr': 'line_pre_low', 'type_': 'list'},
        {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ema1', 'type_': 'list'},
        {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ema2', 'type_': 'list'},
        {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
        {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
        {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},

    ]

    # 自动输出分笔csv
    kline.export_bi_filename = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'export',
                                                u'{}_bi.csv'.format(kline.name)))
    # 如果之前存在，移除
    if os.path.exists(kline.export_bi_filename):
        t1.write_log(f'移除{kline.export_bi_filename}')
        os.remove(kline.export_bi_filename)

    # 自动输出笔中枢csv
    kline.export_zs_filename = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'export',
                                                u'{}_zs.csv'.format(kline.name)))
    # 如果之前存在，移除
    if os.path.exists(kline.export_zs_filename):
        t1.write_log(f'移除{kline.export_zs_filename}')
        os.remove(kline.export_zs_filename)

    # 自动输出段csv
    kline.export_duan_filename = os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'export',
                                                u'{}_duan.csv'.format(kline.name)))
    # 如果之前存在，移除
    if os.path.exists(kline.export_duan_filename):
        t1.write_log(f'移除{kline.export_duan_filename}')
        os.remove(kline.export_duan_filename)

# 推送bar到所有kline中
total_bars = len(bars)
i = 0
for bar in bars:
    i += 1
    if i % 1000 == 0:
        t1.write_log(f'推送进度: {i} => {total_bars}')

    for kline_name, kline in klines.items():
        kline.add_bar(bar, bar_freq=bar_freq)

print(f'推送完毕')
