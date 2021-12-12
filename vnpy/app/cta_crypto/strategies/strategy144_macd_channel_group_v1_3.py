# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
from datetime import datetime, timedelta, time, date
import copy
import traceback
from collections import OrderedDict
from typing import Union
import numpy as np

# 然后是自己编写的模块
from vnpy.trader.utility import round_to
from vnpy.app.cta_crypto.template import CtaFutureTemplate, Direction, get_underlying_symbol, Interval
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar


########################################################################
class StrategyMacdChannelGroup_v1_3(CtaFutureTemplate):
    """数字货币CTA MACD+通道 组合策略
 v1:
    1.使用变量将MACD的快慢均线交叉点记录，然后获取上次交叉到本次交叉之间的周期数。
    2.当MACD出现顶底背离时，开多开空；
    核心计算：   1.MACD交叉状态记录
                        2.构建周期内的高低点区间
                        3.描述背离状态，同时保存结果；
    多头进场：1.最近一个MACD信号是金叉，突破周期内高点；
             2. 出现底背离时，开多；
    空头进场：1.最近一个MACD信号是死叉，突破周期内低点；
             2.出现顶背离时，开空；
    出场：移动出场
    周期：60~120分钟以上
    """
    author = u'大佳'
    # 输入参数 [ macd快均线长度_慢均线长度_信号线长度_K线周期]， 可配置多个参数
    bar_names = ['f12_s26_n9_M120']

    # 策略在外部设置的参数
    parameters = ["activate_market",
                  "max_invest_pos",
                  "max_invest_margin",
                  "max_invest_rate",
                  "bar_names",
                  "backtesting"]

    # ----------------------------------------------------------------------
    def __init__(self, cta_engine,
                 strategy_name,
                 vt_symbol,
                 setting=None):
        """Constructor"""
        super().__init__(cta_engine=cta_engine,
                         strategy_name=strategy_name,
                         vt_symbol=vt_symbol,
                         setting=setting)

        # 创建一个策略规则
        self.policy = GroupPolicy(strategy=self)

        # 仓位状态
        self.position = CtaPosition(strategy=self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        self.kline_count = len(self.bar_names)

        self.display_bars = False

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 更新监控的k线总数
            self.kline_count = len(self.bar_names)

            for bar_name in self.bar_names:
                # 创建K线
                kline_setting = {}
                para_fast_len, para_slow_len, para_signal_len, name = bar_name.split('_')
                kline_class, interval_num = get_cta_bar_type(name)
                kline_setting['name'] = bar_name

                para_fast_len = int(para_fast_len.replace('f', ''))
                para_slow_len = int(para_slow_len.replace('s', ''))
                para_signal_len = int(para_signal_len.replace('n', ''))

                kline_setting['bar_interval'] = interval_num  # K线的Bar时长
                kline_setting['para_atr1_len'] = 2 * para_fast_len  # ATR均值
                kline_setting['para_ma1_len'] = 55  # 缠论常用得第1条均线
                kline_setting['para_ma2_len'] = 89  # 缠论常用得第2条均线

                kline_setting['para_macd_fast_len'] = para_fast_len
                kline_setting['para_macd_slow_len'] = para_slow_len
                kline_setting['para_macd_signal_len'] = para_signal_len

                kline_setting['para_active_chanlun'] = True  # 激活缠论

                kline_setting['price_tick'] = self.price_tick
                kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol.split('.')[0]).upper()
                kline_setting['is_7x24'] = True
                self.write_log(f'创建K线:{kline_setting}')
                kline = kline_class(self, self.on_bar_k, kline_setting)
                self.klines.update({bar_name: kline})

            self.export_klines()

        if self.backtesting:
            # 回测时,自动初始化
            self.on_init()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

        for kline_name, kline in self.klines.items():
            # 写入文件
            import os
            kline.export_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}.csv'.format(self.strategy_name, kline_name)))

            kline.export_fields = [
                {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
                {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
                {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
                {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
                {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
                {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
                {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
                {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
                {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                {'name': f'upper', 'source': 'line_bar', 'attr': 'line_macd_chn_upper', 'type_': 'list'},
                {'name': f'lower', 'source': 'line_bar', 'attr': 'line_macd_chn_lower', 'type_': 'list'},
            ]

            kline.export_bi_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_bi.csv'.format(self.strategy_name, kline_name)))

            kline.export_zs_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_zs.csv'.format(self.strategy_name, kline_name)))

            kline.export_duan_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_duan.csv'.format(self.strategy_name, kline_name)))

    # ----------------------------------------------------------------------
    def on_init(self, force=False):
        """初始化"""
        self.write_log(u'策略初始化')

        if self.inited:
            if force:
                self.write_log(u'策略强制初始化')
                self.inited = False
                self.trading = False  # 控制是否启动交易
                self.position.pos = 0  # 仓差
                self.position.long_pos = 0  # 多头持仓
                self.position.short_pos = 0  # 空头持仓
                self.gt.up_grids = []
                self.gt.dn_grids = []
            else:
                self.write_log(u'策略初始化')
                self.write_log(u'已经初始化过，不再执行')
                return

        # 得到持久化的Policy中的子事务数据
        self.init_policy()
        self.display_tns()

        if not self.backtesting:
            self.init_position()  # 初始持仓数据

        if not self.backtesting:
            # 这里是使用gateway历史数据
            if not self.init_data():
                self.write_error(u'初始数据失败')

        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

    def init_data(self):
        """初始化数据"""
        try:
            # 优先从本地缓存文件，获取缓存
            last_bar_dt = self.load_klines_from_cache()
            dt_now = datetime.now()
            # 开始时间
            if last_bar_dt:
                load_days = max((dt_now - last_bar_dt).days, 1)
            else:
                load_days = 90
                self.display_bars = False

            def on_bar_cb(bar, **kwargs):
                """"""
                if last_bar_dt and bar.datetime < last_bar_dt:
                    return
                self.cur_price = bar.close_price
                self.cur_datetime = bar.datetime
                if self.cur_datetime > dt_now - timedelta(days=1) and not self.display_bars:
                    self.display_bars = True
                for kline in self.klines.values():
                    kline.add_bar(bar)

            self.cta_engine.load_bar(vt_symbol=self.vt_symbol,
                                     days=load_days,
                                     interval=Interval.MINUTE,
                                     callback=on_bar_cb)
            return True

        except Exception as ex:
            self.write_error(u'init_data Exception:{},{}'.format(str(ex), traceback.format_exc()))
            return False

    # ----------------------------------------------------------------------
    def on_tick(self, tick: TickData):
        """行情更新
        :type tick: object
        """
        # 实盘检查是否初始化数据完毕。如果数据未初始化完毕，则不更新tick，避免影响cur_price
        if not self.backtesting:
            if not self.inited:
                self.write_log(u'数据还没初始化完毕，不更新tick')
                return

        # 更新所有tick dict（包括 指数/主力/历史持仓合约)
        self.tick_dict.update({tick.vt_symbol: tick})

        if tick.vt_symbol == self.vt_symbol:
            self.cur_tick = tick
            self.cur_price = tick.last_price

        else:
            # 所有非vt_symbol得tick，全部返回
            return

        # 更新策略执行的时间（用于回测时记录发生的时间）
        self.cur_datetime = tick.datetime
        self.cur_price = tick.last_price

        for kline in self.klines.values():
            kline.on_tick(copy.copy(tick))

        if not self.inited or not self.trading:
            return

        self.account_pos = self.cta_engine.get_position(vt_symbol=self.vt_symbol, direction=Direction.NET)

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime, reopen=False)

        # 网格逐一止损/止盈检查
        self.grid_check_stop()

        # 实盘这里是每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute

            # 开仓逻辑处理
            self.tns_open_logic()

            self.display_grids()
            self.display_tns()

            # 事务平衡仓位
            self.tns_calcute_net_pos()

            self.put_event()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """
        分钟K线数据（仅用于回测时，从策略外部调用)
        :param bar:
        :return:
        """

        # if '201604082134' in self.cur_datetime.strftime("%Y%m%d%H%M"):
        #    a = 1

        if self.backtesting:
            new_dt = bar.datetime + timedelta(seconds=60)
            if self.cur_datetime and new_dt < self.cur_datetime:
                return
            self.cur_datetime = new_dt
            self.cur_price = bar.close_price

            if self.inited:
                self.account_pos = self.cta_engine.get_position(vt_symbol=self.vt_symbol, direction=Direction.NET)

                # 执行撤单逻辑
                self.tns_cancel_logic(bar.datetime)

                # 网格逐一止损/止盈检查
                self.grid_check_stop()

        # 推送tick到大周期K线
        try:
            for kline_name, kline in self.klines.items():
                if kline_name.endswith('M1'):
                    bar_complete = True
                else:
                    bar_complete = False
                kline.add_bar(bar=copy.copy(bar), bar_is_completed=bar_complete)

        except Exception as ex:
            self.write_error(u'{},{}'.format(str(ex), traceback.format_exc()))

        if self.inited and self.trading:

            if '201909262212' in self.cur_datetime.strftime("%Y%m%d%H%M"):
                a = 1

            # 开仓逻辑处理
            self.tns_open_logic()

            # 事务平衡仓位
            self.tns_calcute_net_pos()

        # 显示各指标信息
        self.display_tns()

    def is_macd_signal(self, kline, direction):
        """
        条件1：判断是否突破macd的通道上下轨道,突破就开仓
                'start': self.cur_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                'end': self.cur_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                'cross': self.cur_macd_cross,
                'macd_count': self.cur_macd_count,
                'max_price': self.high_array[-1],
                'min_price': self.low_array[-1],
                'max_dif': self.line_dif[-1],
                'min_dif': self.line_dif[-1],
                'macd_area': abs(self.line_macd[-1]),
                'max_macd': self.line_macd[-1],
                'min_macd': self.line_macd[-1]
        条件2： 判断是否顶背离、底背离
            :param kline: k线
            :param direction: 需要开仓的方向
            :return: True/False, 信号
        """

        if len(kline.macd_segment_list) < 3 or len(kline.bi_zs_list) == 0:
            return False, ""

        # 缠论线段
        cur_duan = kline.cur_duan
        pre_duan = kline.pre_duan
        tre_duan = kline.tre_duan

        cur_bi = kline.bi_list[-1]
        cur_zs = kline.bi_zs_list[-1]
        cur_fx = kline.fenxing_list[-1]

        # 最后三个macd的分段(排除毛刺得分段）
        tre_seg, pre_seg, cur_seg = kline.macd_segment_list[-3:]
        if abs(pre_seg['macd_count']) == 1 and len(kline.macd_segment_list) > 5:
            tre_seg, pre_seg = kline.macd_segment_list[-5:-3]

        #是否有原信号
        signal = self.policy.signals.get(kline.name, {})
        last_signal = signal.get('last_signal', None)
        signal_name = signal.get('signal_name', None)
        open_price = signal.get('open_price', None)
        duan_start = signal.get('duan_start', None)
        bi_start = signal.get('bi_start', None)
        stop_price = signal.get('stop_price')

        # 当前属于下macd的金叉，判断是否突破上一个死叉周期的最高位
        if direction == Direction.LONG:
            if cur_seg['macd_count'] > 0 \
                    and abs(pre_seg['macd_count']) >= 4 \
                    and kline.close_array[-1] > tre_seg['max_close']\
                    and pre_seg['cross'] > cur_seg['cross'] > 0:
                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": 'long_break',
                    "signal": f'{kline.name}.long',
                    "stop_price": pre_seg["min_price"]
                }
                self.save_dist(d)
                return True, 'long_break'

            # # 出现底背离
            if kline.cur_macd_count == 1 \
                    and abs(tre_seg['macd_count']) > 4 \
                    and abs(pre_seg['macd_count']) >= 4 \
                    and kline.dif_buttom_divergence\
                    and cur_bi.low < kline.bi_list[-3].low:
                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": 'buttom_div',
                    "signal": f'{kline.name}.dif_div',
                    "stop_price": pre_seg["min_price"]
                }
                self.save_dist(d)
                return True, 'buttom_div'

            #  做空突破，或顶背离失败
            if open_price and last_signal and duan_start:
                # 线段是多，持仓是空单，开仓价低于当前线段得高点(即当前价),反手做多

                # 反转信号05：顶背离失败，原有段出现新高
                if cur_duan.direction == 1 \
                        and last_signal == 'short' \
                        and open_price < cur_duan.high \
                        and stop_price and stop_price < cur_duan.high \
                        and cur_duan.start < duan_start < cur_duan.end:
                    d = {
                        "datetime": self.cur_datetime,
                        "price": kline.cur_price,
                        "operation": 'top_div_fail',
                        "signal": f'{kline.name}.long',
                        "stop_price": pre_seg["min_price"]
                    }
                    self.save_dist(d)
                    return True, 'long_break'

                # 原有段突破做空失败，出现反向的做多笔，且做多。
                #
                if cur_duan.direction == -1 \
                        and cur_bi.direction == 1 \
                        and last_signal == 'short' \
                        and signal_name in ['short_break'] \
                        and bi_start == kline.bi_list[-2].start \
                        and open_price < cur_bi.high:

                    kline.rt_count_macd()

                    if cur_bi.high > kline.bi_list[-2].high \
                            and (kline.rt_macd_count > 0 or kline.cur_macd_count > 0)\
                            and kline.cur_price > pre_seg['max_price']:
                        d = {
                            "datetime": self.cur_datetime,
                            "price": kline.cur_price,
                            "operation": 'break_fail',
                            "signal": f'{kline.name}.long',
                            "stop_price": cur_bi.low
                        }
                        self.save_dist(d)
                        return True, 'long_break'

                # 原有段突破做空失败，出现反向的做多段。
                # 反向线段的高点，高于做空的位置，当前必须是完整的上涨一笔，当前价格高于开空位置
                if cur_duan.direction == 1 \
                        and last_signal == 'short' \
                        and open_price < cur_duan.high \
                        and duan_start < cur_duan.bi_list[-1].start \
                        and kline.cur_price > open_price:
                    if cur_zs.end > cur_duan.start and cur_bi.low < cur_zs.high:
                        return False, ''

                    # 要么一根分笔直接打穿，或者三根分笔形成小线段
                    if len(cur_duan.bi_list) == 1 \
                            or (cur_bi.direction == -1 and not cur_fx.is_rt):
                        d = {
                            "datetime": self.cur_datetime,
                            "price": kline.cur_price,
                            "operation": 'short_rev',
                            "signal": f'{kline.name}.long',
                            "stop_price": pre_seg["min_price"]
                        }
                        self.save_dist(d)
                        return True, 'long_break'

        # 当前属于macd的死叉，判断是否突破上一个金叉周期的最低位
        if direction == Direction.SHORT:
            if cur_seg['macd_count'] < 0 \
                    and abs(pre_seg['macd_count']) >= 4 \
                    and kline.close_array[-1] < tre_seg['min_close']\
                    and pre_seg['cross'] < cur_seg['cross'] < 0:
                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": 'short_break',
                    "signal": f'{kline.name}.short',
                    "stop_price": float(pre_seg["max_price"])
                }
                self.save_dist(d)
                return True, 'short_break'

            # # 出现顶背离
            if kline.cur_macd_count == -1 \
                    and abs(tre_seg['macd_count']) > 4 \
                    and abs(pre_seg['macd_count']) >= 4 \
                    and kline.dif_top_divergence\
                    and cur_bi.high > kline.bi_list[-3].high:
                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": 'top_div',
                    "signal": f'{kline.name}.dif_div',
                    "stop_price": float(pre_seg["max_price"])
                }
                self.save_dist(d)
                return True, 'top_div'

            # 突破或底背离失败
            if open_price and last_signal and duan_start:

                # 底背离失败，原有段延续出现新低
                if cur_duan.direction == -1 \
                        and last_signal == 'long' \
                        and open_price > cur_duan.low \
                        and stop_price and stop_price > cur_duan.low \
                        and cur_duan.start < duan_start < cur_duan.end:
                    d = {
                        "datetime": self.cur_datetime,
                        "price": kline.cur_price,
                        "operation": 'buttom_div_fail',
                        "signal": f'{kline.name}.short',
                        "stop_price": float(pre_seg["max_price"])
                    }
                    self.save_dist(d)
                    return True, 'short_break'

                # 原有段突破做空失败，出现反向的做多笔，且做多。                #
                if cur_duan.direction == 1 \
                        and cur_bi.direction == -1 \
                        and last_signal == 'long' \
                        and signal_name in ['long_break'] \
                        and bi_start == kline.bi_list[-2].start \
                        and open_price > cur_bi.low:
                    kline.rt_count_macd()
                    if cur_bi.low < kline.bi_list[-2].low \
                            and( kline.rt_macd_count < 0  or kline.cur_macd_count < 0)\
                            and kline.cur_price < pre_seg['min_price']:
                        d = {
                            "datetime": self.cur_datetime,
                            "price": kline.cur_price,
                            "operation": 'break_fail',
                            "signal": f'{kline.name}.short',
                            "stop_price": cur_bi.high
                        }
                        self.save_dist(d)
                        return True, 'short_break'

                # 做多突破失败，反转出现做空线段
                # 反向线段的底点，低于做多的开仓价，当前必须是完整的上涨一笔，当前价格低于开多价
                if cur_duan.direction == -1 \
                        and last_signal == 'long' \
                        and open_price > cur_duan.low \
                        and duan_start <= cur_duan.bi_list[-1].start \
                        and kline.cur_price < open_price:

                    if cur_zs.end > cur_duan.start and cur_bi.high > cur_zs.low:
                        return False, ''
                    if len(cur_duan.bi_list) == 1 \
                            or (cur_bi.direction == 1 \
                                and not cur_fx.is_rt):
                        d = {
                            "datetime": self.cur_datetime,
                            "price": kline.cur_price,
                            "operation": 'long_rev',
                            "signal": f'{kline.name}.short',
                            "stop_price": float(pre_seg["max_price"])
                        }
                        self.save_dist(d)
                        return True, 'short_break'
        return False, ""

    def tns_open_logic(self):
        """
        开仓逻辑
        :return:
        """
        if self.entrust != 0:
            return

        if self.cur_datetime.strftime("%Y-%m-%d") in ['2017-10-12', '2017-10-14', '2017-12-05']:
            a = 1

        for kline_name in list(self.klines.keys()):
            kline = self.klines.get(kline_name)

            if len(kline.ma12_cross_list) < 3 or len(kline.duan_list) < 1:
                continue

            # 做多事务
            if kline_name not in self.policy.long_klines:

                # 判断1：macd金叉+突破通道, 或者是底背离
                cond01, signal_name = self.is_macd_signal(kline, direction=Direction.LONG)

                if cond01:
                    signal = self.policy.signals.get(kline_name, {})
                    if signal.get('last_signal', '') != 'long':
                        # 出现多头突破信号
                        if signal_name in ['long_break']:
                            low = kline.duan_list[-1].low
                            duan_start = kline.duan_list[-1].start
                        # 出现底背离信号,或者中枢背驰等反转信号
                        else:
                            low = kline.duan_list[-1].low
                            duan_start = kline.duan_list[-1].end

                        signal = {'last_signal': 'long',
                                  'last_signal_time': self.cur_datetime,
                                  'signal_name': signal_name,
                                  'stop_price': low,
                                  'open_price': kline.cur_price,
                                  'duan_start': duan_start,
                                  'bi_start': kline.bi_list[-1].start}

                        self.policy.signals.update({kline_name: signal})

                    if kline_name in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                        self.policy.short_klines.remove(kline_name)

                    self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                    self.policy.long_klines.append(kline_name)
                    continue

            # 做空事务
            if kline_name not in self.policy.short_klines:

                # 判断1：死叉+突破 或者顶背离
                cond01, signal_name = self.is_macd_signal(kline, direction=Direction.SHORT)

                if cond01:
                    signal = self.policy.signals.get(kline_name, {})
                    if signal.get('last_signal', '') != 'short':
                        if signal_name in ['short_break']:
                            high = kline.duan_list[-1].high
                            duan_start = kline.duan_list[-1].start
                        else:
                            high = kline.duan_list[-1].high
                            duan_start = kline.duan_list[-1].end

                        signal = {'last_signal': 'short',
                                  'last_signal_time': self.cur_datetime,
                                  'signal_name': signal_name,
                                  'stop_price': high,
                                  'open_price': kline.cur_price,
                                  'duan_start': duan_start,
                                  'bi_start': kline.bi_list[-1].start
                                  }
                        self.policy.signals.update({kline_name: signal})

                    if kline_name in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中移除:{}'.format(kline_name))
                        self.policy.long_klines.remove(kline_name)

                    self.write_log(u'从做空信号队列中增加:{}'.format(kline_name))
                    self.policy.short_klines.append(kline_name)

    def on_bar_k(self, bar, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        pass

    def tns_calcute_net_pos(self):
        """事务计算仓位轧差"""
        if not self.trading or self.entrust != 0:
            return

        self.account_pos = self.cta_engine.get_position(self.vt_symbol, direction=Direction.NET)
        if not self.account_pos:
            self.write_error(f'未能获取{self.vt_symbol}净仓')

        # 校验多一次多空信号
        long_klines = [s for s in self.policy.long_klines if s in self.bar_names]
        short_klines = [s for s in self.policy.short_klines if s in self.bar_names]

        if len(long_klines) != len(self.policy.long_klines):
            self.send_wechat(f'{self.strategy_name}多头信号校验不一致,修正{self.policy.long_klines} => {long_klines}')
            self.policy.long_klines = copy.copy(long_klines)

        if len(short_klines) != len(self.policy.short_klines):
            self.send_wechat(f'{self.strategy_name}空头信号校验不一致，修正:{self.policy.short_klines} => {short_klines}')
            self.policy.short_klines = copy.copy(short_klines)

        # 多/空/净仓分数
        long_kline_count = len(self.policy.long_klines)
        short_kline_count = len(self.policy.short_klines)
        net_kline_count = long_kline_count - short_kline_count

        if net_kline_count != self.policy.last_net_count:
            self.write_log(u'信号K线净仓变化 {} =>{}'.format(self.policy.last_net_count, net_kline_count))
            self.policy.last_net_count = net_kline_count

        # 计算目标头寸，(正数：多头， 负数：空头，0：持平）
        if self.max_invest_pos > 0:
            # 采用固定最大仓位时
            target_volume = round_to(
                value=self.max_invest_pos * net_kline_count / self.kline_count,
                target=self.volumn_tick)
            single_volume = round_to(
                value=float(self.max_invest_pos / self.kline_count),
                target=self.volumn_tick)
            max_volume = self.max_invest_pos
        else:
            # 采用资金投入百分比
            balance, avaliable, _, _ = self.cta_engine.get_account()
            invest_margin = balance * self.max_invest_rate
            if invest_margin > self.max_invest_margin > 0:
                invest_margin = self.max_invest_margin
            max_volume = round_to(
                value=invest_margin / (self.cur_price * self.margin_rate),
                target=self.volumn_tick)
            single_volume = round_to(
                value=float(max_volume / self.kline_count),
                target=self.volumn_tick)
            target_volume = round_to(
                value=max_volume * net_kline_count / self.kline_count,
                target=self.volumn_tick)

        diff_volume = target_volume - self.position.pos
        diff_volume = round(diff_volume, 7)
        single_volume = round(single_volume, 7)

        # 排除一些噪音（根据净值百分比出来的偏差）
        if abs(diff_volume) < single_volume * 0.8:
            return
        acc_volume = self.account_pos.volume if self.account_pos else 0

        self.write_log(f"{self.vt_symbol}, 账号净仓:{acc_volume},"
                       f"策略净仓:{self.position.pos}，多单:{self.position.long_pos},空单：{self.position.short_pos}\n"
                       f"目标仓位:{target_volume}，偏差仓位:{diff_volume},"
                       f"最大限仓:{max_volume}, 单次变动:{single_volume}")

        if diff_volume > 0:

            cover_volume = 0
            buy_volume = diff_volume
            if self.position.short_pos < 0:
                cover_volume = abs(self.position.short_pos)
                if cover_volume > diff_volume:
                    cover_volume = diff_volume
                    buy_volume = 0
                else:
                    buy_volume = round(round_to(diff_volume - cover_volume, self.volumn_tick), 7)

            self.write_log(f'需要增加{self.vt_symbol}仓位{diff_volume} = [平空:{cover_volume}] + 开多{buy_volume}]')

            if cover_volume > 0:
                self.write_log(f'执行 {self.vt_symbol} cover:{cover_volume}')
                ret = self.tns_process_cover(cover_volume=cover_volume)
                if ret:
                    self.write_log(f'委托平仓空单成功')
                    return
                else:
                    self.write_log(u'执行平仓失败，转移买入数量:{} => {}'.format(buy_volume, buy_volume + cover_volume))
                    buy_volume += cover_volume
                    buy_volume = round(buy_volume, 7)

            if buy_volume > 0:
                self.write_log(f'执行 {self.vt_symbol} buy:{buy_volume}')
                grid = CtaGrid(direction=Direction.LONG,
                               vt_symbol=self.vt_symbol,
                               open_price=self.cur_price,
                               close_price=sys.maxsize,
                               stop_price=0,
                               volume=buy_volume)

                ret = self.grid_buy(grid)
                if not ret:
                    self.write_error(u'执行买入仓位事务失败')

        elif diff_volume < 0:

            sell_volume = 0
            short_volume = abs(diff_volume)
            if self.position.long_pos > 0:
                sell_volume = abs(self.position.long_pos)
                if sell_volume > abs(diff_volume):
                    sell_volume = abs(diff_volume)
                    short_volume = 0
                else:
                    short_volume = abs(diff_volume) - sell_volume
                    short_volume = round(round_to(short_volume, self.volumn_tick), 7)
                self.write_log(f'需要减少{self.vt_symbol}仓位{diff_volume} = [多平:{sell_volume}] + 空开{short_volume}]')

            if sell_volume > 0:
                self.write_log(f'执行 {self.vt_symbol}sell:{sell_volume}')
                ret = self.tns_process_sell(sell_volume=sell_volume)
                if ret:
                    self.write_log(f'委托平仓多单成功')
                    return
                else:
                    self.write_log(u'执行平仓失败，转移做空数量:{} => {}'.format(short_volume, short_volume + sell_volume))
                    short_volume += sell_volume
                    short_volume = round_to(short_volume, self.volumn_tick)

            if short_volume > 0:
                self.write_log(f'执行 {self.vt_symbol} short:{short_volume}')
                grid = CtaGrid(direction=Direction.SHORT,
                               vt_symbol=self.vt_symbol,
                               open_price=self.cur_price,
                               close_price=-sys.maxsize,
                               stop_price=0,
                               volume=short_volume)

                ret = self.grid_short(grid)
                if not ret:
                    self.write_error(u'执行调整仓位事务失败')

        self.policy.save()

    def tns_process_cover(self, cover_volume):
        """事务执行平空计划"""

        # 合约得持仓信息
        if self.account_pos is None:
            self.write_error(u'当前{}合约得持仓信息获取不到'.format(self.vt_symbol))
            return False

        cover_grid = self.tns_get_grid(direction=Direction.SHORT, close_volume=cover_volume)
        if cover_grid is None:
            self.write_error(u'无法获取合适的平空网格')
            return False

        return self.grid_cover(cover_grid)

    def tns_process_sell(self, sell_volume):
        """事务执行平多计划"""

        # 合约得持仓信息
        if self.account_pos is None:
            self.write_error(u'当前{}合约得持仓信息获取不到'.format(self.vt_symbol))
            return False

        sell_grid = self.tns_get_grid(direction=Direction.LONG, close_volume=sell_volume)
        if sell_grid is None:
            self.write_error(u'无法获取合适的平多网格')
            return False

        return self.grid_sell(sell_grid)

    def tns_get_grid(self, direction, close_volume):
        """根据需要平仓的volume，选取/创建出一个grid"""

        opened_grids = self.gt.get_opened_grids(direction=direction)
        if len(opened_grids) == 0:
            self.write_error(u'当前没有{}单得网格'.format(direction))
            return None

        select_grid = None
        remove_gids = []

        for g in opened_grids:
            if g.order_status:
                self.write_log(f'该网格正在委托中，不选择:{g.__dict__}')
                continue

            if select_grid is None:
                select_grid = g
                # 恰好等于需要close的数量
                if round(select_grid.volume, 7) == close_volume:
                    self.write_log(u'选中首个网格，仓位:{}'.format(close_volume))
                    break
                # volume 大于需要close的数量
                if select_grid.volume > close_volume:
                    remain_volume = select_grid.volume - close_volume
                    remain_volume = round(remain_volume, 7)
                    select_grid.volume = close_volume
                    remain_grid = copy.copy(select_grid)
                    remain_grid.id = str(uuid.uuid1())
                    remain_grid.volume = remain_volume
                    if direction == Direction.SHORT:
                        self.gt.up_grids.append(remain_grid)
                    else:
                        self.gt.dn_grids.append(remain_grid)
                    self.write_log(u'选择首个网格，仓位超出，创建新的剩余网格:{}'.format(remain_volume))
                    break
            else:
                # 如果
                if select_grid.volume + g.volume <= close_volume:
                    old_volume = select_grid.volume
                    select_grid.volume += g.volume
                    select_grid.volume = round(select_grid.volume, 7)

                    g.volume = 0
                    remove_gids.append(g.id)
                    self.write_log(u'close_volume: {} => {}，需要移除:{}'
                                   .format(old_volume, select_grid.volume, g.__dict__))
                    if select_grid.volume == close_volume:
                        break
                elif select_grid.volume + g.volume > close_volume:
                    g.volume -= (close_volume - select_grid.volume)
                    select_grid.volume = close_volume
                    self.write_log(u'cover_volume已满足')
                    break

        if select_grid is None:
            self.write_error(u'没有可选择的{}单网格'.format(direction))
            return None

        if round(select_grid.volume, 7) != close_volume:
            self.write_error(u'没有可满足数量{}的{}单网格'.format(close_volume, direction))
            return None

        self.gt.remove_grids_by_ids(direction=direction, ids=remove_gids)

        return select_grid

    def display_tns(self):
        if not self.inited:
            return
        if self.backtesting:
            return


class GroupPolicy(CtaPolicy):
    """组合策略事务"""

    def __init__(self, strategy):
        super().__init__(strategy)

        self.signals = {}  # kline_name: { 'last_signal': '', 'last_signal_time': datetime }

        self.long_klines = []  # 做多信号得kline.name list
        self.short_klines = []  # 做空信号得kline.name list

        self.last_net_count = 0
        self.last_fund_rate = 1

    def to_json(self):
        """
        将数据转换成dict
        :return:
        """
        j = dict()
        j['create_time'] = self.create_time.strftime(
            '%Y-%m-%d %H:%M:%S') if self.create_time is not None else ""
        j['save_time'] = self.save_time.strftime('%Y-%m-%d %H:%M:%S') if self.save_time is not None else ""

        d = {}
        for kline_name, signal in self.signals.items():
            save_signal = copy.deepcopy(signal)

            last_signal_time = save_signal.get('last_signal_time', None)

            if isinstance(last_signal_time, datetime):
                save_signal.update({"last_signal_time": last_signal_time.strftime(
                    '%Y-%m-%d %H:%M:%S')})
            elif last_signal_time is None:
                save_signal.update({"last_signal_time": ""})

            d.update({kline_name: save_signal})
        j['signals'] = d

        j['long_klines'] = self.long_klines
        j['short_klines'] = self.short_klines

        j['last_net_count'] = self.last_net_count
        j['last_fund_rate'] = self.last_fund_rate
        return j

    def from_json(self, json_data):
        """
        将dict转化为属性
        :param json_data:
        :return:
        """
        if not isinstance(json_data, dict):
            return

        if 'create_time' in json_data:
            try:
                if len(json_data['create_time']) > 0:
                    self.create_time = datetime.strptime(json_data['create_time'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.create_time = datetime.now()
            except Exception as ex:
                self.create_time = datetime.now()

        if 'save_time' in json_data:
            try:
                if len(json_data['save_time']) > 0:
                    self.save_time = datetime.strptime(json_data['save_time'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.save_time = datetime.now()
            except Exception as ex:
                self.save_time = datetime.now()

        signals = json_data.get('signals', {})
        for kline_name, signal in signals.items():
            last_signal = signal.get('last_signal', "")
            str_last_signal_time = signal.get('last_signal_time', "")
            last_signal_time = None
            try:
                if len(str_last_signal_time) > 0:
                    last_signal_time = datetime.strptime(str_last_signal_time, '%Y-%m-%d %H:%M:%S')
                else:
                    last_signal_time = None
            except Exception as ex:
                last_signal_time = None
            signal.update({'last_signal_time': last_signal_time})
            self.signals.update({kline_name: signal})

        self.long_klines = json_data.get('long_klines', [])
        self.short_klines = json_data.get('short_klines', [])
        self.last_net_count = json_data.get('last_net_count', 0)
        self.last_fund_rate = json_data.get('last_fund_rate', 1)

    def clean(self):
        """
        清空数据
        :return:
        """
        self.write_log(u'清空policy数据')
        self.signals = {}
        self.long_klines = []
        self.short_klines = []
        self.last_net_count = 0
        self.last_fund_rate = 1
