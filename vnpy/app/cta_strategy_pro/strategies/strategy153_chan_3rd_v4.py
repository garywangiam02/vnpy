# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
from datetime import datetime, timedelta, time, date
import copy
import traceback
import numpy as np
from collections import OrderedDict
from typing import List

# 然后是自己编写的模块
from vnpy.trader.utility import round_to
from vnpy.app.cta_strategy_pro.template import (CtaProFutureTemplate, Direction, get_underlying_symbol, Interval, \
                                                TickData, BarData, OrderType, Offset, Status, TradeData, OrderData)
from vnpy.component.cta_policy import (
    CtaPolicy, TNS_STATUS_OBSERVATE, TNS_STATUS_READY, TNS_STATUS_ORDERING, TNS_STATUS_OPENED, TNS_STATUS_CLOSED
)
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid, LOCK_GRID
from vnpy.component.cta_line_bar import CtaMinuteBar, CtaHourBar, CtaDayBar, get_cta_bar_type
from vnpy.component.cta_utility import *
from vnpy.component.chanlun.pyChanlun import ChanDuan, ChanBi, ChanBiZhongShu, ChanDuanZhongShu, ChanFenXing
from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date, append_data

SIGNAL_THREE_BUY = '三买'  # 三买信号
SIGNAL_THREE_SHORT = '三卖'  # 三卖信号

# 开仓后，可能会形成的状态
DUAN_TREND = '线段走势'  # [做多为例] 开仓后，价格突破原高位,形成做多线段延伸
ZS_PANZHENG = '中枢盘整' # [做多为例] 开仓后，区别与原有中枢，形成新的中枢
ZS_BREAK = '回落中枢'    # [做多为例] 开仓后，价格没有突破，往下回落原有中枢

#######################################################################
class Strategy153_Chan_Three_V4(CtaProFutureTemplate):
    """
    缠论策略系列-3买、3卖策略
    三买信号：中枢突破后的三买三卖
    进场信号[做多为例]： 确定的上涨一笔，笔低点满足三买信号
    离场信号[做多为例]：
    成功 =》 形成做多线段趋势 => 线段末端分笔背驰 => 离场
    成功 =》 形成上涨中枢=》 中枢盘整背驰 =》 回落中枢 =》 离场或不处理
    失败 =》 回落中枢 =》 1、未下破中枢底部后，反抽一笔多单离场；2、下破中枢底部离场
    """

    author = u'大佳'

    bar_names = "M1-M15"  # 次级别K线，当前级别K线
    export_csv = []  # 回测时，输出到K线csv文件，全空白时，全输出；有指定时，为白名单

    win_lost_rate = 2  # 盈亏比
    single_lost_rate = None  # 单此投入亏损率, 0 ~0.1
    force_leave_times = ['1450', '0210', '2250']  # 主动离场小时+分钟

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "single_lost_rate", "win_lost_rate", "force_leave_times", "export_csv",
        "bar_names", "backtesting"]

    def __init__(self, cta_engine,
                 strategy_name,
                 vt_symbol,
                 setting=None):
        """Constructor"""
        super().__init__(cta_engine=cta_engine,
                         strategy_name=strategy_name,
                         vt_symbol=vt_symbol,
                         setting=setting)

        # 主力合约， 主力合约交易所
        self.symbol, self.exchange = extract_vt_symbol(vt_symbol)

        self.tick_dict = {}  # 记录所有onTick传入最新tick

        # 仓位状态
        self.position = CtaPosition(self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        self.policy = S153_Policy_v4(self)  # 执行策略
        self.kline_x = None  # 次级别K线
        self.kline_y = None  # 当前级别K线
        self.klines = {}
        self.init_past_3_4 = False  # 初始化经过3/4时间

        self.volume_tick = 1
        self.last_minute = None
        self.debug_dates = ["2019-12-11","2020-01-16"]
        if setting:
            self.update_setting(setting)

            volume_tick = self.cta_engine.get_volume_tick(self.vt_symbol)
            if volume_tick != self.volume_tick:
                self.volume_tick = volume_tick
                self.write_log(f'{self.vt_symbol}的最小成交数量是{self.volume_tick}')

            # bar_names: 次级别K线_本级别K线
            x_name, y_name = self.bar_names.split('-')

            # 创建X 次级别K线
            line_x_setting = {}
            kline_class, interval_num = get_cta_bar_type(x_name)
            line_x_setting['name'] = x_name  # k线名称
            line_x_setting['bar_interval'] = interval_num  # X K线得周期
            line_x_setting['para_pre_len'] = 60
            line_x_setting['para_ma1_len'] = 55
            line_x_setting['para_ma2_len'] = 89
            line_x_setting['para_macd_fast_len'] = 12
            line_x_setting['para_macd_slow_len'] = 26
            line_x_setting['para_macd_signal_len'] = 9
            line_x_setting['para_active_chanlun'] = True
            line_x_setting['para_active_chan_xt'] = True  # 激活缠论形态
            line_x_setting['para_active_skd'] = True
            line_x_setting['price_tick'] = self.price_tick  # 合约最小跳动
            line_x_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()  # 短合约
            self.kline_x = kline_class(self, self.on_bar_x, line_x_setting)
            self.kline_x.max_hold_bars = 1000
            self.klines.update({self.kline_x.name: self.kline_x})

            # 创建的Y 本级别K线
            line_y_setting = {}
            line_y_setting['name'] = y_name
            kline_class, interval_num = get_cta_bar_type(y_name)
            line_y_setting['bar_interval'] = interval_num
            line_y_setting['para_pre_len'] = 60
            line_y_setting['para_ma1_len'] = 55
            line_y_setting['para_ma2_len'] = 89
            line_y_setting['para_macd_fast_len'] = 12
            line_y_setting['para_macd_slow_len'] = 26
            line_y_setting['para_macd_signal_len'] = 9
            line_y_setting['para_active_chanlun'] = True
            line_y_setting['para_active_chan_xt'] = True   # 激活缠论形态
            line_y_setting['para_active_skd'] = True
            line_y_setting['price_tick'] = self.price_tick
            line_y_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()
            self.kline_y = kline_class(self, self.on_bar_y, line_y_setting)
            self.klines.update({self.kline_y.name: self.kline_y})

        if self.backtesting:
            # 输出K线
            self.export_klines()

            self.on_init()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

        for kline_name, kline in self.klines.items():
            if len(self.export_csv) > 0 and kline_name not in self.export_csv:
                continue

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
                {'name': 'pre_high', 'source': 'line_bar', 'attr': 'line_pre_high', 'type_': 'list'},
                {'name': 'pre_low', 'source': 'line_bar', 'attr': 'line_pre_low', 'type_': 'list'},
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},
                {'name': 'sk', 'source': 'line_bar', 'attr': 'line_sk', 'type_': 'list'},
                {'name': 'sd', 'source': 'line_bar', 'attr': 'line_sd', 'type_': 'list'},

            ]

            # 自动输出分笔csv
            kline.export_bi_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_bi.csv'.format(self.strategy_name, kline_name)))

            # 自动输出笔中枢csv
            kline.export_zs_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_zs.csv'.format(self.strategy_name, kline_name)))

            # 自动输出段csv
            kline.export_duan_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_duan.csv'.format(self.strategy_name, kline_name)))

    def on_init(self, force=False):
        """初始化"""
        self.write_log(u'策略初始化')

        if self.inited:
            if not force:
                return
            else:
                self.write_log(u'强制初始化，重置pos & grid')
                self.inited = False
                self.position.long_pos = 0
                self.position.short_pos = 0
                self.position.pos = 0
                self.pos = 0
                self.gt.up_grids = []
                self.gt.dn_grids = []
                self.entrust = 0

        self.policy.load()  # 恢复policy记录
        self.init_position()  # 初始持仓数据

        if not self.backtesting:

            if not self.init_data():
                self.write_error(f'初始化K线数据失败')

        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化完成')

    def init_data(self):
        """初始化数据"""

        """从通达信初始化数据"""
        try:
            from vnpy.data.tdx.tdx_future_data import TdxFutureData

            # 优先从本地缓存文件，获取缓存
            last_bar_dt = self.load_klines_from_cache()

            # 创建接口
            tdx = TdxFutureData(self)

            # 开始时间
            if last_bar_dt:
                start_dt = last_bar_dt - timedelta(days=2)
            else:
                start_dt = datetime.now() - timedelta(days=120)

            # 通达信返回得bar，datetime属性是bar的结束时间，所以不能使用callback函数自动推送Bar
            # 这里可以直接取5分钟，也可以取一分钟数据
            result, min1_bars = tdx.get_bars(symbol=self.idx_symbol, period='1min', callback=None, bar_freq=1,
                                             start_dt=start_dt)

            if not result:
                self.write_error(u'未能取回数据')
                return False
            total_len = len(min1_bars)
            count = 0
            for bar in min1_bars:
                if last_bar_dt and bar.datetime < last_bar_dt:
                    continue
                self.cur_datetime = bar.datetime
                bar.datetime = bar.datetime - timedelta(minutes=1)
                bar.time = bar.datetime.strftime('%H:%M:%S')
                self.cur_99_price = bar.close_price
                self.kline_y.add_bar(bar, bar_freq=1)
                self.kline_x.add_bar(bar, bar_freq=1)
                count += 1
                if count > total_len * 0.75 and not self.init_past_3_4:
                    self.init_past_3_4 = True  # 初始化经过3/4时间

            self.init_past_3_4 = True
            return True

        except Exception as ex:
            self.write_error(u'init_data_from_tdx Exception:{},{}'.format(str(ex), traceback.format_exc()))
            return False

    def on_tick(self, tick: TickData):
        """行情更新
        1、推送Tick到 X、D
        2、逻辑
        :type tick: object
        """
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited:
            return

        # 更新所有tick dict（包括 指数/主力/历史持仓合约)
        self.tick_dict.update({tick.vt_symbol: tick})

        # 只接收主力合约得价格更新 vt_symbol
        if tick.vt_symbol == self.vt_symbol:
            self.cur_mi_tick = tick
            self.cur_mi_price = tick.last_price

        if tick.vt_symbol == self.idx_symbol:
            self.cur_99_tick = tick
            self.cur_99_price = tick.last_price
            # 如果指数得tick先到达，而主力价格未到，则丢弃这个tick
            if self.cur_mi_tick is None:
                self.write_log(u'on_tick: 主力tick未到达，先丢弃当前指数tick:{},价格:{}'.format(self.vt_symbol, self.cur_99_price))
                return
        else:
            # 所有非vtSymbol得tick，全部返回
            return

        # 更新策略执行的时间（用于回测时记录发生的时间）
        self.cur_datetime = tick.datetime
        self.cur_99_price = tick.last_price
        self.cur_99_tick = tick

        self.kline_y.on_tick(copy.copy(tick))
        self.kline_x.on_tick(copy.copy(tick))

        # 4、交易逻辑

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime)

        # 检查止损
        self.grid_check_stop()

        # 实盘每分钟执行一次得逻辑
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute
            self.write_log(f'[心跳] {self.cur_datetime} process_sub_tns & check grids')
            self.display_tns()

    # ----------------------------------------------------------------------
    def on_bar(self, bar):
        """
        分钟K线数据,仅用于：
            - 回测时，从策略外部调用)
        :param bar:
        :return:
        """
        if self.backtesting:
            new_dt = bar.datetime + timedelta(seconds=60)
            if self.cur_datetime and new_dt < self.cur_datetime:
                return
            self.cur_datetime = new_dt

            self.cur_99_price = bar.close_price
            self.cur_mi_price = bar.close_price

            if self.inited:
                self.tns_cancel_logic(dt=self.cur_datetime)

                self.grid_check_stop()

        try:
            # 推送bar到当前级别K线
            self.kline_y.add_bar(bar)
            # 如果次级别为1分钟
            if self.kline_x.bar_interval == 1 and self.kline_x.interval == Interval.MINUTE:
                self.kline_x.add_bar(bar, bar_is_completed=True)
            else:
                self.kline_x.add_bar(bar)

            # 处理信号子事务，进一步发掘开仓
            self.tns_process_sub()

        except Exception as ex:
            msg = u'{},{}'.format(str(ex), traceback.format_exc())
            self.write_error(msg)
            raise Exception(msg)

    def on_bar_x(self, bar):
        """
        本级别K线on_bar事件
        :return:
        """
        if not self.backtesting:
            if self.inited:
                self.write_log(self.kline_x.get_last_bar_str())

            if not self.init_past_3_4:
                return

        # 发现信号 =》子事务
        self.tns_discover_signals()

    def on_bar_y(self, bar):
        """
        次级别K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        if not self.backtesting:
            if self.inited:
                self.write_log(self.kline_y.get_last_bar_str())

            if not self.init_past_3_4:
                return

    def is_entry_close_time(self):
        """是否进入收盘时间(更加精确得方式，是每个品种独立设置自己得收盘时间"""
        hh = self.cur_datetime.strftime('%H')
        hhmm = self.cur_datetime.strftime('%H%M')
        for _hhmm in self.force_leave_times:
            if hh == _hhmm[0:2] and hhmm >= _hhmm:
                return True

        return False

    def kline_has_xt_signals(self, kline, direction, x=1, include_5bi_xt=False, include_Q1=False):
        """
        检查K线是否具有以下缠论分笔形态信号
        :param kline:
        :param direction: Direction.LONG 买点信号， Direction.SHORT 卖点信号
        :param include_5bi_xt: 是否分析五笔形态
        :param include_Q1: 是否包含Q1形态
        :return:
        """
        # 13,11,9得趋势一类，二类、三类买卖点
        for n in range(13, 7, -2):
            x_signal = kline.get_xt_signal(f'xt_{n}_signals', x=x)
            if direction == Direction.LONG:
                if include_Q1 and x_signal.get('signal') == ChanSignals.Q1L0.value:
                    x_signal.update({'xt_source': f'xt_{n}_signals'})
                    return True, x_signal

                if x_signal.get('signal') in [ChanSignals.Q3L0.value, ChanSignals.Q2L0.value]:
                    x_signal.update({'xt_source': f'xt_{n}_signals'})
                    return True, x_signal
            if direction == Direction.SHORT:
                if include_Q1 and x_signal.get('signal') == ChanSignals.Q1S0.value:
                    x_signal.update({'xt_source': f'xt_{n}_signals'})
                    return True, x_signal

                if x_signal.get('signal') in [ChanSignals.Q3S0.value, ChanSignals.Q2S0.value]:
                    x_signal.update({'xt_source': f'xt_{n}_signals'})
                    return True, x_signal

        # 趋势二买二卖信号
        x_signal = kline.get_xt_signal(f'xt_2nd_signals', x=x)
        if direction == Direction.LONG and \
                x_signal.get('signal') == ChanSignals.Q2L0.value:
            x_signal.update({'xt_source': 'xt_2nd_signals'})
            return True, x_signal
        if direction == Direction.SHORT and \
                x_signal.get('signal') == ChanSignals.Q2S0.value:
            x_signal.update({'xt_source': 'xt_2nd_signals'})
            return True, x_signal

        # 5笔形态得类三买卖点信号
        if include_5bi_xt:
            for n in [7,5]:

                x_signal = kline.get_xt_signal(f'xt_{n}_signals', x=x)
                if direction == Direction.LONG and \
                        x_signal.get('signal') == ChanSignals.LI0.value:
                    x_signal.update({'xt_source': f'xt_{n}_signals'})
                    return True, x_signal
                if direction == Direction.SHORT and \
                        x_signal.get('signal') == ChanSignals.SI0.value:
                    x_signal.update({'xt_source': f'xt_{n}_signals'})
                    return True, x_signal

        return False,{}

    def tns_discover_signals(self):
        """事务发现信号"""

        # y K线，至少有一段
        if not self.kline_x.pre_duan:
            return

        # 临时调试
        if self.cur_datetime.strftime('%Y-%m-%d') in self.debug_dates:
            a = 1

        # 如果没有三买信号，就尝试去挖掘
        if SIGNAL_THREE_BUY not in self.policy.sub_tns:
            self.tns_discover_three_buy_signal()

        # 如果没有三卖信号，尝试挖掘
        if SIGNAL_THREE_SHORT not in self.policy.sub_tns:
            self.tns_discover_three_short_signal()

    def tns_discover_three_buy_signal(self):
        """
        发掘三买信号
        【确定性】
        （顺势）倒1笔 =》 11笔、9笔形态 =》 趋势三类买点信号
        （顺势）倒1笔 =》 7笔5笔形态 =》 三类买点信号
        （顺势）笔中枢 => 倒一笔 三类买点信号
        （逆势）倒3笔 1类卖点 =》 倒1笔 5笔形态 上颈线突破信号
        【非确定性】
        （顺势） 倒2笔 5笔形态=》三类买点
        （逆势）倒2笔 形态1类买点 =》 倒0笔 5笔形态 上颈线突破信号
        （逆势）倒2笔 形态1类买点 =》 倒0笔 下跌线段破坏+站稳金叉上方
         (逆势）倒2笔 7笔以上下跌线段macd底背驰 =》 倒0笔 5笔形态 上颈线突破信号

        :return:
        """
        if len(self.kline_y.bi_list) < 5:
            return

        # 当前是向上一笔，寻找该笔开始位置属于三类买点得信号
        if self.kline_y.cur_bi.direction == 1 and self.policy.last_sell_bi_end < self.kline_y.bi_list[-2].start:

            # 连续2上升中枢要拒绝中枢背驰
            if len(self.kline_y.bi_zs_list) >= 2:
                if self.kline_y.bi_zs_list[-2].max_high < self.kline_y.cur_bi_zs.min_low \
                        and self.kline_y.is_zs_beichi(direction=Direction.LONG, last_bi_end=self.kline_y.bi_list[-2].end):
                    return

            # 如果是趋势1卖后，形成的三卖，暂时不做多
            ret,xt_signal = self.kline_has_xt_signals(kline=self.kline_y,direction=Direction.SHORT,x=2, include_Q1=True)
            if ret and xt_signal.get('signal') == ChanSignals.Q1S0.value:
                return

            # 先寻找11笔三买信号 (参见缠论买卖点ppt 11笔类三买信号）
            xt_signal = self.kline_y.get_xt_signal('xt_11_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.Q3L0.value:
                # 中枢高点
                zg = float( min(bi.high for bi in self.kline_y.bi_list[-11:-4]))
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-11:-4]))
                if zd < self.cur_99_price < float(self.kline_y.bi_list[-2].high):
                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-11].start),
                        "zs_end": str(self.kline_y.bi_list[-4].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-2].high),
                        'success_price': float(self.kline_y.bi_list[-2].high),
                        'stop_price': min(zg, float(self.kline_y.cur_bi.low - self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_11笔_{ChanSignals.Q3L0.value}',
                        'xt_value': ChanSignals.Q3L0.value,
                        'xt_source': 'xt_11_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

            # 寻找9分笔中得三买点信号
            xt_signal = self.kline_y.get_xt_signal('xt_9_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.Q3L0.value:
                # 中枢高点
                zg = float(min(bi.high for bi in self.kline_y.bi_list[-8:-4]))
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-8:-4]))

                # 三类买点有两种，区分对待其止损位置
                # 矩形中枢后突破得三买
                if float(self.kline_y.cur_bi.low) > zg:
                    stop_price = zg
                # 三角形收敛后突破得三买
                else:
                    # 计算斜率
                    atan = (self.kline_y.bi_list[-6].high - self.kline_y.bi_list[-4].high) / (self.kline_y.bi_list[-5].bars + self.kline_y.bi_list[-4].bars - 1)
                    p = self.kline_y.bi_list[-4].high - atan * (sum([bi.bars for bi in self.kline_y.bi_list[-4:-1]])- 2)
                    stop_price = float(p)

                if stop_price < self.cur_99_price < float(self.kline_y.bi_list[-2].high):
                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-8].start),
                        "zs_end": str(self.kline_y.bi_list[-4].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-2].high),
                        'success_price': float(self.kline_y.bi_list[-2].high),
                        'stop_price': min(stop_price, float(self.kline_y.cur_bi.low -  self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_9笔_{ChanSignals.Q3L0.value}',
                        'xt_value': ChanSignals.Q3L0.value,
                        'xt_source': 'xt_9_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

            # 寻找7笔中得三类买点
            xt_signal = self.kline_y.get_xt_signal('xt_7_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.LI0.value:
                # 中枢高点
                zg = float(min(bi.high for bi in self.kline_y.bi_list[-8:-6]))
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-8:-6]))
                if zg < self.cur_99_price < float(max(self.kline_y.bi_list[-2].high, self.kline_y.bi_list[-4].high)):
                    # 7笔三买是五笔三买后回落形成得，因此，前高是止盈目标
                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-8].start),
                        "zs_end": str(self.kline_y.bi_list[-4].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(max(self.kline_y.bi_list[-2].high, self.kline_y.bi_list[-4].high)),
                        'success_price': float(max(self.kline_y.bi_list[-2].high, self.kline_y.bi_list[-4].high)),
                        'stop_price': min(zg, float(self.kline_y.cur_bi.low - self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_7笔_{ChanSignals.LI0.value}',
                        'xt_value': ChanSignals.LI0.value,
                        'xt_source': 'xt_7_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

            # 寻找5笔中得三类买点
            xt_signal = self.kline_y.get_xt_signal('xt_5_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.LI0.value:

                # 中枢高点
                zg = float(self.kline_y.bi_list[-4].high)
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-5:-4]))
                if self.kline_y.cur_bi.low > zg and self.cur_99_price < self.kline_y.bi_list[-2].high:
                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-4].start),
                        "zs_end": str(self.kline_y.bi_list[-5].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(min(self.kline_y.cur_duan.low, self.kline_y.bi_list[-5].low, self.kline_y.bi_list[-4].low)),
                        'duan_high': float(self.kline_y.bi_list[-2].high),
                        'win_price': float(self.kline_y.bi_list[-2].high),
                        'success_price': float(self.kline_y.bi_list[-2].high),
                        'stop_price': min(zg, float(self.kline_y.cur_bi.low - self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_5笔_{ChanSignals.LI0.value}',
                        'xt_value': ChanSignals.LI0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

            # 寻找cur_bi_sz的三类买点
            if self.kline_y.cur_bi_zs \
                    and self.kline_y.bi_list[-3].start == self.kline_y.cur_bi_zs.end\
                    and self.kline_y.cur_bi.low > self.kline_y.cur_bi_zs.high:
                long_tns = {
                    "status": TNS_STATUS_OBSERVATE,
                    "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    "zs_start": str(self.kline_y.cur_bi_zs.start),
                    "zs_end": str(self.kline_y.cur_bi_zs.end),
                    "zs_high": float(self.kline_y.cur_bi_zs.high),
                    "zs_low": float(self.kline_y.cur_bi_zs.low),
                    "zs_height": float(self.kline_y.cur_bi_zs.height),
                    "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                    'duan_low': float(self.kline_y.cur_duan.low),
                    'duan_high': float(self.kline_y.cur_duan.high),
                    'win_price': float(self.kline_y.bi_list[-2].high),
                    'success_price': float(self.kline_y.bi_list[-2].high),
                    'stop_price': min(float(self.kline_y.cur_bi_zs.high), float(self.kline_y.cur_bi.low - self.kline_y.bi_height_ma())),
                    'duan_start': self.kline_y.cur_duan.start,
                    'bi_start': self.kline_y.cur_bi.start,
                    'signal_name': f'倒1_中枢_三买',
                    'xt_value': ChanSignals.LI0.value,
                    'xt_source': 'xt_5_signals'
                }

                self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": long_tns['signal_name'],
                    "signal": SIGNAL_THREE_BUY
                }
                self.save_dist(d)
                self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                return

            # 寻找一买后出现的上颈线突破
            ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y,
                                                       direction=Direction.LONG,
                                                       x=3,
                                                       include_Q1=True)
            if ret and xt_signal.get('signal') == ChanSignals.Q1L0.value:
                b_signal = self.kline_y.get_xt_signal('xt_5_signals', x=1)
                if b_signal.get('signal') == ChanSignals.LG0.value \
                        and self.kline_y.cur_duan.direction ==1 \
                        and len(self.kline_y.cur_duan.bi_list) ==1\
                        and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start:
                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.cur_bi_zs.start),
                        "zs_end": str(self.kline_y.cur_bi_zs.end),
                        "zs_high": float(self.kline_y.cur_bi_zs.high),
                        "zs_low": float(self.kline_y.cur_bi_zs.low),
                        "zs_height": float(self.kline_y.cur_bi_zs.height),
                        "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.cur_bi.high),
                        'success_price': float(self.kline_y.cur_bi.high),
                        'stop_price': float(self.kline_y.bi_list[-3].low),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒3_1买_{ChanSignals.LG0.value}',
                        'xt_value': ChanSignals.LG0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

        # 如果是向下一笔，寻找前一下跌笔末端属于五类买点，并且当前形成中枢得信号
        else:
            if check_bi_not_rt(self.kline_y,Direction.SHORT):

                # 寻找5笔中得三类买点
                xt_signal = self.kline_y.get_xt_signal('xt_5_signals', x=2)
                # 存在三类买点，且该买点前后不存在多单平仓信号
                if xt_signal.get('signal') == ChanSignals.LI0.value\
                        and self.policy.last_sell_bi_end < self.kline_y.bi_list[-3].end\
                        and self.kline_y.cur_bi.low < self.kline_y.bi_list[-2].low\
                        and self.cur_99_price > self.kline_y.bi_list[-2].low\
                        and self.kline_y.cur_bi.low > self.kline_y.bi_list[-5].high:

                    # 中枢高点
                    zg = float(self.kline_y.bi_list[-5].high)
                    # 中枢低点
                    zd = float(max(bi.low for bi in self.kline_y.bi_list[-6:-5]))

                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-5].start),
                        "zs_end": str(self.kline_y.bi_list[-6].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(min(self.kline_y.cur_duan.low, self.kline_y.bi_list[-6].low,
                                              self.kline_y.bi_list[-5].low)),
                        'duan_high': float(self.kline_y.bi_list[-3].high),
                        'win_price': float(self.kline_y.bi_list[-3].high),
                        'success_price': float(self.kline_y.bi_list[-3].high),
                        'stop_price': min(zg, float(self.kline_y.cur_bi.low - self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒2_5笔_{ChanSignals.LI0.value}',
                        'xt_value': ChanSignals.LI0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

                # 寻找一买后出现的抄底信号：上颈线突破，或者下跌线段被打破+站稳金叉上方
                ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y,
                                                          direction=Direction.LONG,
                                                          x=2,
                                                          include_Q1=True)
                if ret and xt_signal.get('signal') == ChanSignals.Q1L0.value:
                    # 上颈线突破，
                    b_signal = self.kline_y.get_xt_signal('xt_5_signals',x=0)
                    if b_signal.get('signal') == ChanSignals.LG0.value \
                            and self.kline_y.cur_duan.direction == 1 \
                            and len(self.kline_y.cur_duan.bi_list) == 1 \
                            and self.kline_y.cur_duan.end == self.kline_y.bi_list[-1].start:

                        long_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.cur_bi_zs.start),
                            "zs_end": str(self.kline_y.cur_bi_zs.end),
                            "zs_high": float(self.kline_y.cur_bi_zs.high),
                            "zs_low": float(self.kline_y.cur_bi_zs.low),
                            "zs_height": float(self.kline_y.cur_bi_zs.height),
                            "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.cur_bi.high),
                            'success_price': float(self.kline_y.cur_bi.high),
                            'stop_price': float(max(self.kline_y.bi_list[-2].low, self.kline_y.cur_bi.low - self.kline_y.bi_height_ma(60))),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_1买_{ChanSignals.LG0.value}',
                            'xt_value': ChanSignals.LG0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": long_tns['signal_name'],
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                        return

                    # 下跌线段被打破+站稳金叉上方(趋势二买）
                    if self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start \
                        and self.kline_y.cur_bi.low > self.kline_y.cur_duan.second_low\
                        and self.cur_99_price > self.kline_y.cur_bi.low > max(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1]):
                        long_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.cur_bi_zs.start),
                            "zs_end": str(self.kline_y.cur_bi_zs.end),
                            "zs_high": float(self.kline_y.cur_bi_zs.high),
                            "zs_low": float(self.kline_y.cur_bi_zs.low),
                            "zs_height": float(self.kline_y.cur_bi_zs.height),
                            "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.cur_bi.high),
                            'success_price': float(self.kline_y.cur_bi.high),
                            'stop_price': float(max(self.kline_y.bi_list[-2].low, self.kline_y.cur_bi.low - self.kline_y.bi_height_ma(60))),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_1买_{ChanSignals.Q2L0.value}',
                            'xt_value': ChanSignals.Q2L0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": long_tns['signal_name'],
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                        return

                # 寻找下跌线段末端得macd底背离（一买点）后，出现得上颈线突破买点信号
                if self.kline_y.cur_duan and self.kline_y.cur_duan.direction == -1 \
                    and len(self.kline_y.cur_duan.bi_list) >= 7 \
                    and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start \
                    and self.kline_y.is_fx_macd_divergence(direction=Direction.SHORT,
                                                               cur_duan=self.kline_y.cur_duan)\
                    and self.kline_y.cur_bi.low > max(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1]):

                    b_signal = self.kline_y.get_xt_signal('xt_5_signals', x=0)
                    if b_signal.get('signal') == ChanSignals.LG0.value:
                        # 中枢高点
                        zg = float(self.kline_y.bi_list[-4].high)
                        # 中枢低点
                        zd = float(self.kline_y.bi_list[-4].low)

                        long_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.bi_list[-4].start),
                            "zs_end": str(self.kline_y.bi_list[-2].end),
                            "zs_high": zg,
                            "zs_low": zd,
                            "zs_height": zg - zd,
                            "zs_middle": (zg - zd) / 2,
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.cur_bi.high),
                            'success_price': float(self.kline_y.cur_bi.high),
                            'stop_price':  min(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1], self.kline_y.cur_bi.low - self.kline_y.bi_height_ma(60)),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_段背驰_{ChanSignals.LG0.value}',
                            'xt_value': ChanSignals.LG0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": long_tns['signal_name'],
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                        return

                    if self.kline_y.cur_bi.low > self.kline_y.cur_duan.bi_list[-2].low\
                        and max(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1]) < self.kline_y.cur_bi.low:
                        long_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.cur_bi_zs.start),
                            "zs_end": str(self.kline_y.cur_bi_zs.end),
                            "zs_high": float(self.kline_y.cur_bi_zs.high),
                            "zs_low": float(self.kline_y.cur_bi_zs.low),
                            "zs_height": float(self.kline_y.cur_bi_zs.height),
                            "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.cur_bi.high),
                            'success_price': float(self.kline_y.cur_bi.high),
                            'stop_price': max(float(self.kline_y.bi_list[-2].low), self.kline_y.cur_bi.low - self.kline_y.bi_height_ma(60)),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_段背驰_{ChanSignals.Q2L0.value}',
                            'xt_value': ChanSignals.Q2L0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": long_tns['signal_name'],
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                        return

                # # (逆势）倒2笔、倒1笔超长分笔v反（小转大），站稳金叉上方
                if self.kline_y.bi_list[-3].height >  self.kline_y.bi_list[-2].height > 3 * self.kline_y.bi_height_ma(60)\
                    and self.kline_y.bi_list[-3].end == self.kline_y.cur_duan.end\
                    and self.kline_y.cur_bi.high > max(self.kline_y.line_ma1[-1],self.kline_y.line_ma2[-1])\
                    and self.kline_y.bi_list[-3].atan < self.kline_y.bi_list[-2].atan \
                    and self.kline_y.bi_list[-3].height * 0.819 < self.kline_y.bi_list[-2].height:
                    long_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.cur_bi_zs.start),
                        "zs_end": str(self.kline_y.cur_bi_zs.end),
                        "zs_high": float(self.kline_y.cur_bi_zs.high),
                        "zs_low": float(self.kline_y.cur_bi_zs.low),
                        "zs_height": float(self.kline_y.cur_bi_zs.height),
                        "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.cur_bi.high),
                        'success_price': float(self.kline_y.cur_bi.high),
                        'stop_price': float(
                            max(self.kline_y.bi_list[-2].low, self.kline_y.cur_bi.low - self.kline_y.bi_height_ma(60))),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒2_小转大_{ChanSignals.Q2L0.value}',
                        'xt_value': ChanSignals.Q2L0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_BUY] = long_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": long_tns['signal_name'],
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_BUY, d.get('operation')))
                    return

    def tns_discover_three_short_signal(self):
        """
        发掘x三卖信号
        发掘三买信号
        【确定性】
        （顺势）倒1笔 =》 11笔、9笔形态 =》 趋势三类卖点信号
        （顺势）倒1笔 =》 7笔5笔形态 =》 三类卖点信号
        （顺势）笔中枢 => 倒一笔 三类卖点信号
        （逆势）倒3笔 1类卖点 =》 倒1笔 5笔形态 下颈线突破信号
        【非确定性】
        （顺势） 倒2笔 5笔形态=》三类卖点
        （逆势）倒2笔 形态1类卖点 =》 倒0笔 5笔形态 下颈线突破信号
        （逆势）倒2笔 形态1类卖点 =》 倒0笔 上涨线段破坏+站稳死叉下方
         (逆势）倒2笔 7笔以上上涨线段macd底背驰 =》 倒0笔 5笔形态 下颈线突破信号
         (逆势）倒2笔、倒1笔超长分笔v反（小转大），站稳死叉下方
        :return:
        """
        if len(self.kline_y.bi_list) < 5:
            return
        if self.kline_y.cur_bi.direction == -1 and self.policy.last_cover_bi_end < self.kline_y.bi_list[-2].start:

            # 连续2下跌中枢要拒绝中枢背驰
            if len(self.kline_y.bi_zs_list) >= 2:
                if self.kline_y.bi_zs_list[-2].min_low > self.kline_y.cur_bi_zs.max_high \
                        and self.kline_y.is_zs_beichi(direction=Direction.SHORT,
                                                      last_bi_end=self.kline_y.bi_list[-2].end):
                    return

            # 如果是趋势买后，形成的三卖，暂时不做空
            ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y, direction=Direction.LONG, x=2,
                                                       include_Q1=True)
            if ret and xt_signal.get('signal') == ChanSignals.Q1L0.value:
                return

            # 先寻找11笔三卖信号 (参见缠论买卖点ppt 11笔类三卖信号）
            xt_signal = self.kline_y.get_xt_signal('xt_11_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.Q3S0.value:
                # 中枢高点
                zg = float(min(bi.high for bi in self.kline_y.bi_list[-11:-4]))
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-11:-4]))
                if float(self.kline_y.bi_list[-2].low) < self.cur_99_price < zd:
                    short_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-11].start),
                        "zs_end": str(self.kline_y.bi_list[-4].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-2].low),
                        'success_price': float(self.kline_y.bi_list[-2].low),
                        'stop_price': max(zd,float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_11笔_{ChanSignals.Q3S0.value}',
                        'xt_value': ChanSignals.Q3S0.value,
                        'xt_source': 'xt_11_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": short_tns['signal_name'],
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                    return

            # 寻找9分笔中得三卖点信号
            xt_signal = self.kline_y.get_xt_signal('xt_9_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.Q3S0.value:
                # 中枢高点
                zg = float(min(bi.high for bi in self.kline_y.bi_list[-8:-4]))
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-8:-4]))

                # 三类卖点有两种，区分对待其止损位置
                # 矩形中枢后突破得三卖
                if float(self.kline_y.bi_list[-2].high) < zd:
                    stop_price = zd
                # 三角形收敛后突破得三卖
                else:
                    # 计算斜率
                    atan = (self.kline_y.bi_list[-4].low - self.kline_y.bi_list[-4].low) / (
                                self.kline_y.bi_list[-5].bars + self.kline_y.bi_list[-4].bars - 1)
                    p = self.kline_y.bi_list[-4].low + atan * (sum([bi.bars for bi in self.kline_y.bi_list[-4:-1]]) - 2)
                    stop_price = float(p)

                if float(self.kline_y.bi_list[-2].low) < self.cur_99_price < stop_price:
                    short_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-8].start),
                        "zs_end": str(self.kline_y.bi_list[-4].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-2].low),
                        'success_price': float(self.kline_y.bi_list[-2].low),
                        'stop_price': max(stop_price,float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_9笔_{ChanSignals.Q3S0.value}',
                        'xt_value': ChanSignals.Q3S0.value,
                        'xt_source': 'xt_9_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": short_tns['signal_name'],
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                    return

            # 寻找7笔中得三类卖点
            xt_signal = self.kline_y.get_xt_signal('xt_7_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.SI0.value:
                # 中枢高点
                zg = float(min(bi.high for bi in self.kline_y.bi_list[-8:-6]))
                # 中枢低点
                zd = float(max(bi.low for bi in self.kline_y.bi_list[-8:-6]))

                if float(min(self.kline_y.bi_list[-2].low, self.kline_y.bi_list[-4].low)) < self.cur_99_price < zd:
                    # 7笔三卖是五笔三卖后回抽形成得，因此，前低是止盈目标
                    short_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-8].start),
                        "zs_end": str(self.kline_y.bi_list[-4].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(min(self.kline_y.bi_list[-2].low, self.kline_y.bi_list[-4].low)),
                        'success_price': float(min(self.kline_y.bi_list[-2].low, self.kline_y.bi_list[-4].low)),
                        'stop_price': max(zd,float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒1_7笔_{ChanSignals.SI0.value}',
                        'xt_value': ChanSignals.SI0.value,
                        'xt_source': 'xt_7_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": short_tns['signal_name'],
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                    return

            # 寻找5笔中得三类卖点
            xt_signal = self.kline_y.get_xt_signal('xt_5_signals', x=1)
            if xt_signal.get('signal') == ChanSignals.SI0.value:
                # 中枢高点
                zg = float(min([bi.high for bi in self.kline_y.bi_list[-5:-4]]))
                # 中枢低点
                zd = float(self.kline_y.bi_list[-4].low)
                short_tns = {
                    "status": TNS_STATUS_OBSERVATE,
                    "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    "zs_start": str(self.kline_y.bi_list[-4].start),
                    "zs_end": str(self.kline_y.bi_list[-5].end),
                    "zs_high": zg,
                    "zs_low": zd,
                    "zs_height": zg - zd,
                    "zs_middle": (zg - zd) / 2,
                    'duan_low': float(
                        min(self.kline_y.cur_duan.low, self.kline_y.bi_list[-5].low, self.kline_y.bi_list[-4].low)),
                    'duan_high': float(self.kline_y.cur_duan.high),
                    'win_price': float(self.kline_y.bi_list[-2].low),
                    'success_price': float(self.kline_y.bi_list[-2].low),
                    'stop_price': max(zd,float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                    'duan_start': self.kline_y.cur_duan.start,
                    'bi_start': self.kline_y.cur_bi.start,
                    'signal_name': f'倒1_5笔_{ChanSignals.SI0.value}',
                    'xt_value': ChanSignals.SI0.value,
                    'xt_source': 'xt_5_signals'
                }

                self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": short_tns['signal_name'],
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)
                self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                return

            # 寻找中枢三卖
            if self.kline_y.cur_bi_zs \
                    and self.kline_y.bi_list[-3].start == self.kline_y.cur_bi_zs.end \
                    and self.kline_y.cur_bi.high < self.kline_y.cur_bi_zs.low:
                short_tns = {
                    "status": TNS_STATUS_OBSERVATE,
                    "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    "zs_start": str(self.kline_y.cur_bi_zs.start),
                    "zs_end": str(self.kline_y.cur_bi_zs.end),
                    "zs_high": float(self.kline_y.cur_bi_zs.high),
                    "zs_low": float(self.kline_y.cur_bi_zs.low),
                    "zs_height": float(self.kline_y.cur_bi_zs.height),
                    "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                    'duan_low': float(self.kline_y.cur_duan.low),
                    'duan_high': float(self.kline_y.cur_duan.high),
                    'win_price': float(self.kline_y.bi_list[-2].low),
                    'success_price': float(self.kline_y.bi_list[-2].low),
                    'stop_price': max(self.kline_y.cur_bi_zs.low, float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                    'duan_start': self.kline_y.cur_duan.start,
                    'bi_start': self.kline_y.cur_bi.start,
                    'signal_name': f'倒1_中枢_三卖',
                    'xt_value': ChanSignals.SI0.value,
                    'xt_source': 'xt_5_signals'
                }

                self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": short_tns['signal_name'],
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)
                self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                return

            # 寻找一卖后出现的下颈线突破
            ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y,
                                                       direction=Direction.SHORT,
                                                       x=3,
                                                       include_Q1=True)
            if ret and xt_signal.get('signal') == ChanSignals.Q1S0.value:
                b_signal = self.kline_y.get_xt_signal('xt_5_signals', x=1)
                if b_signal.get('signal') == ChanSignals.SG0.value \
                        and self.kline_y.cur_duan.direction == -1 \
                        and len(self.kline_y.cur_duan.bi_list) == 1 \
                        and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start:
                    short_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.cur_bi_zs.start),
                        "zs_end": str(self.kline_y.cur_bi_zs.end),
                        "zs_high": float(self.kline_y.cur_bi_zs.high),
                        "zs_low": float(self.kline_y.cur_bi_zs.low),
                        "zs_height": float(self.kline_y.cur_bi_zs.height),
                        "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-2].low),
                        'success_price': float(self.kline_y.bi_list[-2].low),
                        'stop_price': max(self.kline_y.cur_bi_zs.low,
                                          float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒3_1卖_{ChanSignals.SG0.value}',
                        'xt_value': ChanSignals.SG0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": short_tns['signal_name'],
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                    return

        # 如果是向上一笔，寻找前一上涨笔末端属于五类卖点，并且当前形成中枢得信号，或者下颈线突破点
        else:
            if check_bi_not_rt(self.kline_y, Direction.LONG):

                # 寻找5笔中得三类卖点
                xt_signal = self.kline_y.get_xt_signal('xt_5_signals', x=2)
                if xt_signal.get('signal') == ChanSignals.SI0.value\
                        and self.policy.last_cover_bi_end < self.kline_y.bi_list[-3].end\
                        and self.kline_y.cur_bi.high > self.kline_y.bi_list[-2].high\
                        and self.cur_99_price < self.kline_y.bi_list[-2].high\
                        and self.kline_y.cur_bi.high < self.kline_y.bi_list[-5].low:

                    # 中枢高点
                    zg = float(min([bi.high for bi in self.kline_y.bi_list[-6:-5]]))
                    # 中枢低点
                    zd = float(self.kline_y.bi_list[-5].low)

                    short_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.bi_list[-5].start),
                        "zs_end": str(self.kline_y.bi_list[-6].end),
                        "zs_high": zg,
                        "zs_low": zd,
                        "zs_height": zg - zd,
                        "zs_middle": (zg - zd) / 2,
                        'duan_low': float(
                            min(self.kline_y.cur_duan.low, self.kline_y.bi_list[-6].low, self.kline_y.bi_list[-6].low)),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-3].low),
                        'success_price': float(self.kline_y.bi_list[-3].low),
                        'stop_price': max(zd, float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒2_5笔_{ChanSignals.SI0.value}',
                        'xt_value': ChanSignals.SI0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": short_tns['signal_name'],
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                    return

                # 寻找一卖后, 出现的下颈线突破，或者上涨线段打破，站稳死叉下方
                ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y,
                                                           direction=Direction.SHORT,
                                                           x=2,
                                                           include_Q1=True)
                if ret and xt_signal.get('signal') == ChanSignals.Q1S0.value:
                    # 出现的下颈线突破
                    b_signal = self.kline_y.get_xt_signal('xt_5_signals', x=0)
                    if b_signal.get('signal') == ChanSignals.SG0.value \
                            and self.kline_y.cur_duan.direction == -1 \
                            and len(self.kline_y.cur_duan.bi_list) == 1 \
                            and self.kline_y.cur_duan.end == self.kline_y.bi_list[-1].start:
                        short_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.cur_bi_zs.start),
                            "zs_end": str(self.kline_y.cur_bi_zs.end),
                            "zs_high": float(self.kline_y.cur_bi_zs.high),
                            "zs_low": float(self.kline_y.cur_bi_zs.low),
                            "zs_height": float(self.kline_y.cur_bi_zs.height),
                            "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.bi_list[-2].low),
                            'success_price': float(self.kline_y.bi_list[-2].low),
                            'stop_price': max(self.kline_y.cur_bi_zs.low,
                                              float(self.kline_y.cur_bi.high + self.kline_y.bi_height_ma())),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_1卖_{ChanSignals.SG0.value}',
                            'xt_value': ChanSignals.SG0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": short_tns['signal_name'],
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                        return

                    # 上涨线段打破，站稳死叉下方，（趋势2卖）
                    if self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start \
                        and self.kline_y.cur_bi.high < self.kline_y.cur_duan.second_high\
                        and self.cur_99_price < self.kline_y.cur_bi.high < min(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1]):
                        short_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.cur_bi_zs.start),
                            "zs_end": str(self.kline_y.cur_bi_zs.end),
                            "zs_high": float(self.kline_y.cur_bi_zs.high),
                            "zs_low": float(self.kline_y.cur_bi_zs.low),
                            "zs_height": float(self.kline_y.cur_bi_zs.height),
                            "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.bi_list[-2].low),
                            'success_price': float(self.kline_y.bi_list[-2].low),
                            'stop_price': float(self.kline_y.bi_list[-2].high),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_1卖_{ChanSignals.Q2S0.value}',
                            'xt_value': ChanSignals.Q2S0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": short_tns['signal_name'],
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                        return

                # 寻找上涨线段末端macd背驰（一类卖点）后，产生得下颈线突破买点
                if self.kline_y.cur_duan and self.kline_y.cur_duan.direction == 1 \
                    and len(self.kline_y.cur_duan.bi_list) >= 7 \
                    and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start \
                    and self.kline_y.is_fx_macd_divergence(direction=Direction.LONG,cur_duan=self.kline_y.cur_duan)\
                    and self.kline_y.cur_bi.high < min(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-2]):

                    b_signal = self.kline_y.get_xt_signal('xt_5_signals', x=0)
                    if b_signal.get('signal') == ChanSignals.SG0.value:
                        # 中枢高点
                        zg = float(self.kline_y.bi_list[-4].high)
                        # 中枢低点
                        zd = float(self.kline_y.bi_list[-4].low)

                        short_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.bi_list[-4].start),
                            "zs_end": str(self.kline_y.bi_list[-2].end),
                            "zs_high": zg,
                            "zs_low": zd,
                            "zs_height": zg - zd,
                            "zs_middle": (zg - zd) / 2,
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.bi_list[-1].low),
                            'success_price': float(self.kline_y.bi_list[-1].low),
                            'stop_price': max(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1], self.kline_y.cur_bi.high + self.kline_y.bi_height_ma(60)),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_段背驰_{ChanSignals.SG0.value}',
                            'xt_value': ChanSignals.SG0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": short_tns['signal_name'],
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                        return
                    if self.kline_y.cur_bi.high > self.kline_y.cur_duan.bi_list[-2].high\
                            and min(self.kline_y.line_ma1[-1], self.kline_y.line_ma2[-1]) > self.kline_y.cur_bi.high:
                        short_tns = {
                            "status": TNS_STATUS_OBSERVATE,
                            "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            "zs_start": str(self.kline_y.cur_bi_zs.start),
                            "zs_end": str(self.kline_y.cur_bi_zs.end),
                            "zs_high": float(self.kline_y.cur_bi_zs.high),
                            "zs_low": float(self.kline_y.cur_bi_zs.low),
                            "zs_height": float(self.kline_y.cur_bi_zs.height),
                            "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                            'duan_low': float(self.kline_y.cur_duan.low),
                            'duan_high': float(self.kline_y.cur_duan.high),
                            'win_price': float(self.kline_y.bi_list[-2].low),
                            'success_price': float(self.kline_y.bi_list[-2].low),
                            'stop_price': float(min(self.kline_y.bi_list[-2].high, self.kline_y.cur_bi.high + self.kline_y.bi_height_ma(60))),
                            'duan_start': self.kline_y.cur_duan.start,
                            'bi_start': self.kline_y.cur_bi.start,
                            'signal_name': f'倒2_段背驰_{ChanSignals.Q2S0.value}',
                            'xt_value': ChanSignals.Q2S0.value,
                            'xt_source': 'xt_5_signals'
                        }

                        self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                        self.policy.save()
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": short_tns['signal_name'],
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)
                        self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                        return

                # (逆势）倒2笔、倒1笔超长分笔v反（小转大），站稳死叉下方
                if self.kline_y.bi_list[-3].height >  self.kline_y.bi_list[-2].height > 3 * self.kline_y.bi_height_ma(60)\
                    and self.kline_y.bi_list[-3].end == self.kline_y.cur_duan.end\
                    and self.kline_y.cur_bi.high < min(self.kline_y.line_ma1[-1],self.kline_y.line_ma2[-1])\
                    and self.kline_y.bi_list[-3].atan < self.kline_y.bi_list[-2].atan \
                    and self.kline_y.bi_list[-3].height * 0.819 < self.kline_y.bi_list[-2].height:
                    short_tns = {
                        "status": TNS_STATUS_OBSERVATE,
                        "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        "zs_start": str(self.kline_y.cur_bi_zs.start),
                        "zs_end": str(self.kline_y.cur_bi_zs.end),
                        "zs_high": float(self.kline_y.cur_bi_zs.high),
                        "zs_low": float(self.kline_y.cur_bi_zs.low),
                        "zs_height": float(self.kline_y.cur_bi_zs.height),
                        "zs_middle": float(self.kline_y.cur_bi_zs.middle),
                        'duan_low': float(self.kline_y.cur_duan.low),
                        'duan_high': float(self.kline_y.cur_duan.high),
                        'win_price': float(self.kline_y.bi_list[-2].low),
                        'success_price': float(self.kline_y.bi_list[-2].low),
                        'stop_price': float(min(self.kline_y.bi_list[-2].high,
                                                self.kline_y.cur_bi.high + self.kline_y.bi_height_ma(60))),
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': f'倒2_小转大_{ChanSignals.Q2S0.value}',
                        'xt_value': ChanSignals.Q2S0.value,
                        'xt_source': 'xt_5_signals'
                    }

                    self.policy.sub_tns[SIGNAL_THREE_SHORT] = short_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": short_tns['signal_name'],
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_THREE_SHORT, d.get('operation')))
                    return

    def tns_process_sub(self):
        """处理各种子事务，"""
        if not self.kline_x.pre_duan:
            return

        for signal in list(self.policy.sub_tns.keys()):

            # 三买做多信号
            if signal == SIGNAL_THREE_BUY:
                self.tns_proces_close_long(signal)
                self.tns_process_open_long(signal)
                continue

            # 三卖做空信号
            if signal == SIGNAL_THREE_SHORT:
                self.tns_process_close_short(signal)
                self.tns_process_open_short(signal)
                continue

    def tns_remove_signal(self, signal):
        """
        移除信号
        :param signal:
        :return:
        """
        if signal in self.policy.sub_tns:
            self.policy.sub_tns.pop(signal,None)
            self.policy.save()
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"{signal}信号移除",
                "signal": signal
            }
            self.save_dist(d)

    def tns_process_open_long(self, signal):
        """
        处理三买子事务
        观测、就绪、开仓、持仓时主动离场等
        """
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')

        # 买入观测状态，
        if status == TNS_STATUS_OBSERVATE:

            if self.cur_99_price <= sub_tns.get('stop_price'):
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == -1:
                self.write_log(f'新一笔出现，还没进场，当前多单观测信号移除')
                self.tns_remove_signal(signal)
                return

            # 动态计算次级别K线摆动指标是否金叉
            self.kline_x.rt_count_skd()
            if self.kline_y.cur_skd_count > 0 and self.kline_x.cur_skd_count > 0:
                status = TNS_STATUS_READY
                # 状态转移成功
                sub_tns.update({"status": status})
                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()
                self.write_log(f'{signal} {TNS_STATUS_OBSERVATE} => {TNS_STATUS_READY}')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}",
                    "signal": signal
                }
                self.save_dist(d)

        # 买入就绪状态
        if status == TNS_STATUS_READY:
            win_price = sub_tns.get('win_price')
            stop_price = sub_tns.get('stop_price')

            if self.cur_99_price < sub_tns.get('stop_price'):
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == -1:
                self.write_log(f'新一笔出现，还没进场，当前多单观测信号移除')
                self.tns_remove_signal(signal)
                return

            # 开始进入委托下单阶段
            if self.cur_99_price > sub_tns.get('stop_price') \
                    and not self.is_entry_close_time():

                if self.tns_buy(signal=signal, stop_price=stop_price, win_price=sys.maxsize):
                    status = TNS_STATUS_ORDERING
                    sub_tns.update({'status': status,
                                    'open_bi_start': self.kline_y.cur_bi.start,
                                    'last_open_price': self.cur_99_price})

                    self.policy.sub_tns.update({signal: sub_tns})
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}",
                        "signal": signal
                    }
                    self.save_dist(d)

        # 委托状态
        if status == TNS_STATUS_ORDERING:

            # 判断是否已经开仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[signal])
            if len(open_grids) > 0:
                self.write_log(f'策略已经持有多单，不再开仓')
                self.write_log(f'{signal} 已持仓.  {status} => {TNS_STATUS_OPENED}')
                status = TNS_STATUS_OPENED
                sub_tns.update({"status": status})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}=> {TNS_STATUS_OPENED}",
                    "signal": signal
                }
                self.save_dist(d)
                if self.policy.last_long_bi_start != self.kline_y.cur_bi.start:
                    self.policy.last_long_bi_start = self.kline_y.cur_bi.start

                self.policy.save()
                return
            else:
                if len(self.active_orders) == 0 and self.entrust == 0:
                    status = TNS_STATUS_READY
                    self.write_log(f'策略未持有多单，修正状态')
                    sub_tns.update({"status": status, 'open_bi_start': None})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}=> {TNS_STATUS_READY}",
                        "signal": signal
                    }
                    self.save_dist(d)
                    self.policy.save()
                    return

    def tns_proces_close_long(self, signal):
        """
        处理多单离场得事务
        - 日内：收盘前离场
        - 持仓状态： 开仓 =》 中枢 《=》段趋势
        - 止损 =》 触碰tns的止损价，止损离场
        - 触碰success_price =》 1) 线段走势 2）中枢
        - 未能突破surcess_price ==》 形成中枢
        - 线段走势 =》 分笔走势背驰 =》主动离场

        :return:
        """
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')
        if status != TNS_STATUS_OPENED:
            return

        last_open_price = sub_tns.get('last_open_price', None)
        stop_price = sub_tns.get('stop_price', None)
        win_price = sub_tns.get('win_price', None)
        success_price = sub_tns.get('success_price', None)
        pos_status = sub_tns.get('pos_status', None)

        # 判断是否已经平仓
        open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[signal])
        if len(open_grids) == 0:
            self.write_log(f'当前{signal}信号退出')
            self.tns_remove_signal(signal)
            return

        elif last_open_price and stop_price:

            # 日内收盘前离场，根据参数配置的离场时间来判断，一般kline_y的周期在3分钟内的
            if self.is_entry_close_time() and win_price > self.cur_99_price:
                self.tns_update_grid(direction=Direction.LONG,
                                     grid_type=signal,
                                     win_price=self.cur_99_price - self.price_tick * 2)
                self.write_log(f'收盘前{signal}主动离场')
                self.tns_remove_signal(signal)
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"收盘前离场",
                    "signal": signal
                }
                self.save_dist(d)
                return

            # 持仓状态01： 开仓成功 => 段走势
            if pos_status is None and success_price and self.cur_99_price >= success_price:
                duan_counts = sub_tns.get('duan_counts',0) + 1
                sub_tns.update({'pos_status': DUAN_TREND, 'duan_counts':duan_counts})
                self.write_log(f'当前{signal}信号突破新高，形成线段走势')
                self.policy.sub_tns.update({signal: sub_tns})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"=>{DUAN_TREND}:{duan_counts}",
                    "signal": signal
                }
                self.save_dist(d)
                return

            # 持仓状态02： 开仓成功或段走势 =》 形成中枢
            if pos_status in [None, DUAN_TREND] and self.kline_y.cur_bi.direction == -1:
                if self.kline_y.cur_bi_zs \
                        and self.kline_y.cur_bi_zs.start > sub_tns.get('zs_end')\
                        and self.kline_y.bi_list[-2].start >= self.kline_y.cur_bi_zs.end\
                        and self.kline_y.cur_bi.low < self.kline_y.cur_bi_zs.high:
                    panzheng_counts = sub_tns.get('panzheng_counts',0) + 1
                    sub_tns.update({'pos_status': ZS_PANZHENG, 'panzheng_counts':panzheng_counts})
                    self.write_log(f'当前{signal}信号形成中枢盘整')

                    if panzheng_counts > 1 and stop_price < last_open_price < self.cur_99_price\
                            and self.kline_y.cur_bi_zs.start > sub_tns.get('bi_start'):
                        new_stop_price = last_open_price + self.price_tick
                        self.write_log(f'第{panzheng_counts}次出现盘整中枢，保本')
                        sub_tns.update({"stop_price": new_stop_price})
                    self.policy.sub_tns.update({signal: sub_tns})

                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"=>{ZS_PANZHENG}:{panzheng_counts}",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

            # 持仓状态03： 盘整=> 脱离中枢，形成新三买
            if pos_status == ZS_PANZHENG and self.kline_y.cur_bi.direction == 1 \
                    and self.kline_y.bi_list[-3].start == self.kline_y.cur_bi_zs.end\
                    and self.kline_y.cur_bi.low > self.kline_y.cur_bi_zs.high > sub_tns.get('zs_high') \
                    and self.kline_y.bi_list[-2].high > self.kline_y.cur_bi_zs.max_high:

                # 提损：中枢得进入笔与离开笔，存在背驰，提损
                if self.kline_y.is_zs_beichi(Direction.LONG, self.kline_y.bi_list[-3].end):
                    if stop_price < float(self.kline_y.cur_bi.low):
                        self.write_log(f'当前信号{signal} 止损价从{stop_price} => {float(self.kline_y.cur_bi.low)}')
                        stop_price = float(self.kline_y.cur_bi.low)
                        sub_tns.update({'success_price': float(self.kline_y.bi_list[-2].high),
                                        'stop_price': stop_price})
                        self.tns_update_grid(direction=Direction.LONG,
                                             grid_type=signal,
                                             stop_price=stop_price)
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"三买提损",
                            "signal": signal
                        }
                        self.save_dist(d)

                self.write_log(f'当前信号{signal} 从{pos_status} => {DUAN_TREND}')
                duan_counts = sub_tns.get('duan_counts',0)+1
                sub_tns.update({'pos_status': DUAN_TREND,'duan_counts':duan_counts})
                self.policy.sub_tns.update({signal: sub_tns})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"=>{DUAN_TREND}:{duan_counts}",
                    "signal": signal
                }
                self.save_dist(d)

            # 提损：段走势得最后两笔，要么斜率不够，要么dif背离
            if pos_status == DUAN_TREND \
                    and check_duan_not_rt(self.kline_y, Direction.LONG)\
                    and self.kline_y.bi_list[-5].start >= self.kline_y.cur_bi_zs.end:
                # 线段的末端两个分笔，斜率（能量）不够
                if self.kline_y.bi_list[-2].atan < self.kline_y.bi_list[-4].atan \
                        and stop_price < float(self.kline_y.bi_list[-1].low)\
                        and last_open_price < float(self.kline_y.bi_list[-1].low):
                    new_stop_price = float(self.kline_y.bi_list[-1].low)
                    self.tns_update_grid(direction=Direction.LONG,
                                         grid_type=signal,
                                         stop_price=new_stop_price)
                    self.write_log(f'{self.kline_y.name} 段趋势后，出现顶分型，atan小于前上涨笔，{signal} 调整止损:{stop_price}=>{new_stop_price}')
                    # self.tns_remove_signal(signal)
                    sub_tns.update({"stop_price":new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"线段背驰提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

                # DIF值顶背驰
                if self.kline_y.is_fx_macd_divergence(direction=Direction.LONG,
                                                      cur_duan=self.kline_y.cur_duan)\
                        and stop_price < float(self.kline_y.bi_list[-1].low)\
                        and last_open_price < float(self.kline_y.bi_list[-1].low):
                    new_stop_price = float(self.kline_y.bi_list[-1].low)
                    self.tns_update_grid(direction=Direction.LONG,
                                         grid_type=signal,
                                         stop_price=new_stop_price)
                    self.write_log(f'{self.kline_y.name} 段趋势后，出现dif顶背驰，{signal} 调整止损:{stop_price}=>{new_stop_price}')
                    # self.tns_remove_signal(signal)
                    sub_tns.update({"stop_price": new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"线段DIF提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

            # 提损：超长分笔时，三倍平均波幅，0.618位置止损，四倍平均波幅，0.5位置止损
            if check_bi_not_rt(self.kline_y, Direction.LONG) \
                    and self.kline_y.cur_bi.middle > self.kline_y.cur_bi_zs.high:

                if self.kline_y.cur_bi.height > 3 * self.kline_y.bi_height_ma(60) \
                        and stop_price < float(self.kline_y.cur_bi.high - 0.618 * self.kline_y.cur_bi.height):
                    new_stop_price = float(self.kline_y.cur_bi.high - 0.618 * self.kline_y.cur_bi.height)
                    self.write_log(f'当前信号{signal}, 超长分笔提损:{stop_price}=>{new_stop_price}')
                    sub_tns.update({'stop_price':new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"超长3分笔提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

                if self.kline_y.cur_bi.height > 4 * self.kline_y.bi_height_ma(60)\
                        and stop_price < float(self.kline_y.cur_bi.high - 0.5 * self.kline_y.cur_bi.height):
                    new_stop_price = float(self.kline_y.cur_bi.high - 0.5 * self.kline_y.cur_bi.height)
                    self.write_log(f'当前信号{signal}, 超长分笔提损:{stop_price}=>{new_stop_price}')
                    sub_tns.update({'stop_price':new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"超长4分笔提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

            # 出现趋势1卖信号,且中枢背驰
            ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y, direction=Direction.SHORT, x=1, include_Q1=True)
            if ret and xt_signal.get('signal') == ChanSignals.Q1S0.value \
                    and self.kline_y.cur_bi.low > self.kline_y.cur_bi_zs.high \
                    and self.kline_y.is_zs_beichi(Direction.LONG, self.kline_y.cur_bi.start)\
                    and stop_price < float(self.kline_y.bi_list[-2].low):
                new_stop_price = float(self.kline_y.bi_list[-2].low)
                self.tns_update_grid(direction=Direction.LONG,
                                     grid_type=signal,
                                     stop_price=new_stop_price)
                self.write_log(f'{self.kline_y.name}出现{xt_signal.get("xt_source")}-'
                               f'{xt_signal.get("signal")}信号，{signal}提升止损:{stop_price}=>{new_stop_price}')
                #self.tns_remove_signal(signal)
                sub_tns.update({"stop_price": new_stop_price})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"趋势1卖提损",
                    "signal": signal
                }
                self.save_dist(d)
                return


            if check_bi_not_rt(self.kline_y, Direction.LONG):

                if self.kline_y.cur_bi_zs.start > sub_tns.get('bi_start') \
                    and float(self.kline_y.cur_bi_zs.high) < last_open_price:
                    self.write_log(f'开仓后，出现一个中枢，顶部低于开仓价，主动离场')
                    self.tns_update_grid(direction=Direction.LONG,
                                         grid_type=signal,
                                         win_price=self.cur_99_price)
                    self.tns_remove_signal(signal)
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"新中枢低于开仓价",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return


                # 判断是否存在5笔下颈线突破得卖点
                xt_signal = self.kline_y.get_xt_signal('xt_5_signals')
                xt_value = xt_signal.get('signal')
                if xt_value in [ChanSignals.SG0.value, ChanSignals.SI0.value]:
                    if sub_tns.get('open_bi_start') >= self.kline_y.bi_list[-3].start \
                            and sub_tns.get('open_bi_start') < self.kline_y.cur_bi.start \
                            and stop_price < float(self.kline_y.cur_bi.middle) < self.cur_99_price:
                        new_stop_price = float(self.kline_y.cur_bi.middle)
                        self.tns_update_grid(direction=Direction.LONG,
                                             grid_type=signal,
                                             stop_price= new_stop_price)
                        self.write_log(f'{self.kline_y.name}出现{xt_value}信号，{signal} 提损:{stop_price}=>{new_stop_price}')
                        # self.tns_remove_signal(signal)
                        sub_tns.update({'stop_price': new_stop_price})
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"下颈线突破主动提损",
                            "signal": signal
                        }
                        self.save_dist(d)
                        return

                    # 单笔=单段
                    if len(self.kline_y.cur_duan.bi_list) == 1:
                        self.write_log(f'{self.kline_y.name}出现{xt_value}信号，{signal},主动离场')
                        self.tns_update_grid(direction=Direction.LONG,
                                             grid_type=signal,
                                             win_price=self.cur_99_price)
                        self.tns_remove_signal(signal)
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"下颈线突破主动离场",
                            "signal": signal
                        }
                        self.save_dist(d)
                        return

                    # 在上涨线段后，出现下颈线突破
                    if sub_tns.get('open_bi_start') < self.kline_y.bi_list[-4].start \
                            and last_open_price < float(self.kline_y.cur_bi.low) \
                            and stop_price < float(self.kline_y.cur_bi.middle)\
                            and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start:
                        self.tns_update_grid(direction=Direction.LONG,
                                             grid_type=signal,
                                             stop_price=float(self.kline_y.cur_bi.middle))
                        self.write_log(
                            f'{self.kline_y.name}出现{xt_value}信号，{signal}更新止损价{stop_price} =>{float(self.kline_y.cur_bi.middle)} ')
                        sub_tns.update({'stop_price': float(self.kline_y.cur_bi.middle)})
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"下颈线突破提损",
                            "signal": signal
                        }
                        self.save_dist(d)

                        return

                # 另类得五笔下颈线线突破。bi_1.high = max_high, bi_1.low = min_low, bi_5.high < bi_3.low
                if self.kline_y.bi_list[-5].high > max([bi.high for bi in self.kline_y.bi_list[-3:]])\
                    and self.kline_y.bi_list[-5].low < min([bi.low for bi in self.kline_y.bi_list[-3:]])\
                    and self.kline_y.cur_bi.high < self.kline_y.bi_list[-3].low \
                    and sub_tns.get('open_bi_start') < self.kline_y.bi_list[-4].start \
                    and last_open_price < float(self.kline_y.cur_bi.low) \
                    and stop_price < float(self.kline_y.cur_bi.middle):
                    self.tns_update_grid(direction=Direction.LONG,
                                         grid_type=signal,
                                         stop_price=float(self.kline_y.cur_bi.middle))
                    self.write_log(
                        f'{self.kline_y.name}出现下颈线信号，{signal}更新止损价{stop_price} =>{float(self.kline_y.cur_bi.middle)} ')
                    sub_tns.update({'stop_price': float(self.kline_y.cur_bi.middle)})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"下颈线突破提损",
                        "signal": signal
                    }
                    self.save_dist(d)

                    return

    def tns_process_open_short(self, signal):
        """处理三卖子事务"""
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')

        # 卖出观测状态，
        if status == TNS_STATUS_OBSERVATE:

            if self.cur_99_price >= sub_tns.get('stop_price'):
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == 1:
                self.write_log(f'新一笔出现，还没进场，当前空单观测信号移除')
                self.tns_remove_signal(signal)
                return

            # 动态计算次级别周期摆动指标是否死叉
            self.kline_x.rt_count_skd()

            if self.kline_y.cur_skd_count < 0\
                    and self.kline_x.cur_skd_count < 0:

                status = TNS_STATUS_READY
                sub_tns.update({"status": status})
                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()
                self.write_log(f'{signal} {TNS_STATUS_OBSERVATE} => {TNS_STATUS_READY}')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}",
                    "signal": signal
                }
                self.save_dist(d)

        # 卖出就绪状态
        if status == TNS_STATUS_READY:
            win_price = sub_tns.get('win_price')
            stop_price = sub_tns.get('stop_price')

            if self.cur_99_price > stop_price:
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == 1:
                self.write_log(f'新一笔出现，还没进场，当前空单观测信号移除')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price < stop_price \
                    and not self.is_entry_close_time():
                if self.tns_short(signal=signal,
                                  stop_price=stop_price,
                                  win_price=-sys.maxsize):
                    status = TNS_STATUS_ORDERING
                    sub_tns.update({'status': status,
                                    'open_bi_start': self.kline_y.cur_bi.start,
                                    'last_open_price': self.cur_99_price})
                    self.policy.sub_tns.update({signal: sub_tns})
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}",
                        "signal": signal
                    }
                    self.save_dist(d)

        # 委托状态
        if status == TNS_STATUS_ORDERING:

            # 判断是否已经开仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[signal])
            if len(open_grids) > 0:
                self.write_log(f'策略已经持有空单，不再开仓')
                self.write_log(f'{signal} 已持仓.  {status} => {TNS_STATUS_OPENED}')
                status = TNS_STATUS_OPENED
                sub_tns.update({"status": status})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}=> {TNS_STATUS_OPENED}",
                    "signal": signal
                }
                if self.policy.last_short_bi_start != self.kline_y.cur_bi.start:
                    self.policy.last_short_bi_start = self.kline_y.cur_bi.start
                self.save_dist(d)
                self.policy.save()
                return
            else:
                if len(self.active_orders) == 0 and self.entrust == 0:
                    status = TNS_STATUS_READY
                    self.write_log(f'策略未持有空单，修正状态')
                    sub_tns.update({"status": status, "open_bi_start": None})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}=> {TNS_STATUS_READY}",
                        "signal": signal
                    }
                    self.save_dist(d)
                    self.policy.save()
                    return

    def tns_process_close_short(self, signal):
        """
        处理空单得离场事务
        :param signal:
        :return:
        """
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')
        if status != TNS_STATUS_OPENED:
            return

        last_open_price = sub_tns.get('last_open_price', None)
        stop_price = sub_tns.get('stop_price', None)
        win_price = sub_tns.get('win_price', None)
        success_price = sub_tns.get('success_price',None)
        pos_status = sub_tns.get('pos_status',None)
        # 判断是否已经平仓
        open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[signal])
        if len(open_grids) == 0:
            self.write_log(f'当前{signal}信号退出')
            self.tns_remove_signal(signal)
            return

        elif last_open_price and stop_price:

            # 日内离场逻辑
            if self.is_entry_close_time() and win_price < self.cur_99_price:
                self.tns_update_grid(direction=Direction.SHORT,
                                     grid_type=signal,
                                     win_price=self.cur_99_price + 2 * self.price_tick)
                self.write_log(f'收盘前{signal}主动离场')
                self.tns_remove_signal(signal)
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"收盘前离场",
                    "signal": signal
                }
                self.save_dist(d)
                return

            # 持仓状态01： 开仓成功 => 段走势
            if pos_status is None and success_price and self.cur_99_price <= success_price:
                duan_counts = sub_tns.get('duan_counts',0) + 1
                sub_tns.update({'pos_status': DUAN_TREND,
                                'trend_counts':duan_counts})
                self.write_log(f'当前{signal}信号突破新新，形成下跌线段走势')
                self.policy.sub_tns.update({signal: sub_tns})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"=>{DUAN_TREND}:{duan_counts}",
                    "signal": signal
                }
                self.save_dist(d)
                return

            # 持仓状态02： 开仓成功，段走势 =》 中枢
            if pos_status in [None, DUAN_TREND] and self.kline_y.cur_bi.direction == 1:
                if self.kline_y.cur_bi_zs \
                        and self.kline_y.cur_bi_zs.start > sub_tns.get('zs_end')\
                        and self.kline_y.bi_list[-2].start >= self.kline_y.cur_bi_zs.end\
                        and self.kline_y.cur_bi.high < self.kline_y.cur_bi_zs.low:
                    panzheng_counts = sub_tns.get('panzheng_counts',0) + 1
                    sub_tns.update({'pos_status': ZS_PANZHENG,
                                    'panzheng_counts':panzheng_counts })
                    self.write_log(f'当前{signal}信号形成中枢盘整')

                    if panzheng_counts > 1 and stop_price > last_open_price > self.cur_99_price\
                            and self.kline_y.cur_bi_zs.start > sub_tns.get('bi_start'):
                        new_stop_price = last_open_price + self.price_tick
                        self.write_log(f'第{panzheng_counts}次出现盘整中枢，保本')
                        sub_tns.update({"stop_price": new_stop_price})

                    self.policy.sub_tns.update({signal: sub_tns})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"=>{ZS_PANZHENG}:{panzheng_counts}",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

            # 持仓状态03：中枢 =》 段走势
            if pos_status == ZS_PANZHENG and self.kline_y.cur_bi.direction == -1\
                    and self.kline_y.bi_list[-3].start == self.kline_y.cur_bi_zs.end \
                    and self.kline_y.cur_bi.high < self.kline_y.cur_bi_zs.low < sub_tns.get('zs_low')\
                    and self.kline_y.bi_list[-2].low < self.kline_y.cur_bi_zs.min_low:

                # 中枢进入笔与离开笔，存在背驰 => 提损
                if self.kline_y.is_zs_beichi(Direction.SHORT,self.kline_y.bi_list[-3].end):
                    if stop_price > float(self.kline_y.cur_bi.high):
                        self.write_log(f'当前信号{signal} 止损价从{stop_price} => {float(self.kline_y.cur_bi.high)}')
                        stop_price = float(self.kline_y.cur_bi.high)
                        sub_tns.update({'success_price': float(self.kline_y.bi_list[-2].low),
                                        'stop_price': stop_price})
                        self.tns_update_grid(direction=Direction.SHORT,
                                             grid_type=signal,
                                             stop_price=stop_price)
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"三卖提损",
                            "signal": signal
                        }
                        self.save_dist(d)

                self.write_log(f'当前信号{signal} 从{pos_status} => {DUAN_TREND}')
                duan_counts = sub_tns.get('duan_counts',0) + 1
                sub_tns.update({'pos_status': DUAN_TREND,
                                'duan_counts': duan_counts+1})
                self.policy.sub_tns.update({signal: sub_tns})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"=>{DUAN_TREND}:{duan_counts}",
                    "signal": signal
                }
                self.save_dist(d)

            # 提损02：
            if pos_status == DUAN_TREND \
                    and check_duan_not_rt(self.kline_y,Direction.SHORT)\
                    and self.kline_y.bi_list[-5].start >= self.kline_y.cur_bi_zs.end:

                if self.kline_y.bi_list[-2].atan < self.kline_y.bi_list[-4].atan\
                        and stop_price > float(self.kline_y.bi_list[-1].high)\
                        and last_open_price > float(self.kline_y.bi_list[-1].high):
                    new_stop_price = float(self.kline_y.bi_list[-1].high)
                    self.tns_update_grid(direction=Direction.SHORT,
                                         grid_type=signal,
                                         stop_price=new_stop_price)
                    self.write_log(f'{self.kline_x.name} 段趋势后，出现顶分型，atan小于前下涨笔'
                                   f'，{signal},调整止损:{stop_price}=>{new_stop_price}')
                    # self.tns_remove_signal(signal)
                    sub_tns.update({"stop_price":new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"线段背驰提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

                if self.kline_y.is_fx_macd_divergence(direction=Direction.SHORT,
                                                      cur_duan=self.kline_y.cur_duan)\
                        and stop_price > float(self.kline_y.bi_list[-1].high)\
                        and last_open_price > float(self.kline_y.bi_list[-1].high):
                    new_stop_price = float(self.kline_y.bi_list[-1].high)
                    self.tns_update_grid(direction=Direction.SHORT,
                                         grid_type=signal,
                                         stop_price=new_stop_price)
                    self.write_log(f'{self.kline_x.name} 段趋势后，出现dif底背驰'
                                   f'，{signal},调整止损:{stop_price}=>{new_stop_price}')
                    # self.tns_remove_signal(signal)
                    sub_tns.update({"stop_price": new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"分笔DIF提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

            # 提损： 超长分笔时，三倍平均波幅，0.618位置止损，四倍平均波幅，0.5位置止损
            if check_bi_not_rt(self.kline_y, Direction.SHORT) \
                    and self.kline_y.cur_bi.middle < self.kline_y.cur_bi_zs.low:

                if self.kline_y.cur_bi.height > 3 * self.kline_y.bi_height_ma(60) \
                        and stop_price > float(self.kline_y.cur_bi.low + 0.618 * self.kline_y.cur_bi.height):
                    new_stop_price = float(self.kline_y.cur_bi.low + 0.618 * self.kline_y.cur_bi.height)
                    self.write_log(f'当前信号{signal}, 超长分笔提损:{stop_price}=>{new_stop_price}')
                    sub_tns.update({'stop_price': new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"超长3分笔提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

                if self.kline_y.cur_bi.height > 4 * self.kline_y.bi_height_ma(60) \
                        and stop_price > float(self.kline_y.cur_bi.low + 0.5 * self.kline_y.cur_bi.height):
                    new_stop_price = float(self.kline_y.cur_bi.low + 0.5 * self.kline_y.cur_bi.height)
                    self.write_log(f'当前信号{signal}, 超长分笔提损:{stop_price}=>{new_stop_price}')
                    sub_tns.update({'stop_price': new_stop_price})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"超长4分笔提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

            # 提损： 趋势一买信号
            ret, xt_signal = self.kline_has_xt_signals(kline=self.kline_y, direction=Direction.LONG, x=1, include_Q1=True)
            if ret and xt_signal.get('signal') == ChanSignals.Q1L0.value \
                    and self.kline_y.cur_bi.high < self.kline_y.cur_bi_zs.low\
                    and self.kline_y.is_zs_beichi(Direction.SHORT,self.kline_y.cur_bi.start)\
                    and stop_price > float(self.kline_y.bi_list[-2].high):

                # 如果开仓位置在cur_bi_zs之上
                new_stop_price = float(self.kline_y.bi_list[-2].high)
                self.tns_update_grid(direction=Direction.SHORT,
                                     grid_type=signal,
                                     stop_price=new_stop_price)
                self.write_log(f'{self.kline_y.name} 出现{xt_signal.get("xt_source")}-{xt_signal.get("signal")}信号，'
                               f'{signal} 止损提升:{stop_price}=>{new_stop_price}')
                # self.tns_remove_signal(signal)
                sub_tns.update({"stop_price": new_stop_price})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"趋势1买提损",
                    "signal": signal
                }
                self.save_dist(d)
                return

            # 向下一笔+底分型
            if check_bi_not_rt(self.kline_y, Direction.SHORT):

                if self.kline_y.cur_bi_zs.start > sub_tns.get('bi_start') \
                        and float(self.kline_y.cur_bi_zs.low) > last_open_price:
                    self.write_log(f'开仓后，出现一个中枢，底部高于开仓价，主动离场')
                    self.tns_update_grid(direction=Direction.SHORT,
                                         grid_type=signal,
                                         win_price=self.cur_99_price)
                    self.tns_remove_signal(signal)
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"新中枢高于开仓价",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

                # 判断是否存在5笔上颈线突破得卖点
                xt_signal = self.kline_y.get_xt_signal('xt_5_signals')
                xt_value = xt_signal.get('signal')
                if xt_value in [ChanSignals.LG0.value, ChanSignals.LI0.value]:

                    # 如果开仓发生在三笔以内，主动离场
                    if sub_tns.get('open_bi_start') >= self.kline_y.bi_list[-3].start\
                            and sub_tns.get('open_bi_start') < self.kline_y.cur_bi.start\
                            and stop_price > float(self.kline_y.cur_bi.middle) > self.cur_99_price:
                        new_stop_price = float(self.kline_y.cur_bi.middle)
                        self.tns_update_grid(direction=Direction.SHORT,
                                             grid_type=signal,
                                             stop_price=new_stop_price)
                        self.write_log(f'{self.kline_y.name}出现{xt_value}信号，{signal}主动提损:{stop_price}=>{new_stop_price}')
                        # self.tns_remove_signal(signal)
                        sub_tns.update({'stop_price':new_stop_price})
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"上颈线突破提损",
                            "signal": signal
                        }
                        self.save_dist(d)
                        return

                    if len(self.kline_y.cur_duan.bi_list) == 1:
                        self.write_log(f'{self.kline_y.name}出现{xt_value}信号，{signal}主动离场')
                        self.tns_update_grid(direction=Direction.SHORT,
                                             grid_type=signal,
                                             win_price=self.cur_99_price)
                        self.tns_remove_signal(signal)
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"上颈线突破离场",
                            "signal": signal
                        }
                        self.save_dist(d)
                        return

                    if sub_tns.get('open_bi_start') < self.kline_y.bi_list[-4].start \
                            and last_open_price > float(self.kline_y.cur_bi.high)\
                            and stop_price > float(self.kline_y.cur_bi.middle)\
                            and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start:
                        self.tns_update_grid(direction=Direction.SHORT,
                                             grid_type=signal,
                                             stop_price=float(self.kline_y.cur_bi.middle))
                        self.write_log(f'{self.kline_y.name}出现{xt_value}信号，{signal}更新止损价{stop_price} =>{float(self.kline_y.cur_bi.middle)} ')
                        sub_tns.update({'stop_price': float(self.kline_y.cur_bi.middle)})
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"上颈线突破提损",
                            "signal": signal
                        }
                        self.save_dist(d)
                        return

                # 另类得五笔上颈线线突破。bi_1.high = max_high, bi_1.low = min_low, bi_5.low > bi_3.high
                if self.kline_y.bi_list[-5].high > max([bi.high for bi in self.kline_y.bi_list[-3:]]) \
                        and self.kline_y.bi_list[-5].low < min([bi.low for bi in self.kline_y.bi_list[-3:]]) \
                        and self.kline_y.cur_bi.low > self.kline_y.bi_list[-3].high \
                        and sub_tns.get('open_bi_start') < self.kline_y.bi_list[-4].start \
                        and last_open_price > float(self.kline_y.cur_bi.high) \
                        and stop_price > float(self.kline_y.cur_bi.middle):
                    self.tns_update_grid(direction=Direction.SHORT,
                                         grid_type=signal,
                                         stop_price=float(self.kline_y.cur_bi.middle))
                    self.write_log(f'{self.kline_y.name}出现上颈线突破信号，{signal}更新'
                                   f'止损价{stop_price} =>{float(self.kline_y.cur_bi.middle)} ')
                    sub_tns.update({'stop_price': float(self.kline_y.cur_bi.middle)})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"上颈线突破提损",
                        "signal": signal
                    }
                    self.save_dist(d)
                    return

    def tns_update_grid(self, direction, grid_type, win_price=None, stop_price=None):
        """
        更新持仓网格得止盈、止损价
        :param direction:
        :param grid_type:
        :param win_price:
        :param stop_price:
        :return:
        """
        opened_grids = self.gt.get_opened_grids_within_types(direction, [grid_type])
        for g in opened_grids:
            if win_price:
                if g.close_price and g.close_price != win_price:
                    g.close_price = win_price
            if stop_price:
                if g.stop_price and g.stop_price != stop_price:
                    g.stop_price = stop_price

    def tns_buy(self, signal, stop_price, win_price, first_open=True):
        """处理Ordering状态的tns买入处理"""
        if not (self.inited and self.trading):
            return False
        if self.entrust != 0:
            return False

        sub_tns = self.policy.sub_tns.get(signal)

        # 检查是否已经开仓成功
        opened_long_grids = [g for g in self.gt.dn_grids if g.type == signal and g.open_status]
        if len(opened_long_grids) > 0 and first_open:
            self.write_log(f'{signal}已开多完成, 更新状态{TNS_STATUS_ORDERING}=>{TNS_STATUS_OPENED}')
            sub_tns.update({'status': TNS_STATUS_OPENED})
            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "operation": f"{TNS_STATUS_ORDERING}=>{TNS_STATUS_OPENED}",
                "signal": signal,
                "stop_price": stop_price

            }
            self.save_dist(d)
            return False

        if self.position.pos > 0 and self.position.long_pos > 0 and first_open:
            self.write_log(f'已经存在其他信号的开多仓位，不再开仓')
            sub_tns.update({'status': TNS_STATUS_CLOSED})
            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "operation": f"{TNS_STATUS_ORDERING}=>{TNS_STATUS_CLOSED}",
                "signal": signal,
                "stop_price": stop_price
            }
            self.save_dist(d)
            return False

        # 未开仓
        # 开仓手数
        open_volume = self.tns_get_open_volume(stop_price)
        if open_volume == 0:
            return False

        grid = CtaGrid(direction=Direction.LONG,
                       vt_symbol=self.vt_symbol,
                       open_price=self.cur_mi_price,
                       close_price=win_price,
                       stop_price=stop_price,
                       volume=open_volume,
                       type=signal,
                       snapshot={'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})

        order_ids = self.buy(price=self.cur_mi_price,
                             volume=grid.volume,
                             order_time=self.cur_datetime,
                             order_type=self.order_type,
                             grid=grid)
        if len(order_ids) > 0:
            self.write_log(f'[事务开多] {signal} 委托成功{order_ids},开仓价：{grid.open_price}，数量：{grid.volume}'
                           f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')
            grid.open_time = self.cur_datetime
            self.gt.dn_grids.append(grid)
            self.gt.save()

            # 加仓部分，增加最后开仓价格
            if not first_open:
                sub_tns.update({"last_open_price": self.cur_99_price})
                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()

            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "price": self.cur_mi_price,
                "volume": open_volume,
                "operation": f"send buy order",
                "signal": signal,
                "stop_price": stop_price
            }
            self.save_dist(d)
            return True
        else:
            self.write_error(f'[事务开多] {signal} 委托失败,开仓价：{grid.open_price}，数量：{grid.volume}'
                             f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')

        return False

    def tns_short(self, signal, stop_price, win_price, first_open=True):
        """处理Ordering状态的tns开空处理"""
        if not (self.inited and self.trading):
            return False
        if self.entrust != 0:
            return False

        sub_tns = self.policy.sub_tns.get(signal)

        # 检查是否已经开仓成功
        opened_short_grids = [g for g in self.gt.up_grids if g.type == signal and g.open_status]
        if len(opened_short_grids) > 0 and first_open:
            self.write_log(f'{signal}已开空完成, 更新状态{TNS_STATUS_ORDERING}=>{TNS_STATUS_OPENED}')
            sub_tns.update({'status': TNS_STATUS_OPENED})
            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "operation": f"{TNS_STATUS_ORDERING}=>{TNS_STATUS_OPENED}",
                "signal": signal,
                "stop_price": stop_price
            }
            self.save_dist(d)
            return False

        if self.position.pos < 0 and self.position.short_pos < 0 and first_open:
            self.write_log(f'已有其他空单，更新状态{TNS_STATUS_ORDERING}=>{TNS_STATUS_CLOSED}')
            sub_tns.update({'status': TNS_STATUS_CLOSED})
            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "operation": f"{TNS_STATUS_ORDERING}=>{TNS_STATUS_CLOSED}",
                "signal": signal,
                "stop_price": stop_price
            }
            self.save_dist(d)
            return False

        # 未开仓
        # 开仓手数
        open_volume = self.tns_get_open_volume(stop_price)
        if open_volume == 0:
            return False

        grid = CtaGrid(direction=Direction.SHORT,
                       vt_symbol=self.vt_symbol,
                       open_price=self.cur_mi_price,
                       close_price=win_price,
                       stop_price=stop_price,
                       volume=open_volume,
                       type=signal,
                       snapshot={'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})

        order_ids = self.short(price=self.cur_mi_price,
                               volume=grid.volume,
                               order_time=self.cur_datetime,
                               order_type=self.order_type,
                               grid=grid)
        if len(order_ids) > 0:
            self.write_log(f'[事务开空] {signal} 委托成功{order_ids},开仓价：{grid.open_price}，数量：{grid.volume}'
                           f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')
            grid.open_time = self.cur_datetime
            self.gt.up_grids.append(grid)
            self.gt.save()

            # 加仓部分，增加最后开仓价格
            if not first_open:
                sub_tns.update({"last_open_price": self.cur_99_price})
                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()

            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "price": self.cur_mi_price,
                "volume": open_volume,
                "operation": f"send short order",
                "signal": signal,
                "stop_price": stop_price
            }
            self.save_dist(d)
            return True
        else:
            self.write_error(f'[事务开空] {signal} 委托失败,开仓价：{grid.open_price}，数量：{grid.volume}'
                             f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')

        return False

    def tns_get_open_volume(self, stop_price):
        """事务计算开仓的手数
        根据投资份额，计算投入手数
        根据亏损份额，计算亏损手数
        取其最小
        """
        # 当前权益，可用资金，当前比例，最大仓位比例
        balance, avaliable, percent, percent_limit = self.cta_engine.get_account()
        invest_money = float(balance * self.max_invest_rate)
        if self.max_invest_margin > 0:
            invest_money = min(invest_money, self.max_invest_margin)

        if invest_money <= 0:
            self.write_error(
                u'没有可使用的资金：balance:{},avaliable:{},percent:{},percentLimit:{}'.format(balance, avaliable, percent,
                                                                                      percent_limit))
            return 0

        if percent > percent_limit:
            self.write_error(
                u'超过仓位限制：balance:{},avaliable:{},percent:{},percentLimit:{}'.format(balance, avaliable, percent,
                                                                                    percent_limit))
            return 0

        # 投资资金总额允许的开仓数量
        max_unit = max(1, int(invest_money / (self.cur_mi_price * self.symbol_size * self.margin_rate)))
        if self.max_invest_pos > 0:
            max_unit = min(max_unit, self.max_invest_pos)

        avaliable_unit = int(avaliable / (self.cur_mi_price * self.symbol_size * self.margin_rate))
        self.write_log(u'投资资金总额{}允许的开仓数量：{},剩余资金允许得开仓数：{}，当前已经开仓手数:{}'
                       .format(invest_money, max_unit,
                               avaliable_unit,
                               self.position.long_pos + abs(self.position.short_pos)))

        if self.single_lost_rate is not None and self.single_lost_rate < 0.1:
            # 损失金额
            lost_money = balance * self.single_lost_rate
            # 亏损金额=> 手数
            lost_unit = max(1,int(lost_money / (abs(self.cur_99_price - stop_price) * self.symbol_size)))
            self.write_log(f'投资资金总额{balance}，亏损比率:{self.single_lost_rate}=> 亏损金额{lost_money} =》亏损手数:{lost_unit}')
        else:
            lost_unit = max_unit
        return min(max_unit, avaliable_unit, lost_unit)

    def grid_check_stop(self):
        """
        网格逐一止损/止盈检查 (根据指数价格进行止损止盈）
        :return:
        """
        if self.entrust != 0:
            return

        if not self.trading:
            if not self.backtesting:
                self.write_error(u'当前不允许交易')
            return

        # 多单网格逐一止损/止盈检查：
        long_grids = self.gt.get_opened_grids_without_types(direction=Direction.LONG, types=[LOCK_GRID])

        for g in long_grids:
            # sub_tns 止损价触发
            if g.open_status and not g.order_status:
                sub_tns = self.policy.sub_tns.get(g.type, {})
                stop_price = sub_tns.get('stop_price', None)

                if stop_price and stop_price > self.cur_99_price and stop_price != g.stop_price:
                    g.stop_price = stop_price

            # 满足离场条件，
            if g.close_price and self.cur_99_price >= g.close_price \
                    and g.open_status and not g.order_status:
                dist_record = dict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_99_price
                dist_record['operation'] = 'win leave'
                dist_record['signals'] = '{}>{}'.format(self.cur_99_price, g.close_price)
                # 止损离场
                self.write_log(u'{} 指数价:{} 触发多单止盈线{},{}当前价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                               format(self.cur_datetime, self.cur_99_price, g.close_price, self.vt_symbol,
                                      self.cur_mi_price,
                                      g.open_price, g.snapshot.get('open_price'), g.volume))
                self.save_dist(dist_record)

                if self.tns_close_long_pos(g):
                    self.write_log(u'多单止盈委托成功')
                    self.tns_remove_signal(g.type)
                    if self.policy.last_sell_bi_end != self.kline_y.cur_bi.end:
                        self.policy.last_sell_bi_end = self.kline_y.cur_bi.end
                else:
                    self.write_error(u'多单止盈委托失败')

            # 碰到止损价格
            if g.stop_price > 0 and g.stop_price > self.cur_99_price \
                    and g.stop_price > self.kline_x.close_array[-1] \
                    and g.open_status and not g.order_status:
                dist_record = dict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_99_price
                dist_record['operation'] = 'stop leave'
                dist_record['signals'] = '{}<{}'.format(self.cur_99_price, g.stop_price)
                # 止损离场
                self.write_log(u'{} 指数价:{} 触发多单止损线{},{}当前价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                               format(self.cur_datetime, self.cur_99_price, g.stop_price, self.vt_symbol,
                                      self.cur_mi_price,
                                      g.open_price, g.snapshot.get('open_price'), g.volume))
                self.save_dist(dist_record)

                # 离场时也要更新，避免再次重复开仓
                self.policy.last_long_bi_start = self.kline_x.cur_bi.start

                if self.tns_close_long_pos(g):
                    self.write_log(u'多单止盈/止损委托成功')
                    if self.policy.last_sell_bi_end != self.kline_y.cur_bi.end:
                        self.policy.last_sell_bi_end = self.kline_y.cur_bi.end
                else:
                    self.write_error(u'多单止损委托失败')

        # 空单网格止损检查
        short_grids = self.gt.get_opened_grids_without_types(direction=Direction.SHORT, types=[LOCK_GRID])
        for g in short_grids:
            # sub_tns 止损价触发
            if g.open_status and not g.order_status:
                sub_tns = self.policy.sub_tns.get(g.type, {})
                stop_price = sub_tns.get('stop_price', None)
                if stop_price and stop_price < self.cur_99_price and stop_price != g.stop_price:
                    g.stop_price = stop_price

            if g.close_price and g.close_price >= self.cur_99_price \
                    and g.open_status and not g.order_status:
                dist_record = dict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_99_price
                dist_record['operation'] = 'stop leave'
                dist_record['signals'] = '{}>={}'.format(self.cur_99_price, g.close_price)
                # 网格止损
                self.write_log(u'{} 指数价:{} 触发空单止盈线:{},{}最新价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                               format(self.cur_datetime, self.cur_99_price, g.close_price, self.vt_symbol,
                                      self.cur_mi_price,
                                      g.open_price, g.snapshot.get('open_price'), g.volume))
                self.save_dist(dist_record)

                if self.tns_close_short_pos(g):
                    self.write_log(u'空单止盈委托成功')
                    self.tns_remove_signal(g.type)
                    if self.policy.last_cover_bi_end != self.kline_y.cur_bi.end:
                        self.policy.last_cover_bi_end = self.kline_y.cur_bi.end
                else:
                    self.write_error(u'委托空单平仓失败')

            if g.stop_price > 0 and g.stop_price < self.cur_99_price \
                    and g.stop_price < self.kline_x.close_array[-1] \
                    and g.open_status and not g.order_status:
                dist_record = dict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_99_price
                dist_record['operation'] = 'stop leave'
                dist_record['signals'] = '{}<{}'.format(self.cur_99_price, g.stop_price)
                # 网格止损
                self.write_log(u'{} 指数价:{} 触发空单止损线:{},{}最新价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                               format(self.cur_datetime, self.cur_99_price, g.stop_price, self.vt_symbol,
                                      self.cur_mi_price,
                                      g.open_price, g.snapshot.get('open_price'), g.volume))
                self.save_dist(dist_record)

                # 离场时也要更新，避免再次重复开仓
                self.policy.last_short_bi_start = self.kline_x.cur_bi.start

                if self.tns_close_short_pos(g):
                    self.write_log(u'空单止盈/止损委托成功')
                    if self.policy.last_cover_bi_end != self.kline_y.cur_bi.end:
                        self.policy.last_cover_bi_end = self.kline_y.cur_bi.end
                else:
                    self.write_error(u'委托空单平仓失败')

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return
        self.write_log(u'{} 当前指数{}价格:{},当前主力{}价格：{}'
                       .format(self.cur_datetime,
                               self.idx_symbol, self.cur_99_price,
                               self.vt_symbol, self.cur_mi_price))

        x_info = ""
        if self.kline_x.duan_list:
            duan = self.kline_x.duan_list[-1]
            x_info += "[线段] 方向:{}, 开始[{}]价格:{} =>[{}]价格:{}\n".format(
                '多' if duan.direction == 1 else '空',
                duan.start,
                duan.low if duan.direction == 1 else duan.high,
                duan.end,
                duan.high if duan.direction == 1 else duan.low
            )
        if self.kline_x.bi_zs_list:
            zs = self.kline_x.bi_zs_list[-1]
            x_info += "[中枢] 方向:{}, 开始[{}]价格:{} =>[{}]价格:{}\n".format(
                '多' if zs.direction == 1 else '空',
                zs.start,
                zs.low if zs.direction == 1 else zs.high,
                zs.end,
                zs.high if zs.direction == 1 else zs.low
            )

        if self.kline_x.bi_list:
            bi = self.kline_x.bi_list[-1]
            x_info += "[分笔] 方向:{}, 开始[{}]价格:{} =>[{}]价格:{}\n".format(
                '多' if bi.direction == 1 else '空',
                bi.start,
                bi.low if bi.direction == 1 else bi.high,
                bi.end,
                bi.high if bi.direction == 1 else bi.low
            )

        if self.kline_x.fenxing_list:
            fx = self.kline_x.fenxing_list[-1]
            x_info += "[分形] 类型:{}, [{}]价格:{} {}\n".format(
                '顶' if fx.direction == 1 else '底',
                fx.index,
                fx.high if fx.direction == 1 else fx.low,
                '未完成' if fx.is_rt else '完成'
            )
        if len(x_info) > 0:
            self.write_log(self.kline_x.name + ":\n" + x_info)

        y_info = ""
        if self.kline_y.duan_list:
            duan = self.kline_y.duan_list[-1]
            y_info += "[线段] 方向:{}, 开始[{}]价格:{} =>[{}]价格:{}\n".format(
                '多' if duan.direction == 1 else '空',
                duan.start,
                duan.low if duan.direction == 1 else duan.high,
                duan.end,
                duan.high if duan.direction == 1 else duan.low
            )
        if self.kline_y.bi_zs_list:
            zs = self.kline_y.bi_zs_list[-1]
            y_info += "[中枢] 方向:{}, 开始[{}]价格:{} =>[{}]价格:{}\n".format(
                '多' if zs.direction == 1 else '空',
                zs.start,
                zs.low if zs.direction == 1 else zs.high,
                zs.end,
                zs.high if zs.direction == 1 else zs.low
            )

        if self.kline_y.bi_list:
            bi = self.kline_y.bi_list[-1]
            y_info += "[分笔] 方向:{}, 开始[{}]价格:{} =>[{}]价格:{}\n".format(
                '多' if bi.direction == 1 else '空',
                bi.start,
                bi.low if bi.direction == 1 else bi.high,
                bi.end,
                bi.high if bi.direction == 1 else bi.low
            )

        if self.kline_y.fenxing_list:
            fx = self.kline_y.fenxing_list[-1]
            y_info += "[分形] 类型:{}, [{}]价格:{} {}\n".format(
                '顶' if fx.direction == 1 else '底',
                fx.index,
                fx.high if fx.direction == 1 else fx.low,
                '未完成' if fx.is_rt else '完成'
            )
        if len(y_info) > 0:
            self.write_log(self.kline_y.name + ":\n" + y_info)

        self.write_log(u'当前Policy:{}'.format(self.policy.sub_tns))


class S153_Policy_v4(CtaPolicy):
    """S153策略配套得事务"""

    def __init__(self, strategy):
        super().__init__(strategy)
        self.last_long_bi_start = ""  # 最后一次开多位置得所在分笔开始时间
        self.last_short_bi_start = "" # 最后一次开空位置所在分笔开始时间
        self.last_sell_bi_end = ""    # 最后一次多单平仓时，所在分笔得结束时间
        self.last_cover_bi_end = ""   # 最后一次空单平仓时，所在分笔得结束时间

        self.sub_tns = {}

    def to_json(self):
        """
        将数据转换成dict
        :return:
        """
        j = super().to_json()

        j['sub_tns'] = self.sub_tns
        j['last_long_bi_start'] = self.last_long_bi_start
        j['last_short_bi_start'] = self.last_short_bi_start
        j['last_sell_bi_end'] = self.last_sell_bi_end
        j['last_cover_bi_end'] = self.last_cover_bi_end

        return j

    def from_json(self, json_data):
        """
        将dict转化为属性
        :param json_data:
        :return:
        """
        super().from_json(json_data)

        self.sub_tns = json_data.get('sub_tns', {})
        self.last_long_bi_start = json_data.get('last_long_bi_start', "")
        self.last_short_bi_start = json_data.get('last_short_bi_start', "")
        self.last_sell_bi_end = json_data.get('last_sell_bi_end', "")
        self.last_cover_bi_end = json_data.get('last_cover_bi_end', "")


    def clean(self):
        """清除"""
        self.chan_signals = {}
        self.sub_tns = {}
