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


#######################################################################
class Strategy153_Chan_Three_V1(CtaProFutureTemplate):
    """
    缠论策略系列-3买、3卖策略
    三买信号：中枢突破后的三买三卖
    离场信号：有效下破中枢，或者次级别出现顶背驰信号或三卖信号

    """

    author = u'华富资产'

    bar_names = "M3-M15"  # 次级别K线，当前级别K线

    single_lost_rate = None  # 单此投入亏损率, 0 ~0.1

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "single_lost_rate",
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

        self.policy = S153_Policy(self)  # 执行策略
        self.kline_x = None  # 当前级别K线
        self.kline_y = None  # 次级别K线
        self.klines = {}
        self.init_past_3_4 = False  # 初始化经过3/4时间

        self.volume_tick = 1
        self.last_minute = None
        self.debug_dates = ['2016-04-14','2016-05-25', '2018-08-20', '2018-03-09', '2018-08-28']
        if setting:
            self.update_setting(setting)

            volume_tick = self.cta_engine.get_volume_tick(self.vt_symbol)
            if volume_tick != self.volume_tick:
                self.volume_tick = volume_tick
                self.write_log(f'{self.vt_symbol}的最小成交数量是{self.volume_tick}')

            # bar_names: 次级别K线_本级别K线
            y_name, x_name = self.bar_names.split('-')
            # 创建X K线
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
            line_x_setting['para_active_skd'] = True
            line_x_setting['price_tick'] = self.price_tick  # 合约最小跳动
            line_x_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()  # 短合约
            self.kline_x = kline_class(self, self.on_bar_x, line_x_setting)
            self.klines.update({self.kline_x.name: self.kline_x})

            # 创建的Y K线
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
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ema1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ema2', 'type_': 'list'},
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
            self.kline_x.add_bar(copy.copy(bar))
            # 如果次级别为1分钟
            if self.kline_y.bar_interval == 1 and self.kline_y.interval == Interval.MINUTE:
                self.kline_y.add_bar(bar, bar_is_completed=True)
            else:
                self.kline_y.add_bar(bar)

            # 处理信号子事务，进一步发掘开仓
            self.tns_process_sub()

        except Exception as ex:
            self.write_error(u'{},{}'.format(str(ex), traceback.format_exc()))

    def on_bar_x(self, bar):
        """
        x分钟K线OnBar事件
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
        :return:
        """
        # 排除x线段级别的顶背驰
        # duan_beichi_signals = [check_chan_xt(kline=self.kline_x, bi_list=self.kline_x.duan_list[-n:]) for n in
        #                        range(5, 15, 2)]
        # if ChanSignals.SA0.value in duan_beichi_signals:
        #     return

        signal_type = None

        zs_info = {}
        # x三买信号
        x_zs_3rd_buy = check_zs_3rd(big_kline=self.kline_x,
                                  small_kline=None,
                                  signal_direction=Direction.LONG,
                                  first_zs=False,
                                  all_zs=False)
        if x_zs_3rd_buy:
            signal_type = '中枢三买'
            zs_info = {
                "zs_start": str(self.kline_x.cur_bi_zs.start),
                "zs_end": str(self.kline_x.cur_bi_zs.end),
                "zs_high": float(self.kline_x.cur_bi_zs.high),
                "zs_low": float(self.kline_x.cur_bi_zs.low),
                "zs_height": float(self.kline_x.cur_bi_zs.height),
                "zs_middle": (float(self.kline_x.cur_bi_zs.high) + float(self.kline_x.cur_bi_zs.low)) / 2
            }

        # 通过分笔形态识别的三买信号
        if check_bi_not_rt(self.kline_x, direction=Direction.SHORT):
            for n in [5, 9]:
                # 通过指定n笔（不限于线段），获取其分笔方法
                signal = check_chan_xt(self.kline_x, self.kline_x.bi_list[-n:])
                if signal in [ChanSignals.LI0.value]:
                    signal_type = f'{n}分笔三买'
                    if self.kline_x.cur_bi_zs.end < self.kline_x.bi_list[-n].end:
                        if n == 5:
                            zs_info = {
                                "zs_start": str(self.kline_x.bi_list[-5].start),
                                "zs_end": str(self.kline_x.bi_list[-3].end),
                                "zs_high": min(float(self.kline_x.bi_list[-5].high),
                                               float(self.kline_x.bi_list[-3].high)),
                                "zs_low": max(float(self.kline_x.bi_list[-5].low), float(self.kline_x.bi_list[-3].low))
                            }
                        # elif n == 7 :
                        #     zs_info = {
                        #         "zs_start": str(self.kline_x.bi_list[-7].start),
                        #         "zs_end": str(self.kline_x.bi_list[-5].end),
                        #         "zs_high": min(float(self.kline_x.bi_list[-5].high), float(self.kline_x.bi_list[-7].high)),
                        #         "zs_low": max(float(self.kline_x.bi_list[-5].low), float(self.kline_x.bi_list[-7].low))
                        #     }
                        else:  # n = 9
                            zs_info = {
                                "zs_start": str(self.kline_x.bi_list[-9].start),
                                "zs_end": str(self.kline_x.bi_list[-5].end),
                                "zs_high": min(float(self.kline_x.bi_list[-5].high),
                                               float(self.kline_x.bi_list[-7].high),
                                               float(self.kline_x.bi_list[-9].high)),
                                "zs_low": max(float(self.kline_x.bi_list[-5].low), float(self.kline_x.bi_list[-7].low),
                                              float(self.kline_x.bi_list[-9].low))
                            }
                        zs_info["zs_height"] = float(zs_info['zs_high'] - zs_info['zs_low'])
                        zs_info["zs_middle"] = float(zs_info['zs_high'] - zs_info['zs_low']) / 2
                    else:
                        zs_info = {
                            "zs_start": str(self.kline_x.cur_bi_zs.start),
                            "zs_end": str(self.kline_x.cur_bi_zs.end),
                            "zs_high": float(self.kline_x.cur_bi_zs.high),
                            "zs_low": float(self.kline_x.cur_bi_zs.low),
                            "zs_height": float(self.kline_x.cur_bi_zs.height),
                            "zs_middle": float(self.kline_x.cur_bi_zs.bi_list[0].middle)
                        }
                    break

        # 这里自行添加更多的三买信号

        # 发现了三买信号
        if signal_type:
            # 检查上升线段的最后一笔，对应的次级别走势，是否有背驰，并且当前是否存在三卖

            # 记录信号的相关信息
            sub_tns = {
                "status": TNS_STATUS_OBSERVATE,
                "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                "signal_type": signal_type,
                'duan_low': float(self.kline_x.cur_duan.low),
                'duan_high': float(self.kline_x.cur_duan.high),
                'duan_start': self.kline_x.cur_duan.start,
                'duan_end': self.kline_x.cur_duan.end,
                'bi_start': self.kline_x.cur_bi.start,
                'bi_end': self.kline_x.cur_bi.end,
                'bi_high': float(self.kline_x.cur_bi.high),
                'bi_low': float(self.kline_x.cur_bi.low)
            }
            sub_tns.update(zs_info)
            # 添加到sub_tns的逻辑记录中
            self.write_log(f'添加{SIGNAL_THREE_BUY}子事务')
            self.policy.sub_tns[SIGNAL_THREE_BUY] = sub_tns
            if SIGNAL_THREE_SHORT in self.policy.sub_tns \
                    and self.policy.sub_tns[SIGNAL_THREE_SHORT]['status'] == TNS_STATUS_OBSERVATE:
                self.write_log(f'移除{SIGNAL_THREE_SHORT}观测事务')
                self.policy.sub_tns.pop(SIGNAL_THREE_SHORT, None)
            self.policy.save()

            # 格式化日志
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"{signal_type}",
                "signal": SIGNAL_THREE_BUY
            }
            self.save_dist(d)

    def tns_discover_three_short_signal(self):
        """
        发掘x三卖信号
        :return:
        """
        # # 排除x线段级别的顶背驰
        # duan_beichi_signals = [check_chan_xt(kline=self.kline_x, bi_list=self.kline_x.duan_list[-n:]) for n in
        #                        range(5, 15, 2)]
        # if ChanSignals.LA0.values in duan_beichi_signals:
        #     return

        signal_type = None
        zs_info = {}
        # x三卖信号
        x_zs_3rd_short = check_zs_3rd(big_kline=self.kline_x,
                                    small_kline=None,
                                    signal_direction=Direction.SHORT,
                                    first_zs=False,
                                    all_zs=False)
        if x_zs_3rd_short:
            signal_type = '中枢三卖'
            zs_info = {
                "zs_start": str(self.kline_x.cur_bi_zs.start),
                "zs_end": str(self.kline_x.cur_bi_zs.end),
                "zs_high": float(self.kline_x.cur_bi_zs.high),
                "zs_low": float(self.kline_x.cur_bi_zs.low),
                "zs_height": float(self.kline_x.cur_bi_zs.height),
                "zs_middle": (float(self.kline_x.cur_bi_zs.high) + float(self.kline_x.cur_bi_zs.low)) / 2
            }
        # 通过分笔形态识别的三卖信号
        if check_bi_not_rt(self.kline_x, direction=Direction.LONG):
            for n in [5, 9]:
                # 通过指定n笔（不限于线段），获取其分笔方法
                signal = check_chan_xt(self.kline_x, self.kline_x.bi_list[-n:])
                if signal in [ChanSignals.SI0.value]:
                    signal_type = f'{n}分笔三卖'
                    if self.kline_x.cur_bi_zs.end < self.kline_x.bi_list[-n].end:
                        if n == 5:
                            zs_info = {
                                "zs_start": str(self.kline_x.bi_list[-5].start),
                                "zs_end": str(self.kline_x.bi_list[-3].end),
                                "zs_high": min(float(self.kline_x.bi_list[-5].high),
                                               float(self.kline_x.bi_list[-3].high)),
                                "zs_low": max(float(self.kline_x.bi_list[-5].low), float(self.kline_x.bi_list[-3].low))
                            }
                        # elif n == 7 :
                        #     zs_info = {
                        #         "zs_start": str(self.kline_x.bi_list[-7].start),
                        #         "zs_end": str(self.kline_x.bi_list[-5].end),
                        #         "zs_high": min(float(self.kline_x.bi_list[-5].high), float(self.kline_x.bi_list[-7].high)),
                        #         "zs_low": max(float(self.kline_x.bi_list[-5].low), float(self.kline_x.bi_list[-7].low))
                        #     }
                        else:  # n = 9
                            zs_info = {
                                "zs_start": str(self.kline_x.bi_list[-9].start),
                                "zs_end": str(self.kline_x.bi_list[-5].end),
                                "zs_high": min(float(self.kline_x.bi_list[-5].high),
                                               float(self.kline_x.bi_list[-7].high),
                                               float(self.kline_x.bi_list[-9].high)),
                                "zs_low": max(float(self.kline_x.bi_list[-5].low), float(self.kline_x.bi_list[-7].low),
                                              float(self.kline_x.bi_list[-9].low))
                            }
                        zs_info["zs_height"] = float(zs_info['zs_high'] - zs_info['zs_low'])
                        zs_info["zs_middle"] = float(zs_info['zs_high'] - zs_info['zs_low']) / 2
                    else:
                        zs_info = {
                            "zs_start": str(self.kline_x.cur_bi_zs.start),
                            "zs_end": str(self.kline_x.cur_bi_zs.end),
                            "zs_high": float(self.kline_x.cur_bi_zs.high),
                            "zs_low": float(self.kline_x.cur_bi_zs.low),
                            "zs_height": float(self.kline_x.cur_bi_zs.height),
                            "zs_middle": float(self.kline_x.cur_bi_zs.bi_list[0].middle)
                        }
                    break

        # 这里可以补充更多的三卖信号，例如小转大[本级别分笔超长，次级别存在趋势背驰]

        # 存在三卖信号
        if signal_type:

            sub_tns = {
                "status": TNS_STATUS_OBSERVATE,
                "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                "signal_type": signal_type,

                'duan_low': float(self.kline_x.cur_duan.low),
                'duan_high': float(self.kline_x.cur_duan.high),
                'duan_start': self.kline_x.cur_duan.start,
                'duan_end': self.kline_x.cur_duan.end,
                'bi_start': self.kline_x.cur_bi.start,
                'bi_end': self.kline_x.cur_bi.end,
                'bi_high': float(self.kline_x.cur_bi.high),
                'bi_low': float(self.kline_x.cur_bi.low)
            }
            sub_tns.update(zs_info)
            # 更新到sub_tns逻辑事务中
            self.policy.sub_tns[SIGNAL_THREE_SHORT] = sub_tns
            if SIGNAL_THREE_BUY in self.policy.sub_tns \
                    and self.policy.sub_tns[SIGNAL_THREE_BUY]['status'] == TNS_STATUS_OBSERVATE:
                self.write_log(f'移除{SIGNAL_THREE_BUY}观测事务')
                self.policy.sub_tns.pop(SIGNAL_THREE_BUY, None)
            self.policy.save()

            # 格式化写入日志
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"{signal_type}",
                "signal": SIGNAL_THREE_SHORT
            }
            self.save_dist(d)

    def tns_process_sub(self):
        """处理各种子事务，"""
        if not self.kline_x.pre_duan:
            return

        for signal in list(self.policy.sub_tns.keys()):

            # 三买做多信号
            if signal == SIGNAL_THREE_BUY:
                self.tns_process_three_buy(signal)
                continue

            # 三卖做空信号
            if signal == SIGNAL_THREE_SHORT:
                self.tns_process_three_short(signal)
                continue

    def tns_process_three_buy(self, signal):
        """
        处理三买子事务
        观测、就绪、开仓、持仓时主动离场等
        """
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')

        # 买入观测状态
        if status == TNS_STATUS_OBSERVATE:

            # 如果还没买入时，当前分型高位低于zs_high，则取消其信号
            if check_bi_not_rt(self.kline_x, Direction.SHORT) \
                    and self.kline_x.cur_fenxing.direction == -1 \
                    and self.kline_x.cur_fenxing.high < sub_tns.get('zs_high'):
                self.write_log(f'下跌分笔落入中枢区间，取消其观测状态')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> exit",
                    "signal": SIGNAL_THREE_BUY
                }
                self.save_dist(d)
                self.policy.sub_tns.pop(signal, None)
                self.policy.save()
                return

            # 回抽分笔期间，没有次级别买入点，直接产生新高，取消当前三买信号
            if self.kline_x.cur_duan.direction == 1 \
                    and self.kline_x.cur_duan.high > sub_tns.get('duan_high'):
                self.write_log(f'上涨线段延长，取消其观测状态')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> exit",
                    "signal": SIGNAL_THREE_BUY
                }
                self.save_dist(d)
                self.policy.sub_tns.pop(signal, None)
                self.policy.save()
                return

            # 观测期内，下跌笔在延长，继续观测
            if self.kline_x.cur_bi.direction == -1 \
                    and self.kline_x.cur_bi.start == sub_tns.get('bi_start') \
                    and sub_tns.get('zs_high') < float(self.kline_x.cur_bi.low) < sub_tns.get('bi_high') \
                    and check_bi_not_rt(self.kline_x, Direction.SHORT):
                self.write_log(f'回调笔继续延长，未进入中枢')
                sub_tns.update({'bi_end': self.kline_x.cur_bi.end, 'bi_low': float(self.kline_x.cur_bi.low)})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> extend",
                    "signal": SIGNAL_THREE_BUY
                }
                self.save_dist(d)
                self.policy.save()

            # 观测期间，寻找入场点：
            # 在本级别下跌笔中，
            # 寻找次级别的背驰买点，次级别线段下跌，末笔是下跌+底分型+背驰
            if check_bi_not_rt(self.kline_x, Direction.SHORT) \
                    and self.kline_y.cur_duan.start >= sub_tns.get('duan_end') \
                    and check_bi_not_rt(self.kline_y, Direction.SHORT):
                y_signal_type = None

                # 入场点1：次级别当前笔和段末端一致情况下的底背驰
                if check_duan_not_rt(self.kline_y, Direction.SHORT):
                    # 入场点1.1: 5分笔及以上出现底背驰信号
                    if len(self.kline_y.cur_duan.bi_list) >= 5 \
                            and check_chan_xt(self.kline_y, self.kline_y.cur_duan.bi_list) in DI_BEICHI_SIGNALS:
                        y_signal_type = f'{self.kline_y.name}底背驰'

                    # 入场点1.2:三分笔，力度背驰
                    elif len(self.kline_y.cur_duan.bi_list) == 3:
                        if self.kline_y.cur_duan.bi_list[0].height > self.kline_y.cur_duan.bi_list[-1].height \
                                and self.kline_y.cur_duan.bi_list[0].atan > self.kline_y.cur_duan.bi_list[-1].atan:
                            y_signal_type = f'{self.kline_y.name}三笔背驰'

                    # 入场点1.3: macd dif底背离
                    if not y_signal_type and \
                            self.kline_y.is_fx_macd_divergence(
                                direction=Direction.SHORT,
                                cur_duan=self.kline_y.cur_duan):
                        y_signal_type = f'{self.kline_y.name}末三笔DIF背驰'

                # 入场点2： 寻找次级别下跌线段被打破后的二买信号
                elif self.kline_y.bi_list[-2].start == self.kline_y.cur_duan.end:
                    # 入场点2.1: 当前次级别下跌线段包含多个分笔
                    if len(self.kline_y.cur_duan.bi_list) > 1:
                        second_low = min([bi.low for bi in self.kline_y.cur_duan.bi_list if bi.low != self.kline_y.cur_duan.low])
                        if self.kline_y.bi_list[-2].high > second_low:
                            y_signal_type = f'{self.kline_y.name}下跌段二买'
                    # 入场点2.2: 次级别下跌线段只有一个分笔,并且该下跌线段不能够是回调后的第一个线段
                    elif self.kline_y.cur_duan.start > sub_tns.get('duan_end'):
                        if self.kline_y.bi_list[-2].atan > self.kline_y.bi_list[-1].atan:
                            y_signal_type = f'{self.kline_y.name}下跌段二买'

                # 找到入场点了 => 转移至开仓就绪
                if y_signal_type:
                    status = TNS_STATUS_READY
                    sub_tns.update({"status": status, 'max_high': float(self.kline_x.cur_duan.high)})
                    self.policy.sub_tns.update({signal: sub_tns})
                    self.policy.save()
                    self.write_log(f'{signal} {TNS_STATUS_OBSERVATE} => {TNS_STATUS_READY}')
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{y_signal_type}",
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)

        # 买入就绪状态
        if status == TNS_STATUS_READY:
            # 止损价, 当前三买信号的下跌线段最低点
            stop_price = min(sub_tns.get('zs_middle'), sub_tns.get('bi_low'))
            # 止盈价：当前线段高点+当前线段高度
            win_price = float(self.kline_x.cur_duan.high + self.kline_x.cur_duan.height)

            # 满足止损价之上，且当前开仓位置，与上一次发生止损时的开仓价，不在同一笔
            if self.cur_99_price > stop_price \
                    and self.kline_y.cur_bi.start != self.policy.last_long_bi_start:
                # 开始发单买入
                self.tns_buy(signal, stop_price, win_price)
                status = TNS_STATUS_ORDERING
                sub_tns.update({'status': status,
                                'open_bi_start': self.kline_x.cur_bi.start,
                                'last_open_price': self.cur_99_price,
                                'stop_price': stop_price,
                                'win_price': win_price})

                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()

        # 委托状态
        if status == TNS_STATUS_ORDERING:

            # 判断是否已经开仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[SIGNAL_THREE_BUY])
            if len(open_grids) > 0:
                self.write_log(f'策略已经持有多单，不再开仓')
                self.write_log(f'{signal} 已持仓.  {status} => {TNS_STATUS_OPENED}')
                status = TNS_STATUS_OPENED
                sub_tns.update({"status": status})
                # 记录下当前委托的所在分笔开始时间。用于避免打止损后重复开仓
                self.policy.last_long_bi_start = self.kline_x.cur_bi.start
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}=> {TNS_STATUS_OPENED}",
                    "signal": SIGNAL_THREE_BUY
                }
                self.save_dist(d)
                self.policy.save()
                return
            else:
                if len(self.active_orders) == 0 and self.entrust == 0:
                    status = TNS_STATUS_READY
                    self.write_log(f'策略未持有多单，修正状态')
                    sub_tns.update({"status": status})
                    sub_tns.pop('open_bi_start', None)
                    sub_tns.pop('last_open_price', None)
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}=> {TNS_STATUS_READY}",
                        "signal": SIGNAL_THREE_BUY
                    }
                    self.save_dist(d)
                    self.policy.save()
                    return

        # 持仓状态
        if status == TNS_STATUS_OPENED:
            # => 判断是否已经平仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[SIGNAL_THREE_BUY])
            if len(open_grids) == 0:
                self.policy.sub_tns.pop(signal, None)
                self.write_log(f'当前{SIGNAL_THREE_BUY}信号退出')

            # 持仓后，识别本级别&次级别的背驰信号，在背驰信号后的次级别三卖时，离场
            else:
                # 当前最新的事务止损价格
                stop_price = sub_tns.get('stop_price', None)

                # 更新下事务内的最高价格
                if float(self.kline_x.cur_duan.high) > sub_tns.get('max_high',0):
                    sub_tns['max_high'] = float(self.kline_x.cur_duan.high)

                # 识别1：本级别趋势背驰（告警）
                if check_duan_not_rt(self.kline_x, Direction.LONG) \
                        and check_bi_not_rt(self.kline_x, Direction.LONG) \
                        and self.kline_x.cur_bi.end != sub_tns.get('x_beichi_bi_end'):
                    x_signal_type = None
                    # 根据本级别的分笔形态（5~13笔），判断是否有顶背驰信号
                    duan_beichi_signals = [check_chan_xt(kline=self.kline_x,
                                                         bi_list=self.kline_x.duan_list[-n:]) for n
                                           in
                                           range(5, 15, 2)]
                    # 识别1.1 顶背驰
                    if ChanSignals.SA0.value in duan_beichi_signals:
                        self.write_log(f'{self.kline_x.name}出现{ChanSignals.SA0.value}，多单告警')
                        if sub_tns['stop_price'] < float(self.kline_x.cur_bi_zs.high):
                            self.write_log(f'修改止损价至{self.kline_x.name}线段中枢顶部{float(self.kline_x.cur_bi_zs.high)}')
                            sub_tns['stop_price'] = float(self.kline_x.cur_bi_zs.high)
                        x_signal_type = f"本级别{ChanSignals.SA0.value}"

                    # 识别1.2 双重顶背驰
                    if ChanSignals.SB0.value in duan_beichi_signals:
                        self.write_log(f'{self.kline_x.name}出现{ChanSignals.SB0.value}，多单告警')
                        if sub_tns['stop_price'] < float(self.kline_x.cur_bi_zs.high):
                            self.write_log(f'修改止损价至{self.kline_x.name}线段中枢顶部{float(self.kline_x.cur_bi_zs.high)}')
                            sub_tns['stop_price'] = float(self.kline_x.cur_bi_zs.high)
                        x_signal_type = f"本级别{ChanSignals.SB0.value}"

                    # 识别1.3 判断是否有上涨趋势顶背驰一卖信号
                    if check_qsbc_1st(big_kline=self.kline_x,
                                        small_kline=None,
                                        signal_direction=Direction.SHORT):
                        self.write_log(f'{self.kline_x.name}出现上涨趋势顶背驰一卖信号')
                        if sub_tns['stop_price'] < float(self.kline_x.cur_bi_zs.high):
                            self.write_log(f'修改止损价至中枢上沿')
                            sub_tns['stop_price'] = float(self.kline_x.cur_bi_zs.high)
                        x_signal_type = f"本级别上涨趋势顶背驰一卖"

                    # 识别1.4 判断本级别的线段后两个分笔顶分型，是否有MACD DIF值顶背离
                    if self.kline_x.is_fx_macd_divergence(
                            direction=Direction.LONG,
                            cur_duan=self.kline_x.cur_duan):
                        if sub_tns['stop_price'] < float(self.kline_x.cur_duan.bi_list[1].low):
                            self.write_log(f'修改止损价至上涨段第二低点')
                            sub_tns['stop_price'] = float(self.kline_x.cur_duan.bi_list[1].low)
                        x_signal_type = f"本级别上涨线段DIF顶背离"

                    if x_signal_type:
                        # 更新x底背驰的结束时间
                        sub_tns.update({'x_beichi_bi_end': self.kline_x.cur_bi.end})
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": x_signal_type,
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)

                # 识别2： 次级别出现顶背驰信号，告警
                if self.kline_x.cur_duan.direction == 1\
                        and check_duan_not_rt(self.kline_y, Direction.LONG) \
                        and check_bi_not_rt(self.kline_y, Direction.LONG) \
                        and float(self.kline_y.cur_duan.high) == sub_tns['max_high'] \
                        and self.kline_y.cur_duan.start >= self.kline_x.cur_bi_zs.end \
                        and self.kline_y.cur_bi.end != sub_tns.get('y_beichi_bi_end'):
                    y_signal_type = None

                    # 识别2.1 根据次级别线段的分笔形态，判断是否有顶背驰信号
                    exit_signal = check_chan_xt(self.kline_y, self.kline_y.cur_duan.bi_list)
                    if exit_signal in DING_BEICHI_SIGNALS:
                        self.write_log(f'{self.kline_y.name}出现{exit_signal}，多单告警')
                        if sub_tns['stop_price'] < float(self.kline_y.cur_duan.bi_list[1].low):
                            self.write_log(f'修改止损价至{self.kline_y.name}线段第二笔低部{float(self.kline_y.cur_duan.bi_list[1].low)}')
                            sub_tns['stop_price'] = float(self.kline_y.cur_duan.low)

                        # 更新x顶背驰的结束时间
                        sub_tns.update({'y_beichi_bi_end': self.kline_y.cur_bi.end})
                        y_signal_type = f"次级别{exit_signal}"

                    # 识别2.2 判断次级别是否有上涨趋势顶背驰一卖信号
                    if check_qsbc_1st(big_kline=self.kline_y, small_kline=None, signal_direction=Direction.SHORT):
                        self.write_log(f'{self.kline_y.name}出现上涨趋势顶背驰一卖信号，多单告警')
                        if sub_tns['stop_price'] < float(self.kline_y.cur_bi_zs.high):
                            self.write_log(f'修改止损价至{self.kline_y.name}中枢上沿{float(self.kline_y.cur_bi_zs.high)}')
                            sub_tns['stop_price'] = float(self.kline_y.cur_bi_zs.high)

                        # 更新x上涨趋势顶背驰一卖的结束时间
                        sub_tns.update({'y_beichi_bi_end': self.kline_y.cur_bi.end})
                        y_signal_type = f"次级别上涨趋势顶背驰一卖"

                    if y_signal_type:
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": y_signal_type,
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)

                # 离场信号1, 出现x趋势背驰或y顶背驰告警后
                if self.kline_x.cur_duan.end in [sub_tns.get('x_beichi_bi_end'), sub_tns.get('y_beichi_bi_end')] \
                        or self.kline_y.cur_duan.start in [sub_tns.get('x_beichi_bi_end'),
                                                           sub_tns.get('y_beichi_bi_end')]:

                    # 离场信号1.1 次级别下跌线段+中枢三卖
                    y_zs_3rd_short = check_zs_3rd(big_kline=self.kline_y,
                                                  small_kline=None,
                                                  signal_direction=Direction.SHORT,
                                                  first_zs=True, all_zs=False)

                    # 离场信号1.2 尝试使用次级别分笔判断形态是否有三卖
                    if not y_zs_3rd_short:
                        # 背驰最高点后，次级别出现下跌线段
                        if float(self.kline_y.cur_duan.high) <= sub_tns['max_high'] \
                                    and ((sub_tns.get('x_beichi_bi_end',None) \
                                        and self.kline_y.cur_duan.start >= sub_tns.get('x_beichi_bi_end')
                                        ) or \
                                    (sub_tns.get('y_beichi_bi_end',None) \
                                        and self.kline_y.cur_duan.start == sub_tns.get('y_beichi_bi_end'))):

                            # 最后一笔向上
                            if self.kline_y.cur_duan.direction == -1 and check_bi_not_rt(self.kline_y, Direction.LONG):
                                # 利用分笔判断是否存在5笔三卖信号
                                chan_3rd_short_signal = [check_chan_xt(self.kline_y, self.kline_y.bi_list[-n:]) for n in [5,9]]
                                if ChanSignals.SI0.value in chan_3rd_short_signal:
                                    y_zs_3rd_short = True

                    # 告警+离场信号，更新止损或即时离场
                    if y_zs_3rd_short and sub_tns['stop_price'] < float(self.kline_y.cur_bi.low):
                        self.write_log(f'{self.kline_y.name}出现三卖信号，上涨趋势结束，多单离场')

                        # 记录最后一次出现次级别三卖信号
                        self.policy.last_3rd_short_y_bi_end = self.kline_y.cur_bi.end

                        # 本级别还是上涨线段的话，给它一个机会,除非它突破
                        if self.kline_x.cur_duan.direction == 1:
                            sub_tns.update({'stop_price': float(self.kline_y.cur_bi.low)})
                        else:
                            # 直接离场
                            sub_tns.update({'stop_price': float(self.kline_y.cur_bi.high)})

                        # 更新多单的开仓笔开始时间，避免马上重新开仓
                        self.policy.last_long_bi_start = self.kline_x.cur_bi.start

                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"次级别三卖离场",
                            "signal": SIGNAL_THREE_BUY
                        }
                        self.save_dist(d)
                        return

    def tns_process_three_short(self, signal):
        """处理三卖子事务"""
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')

        # 卖出观测状态，
        if status == TNS_STATUS_OBSERVATE:

            # 如果还没做空时，当前分型低位高于zs_low，则取消其信号
            if check_bi_not_rt(self.kline_x, Direction.LONG) \
                    and self.kline_x.cur_fenxing.direction == 1 \
                    and self.kline_x.cur_fenxing.low > sub_tns.get('zs_low'):
                self.write_log(f'上涨分笔进入中枢，取消其观测状态')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> exit",
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)
                self.policy.sub_tns.pop(signal, None)
                self.policy.save()
                return

            # 观测期间，没有找到做空的背驰点，价格直接下破了，只好取消其信号
            if self.kline_x.cur_duan.direction == -1 \
                    and self.kline_x.cur_duan.low < sub_tns.get('duan_low'):
                self.write_log(f'下跌线段延长，取消其观测状态')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> exit",
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)
                self.policy.sub_tns.pop(signal, None)
                self.policy.save()
                return

            # 观测期内，上涨笔在延长,继续观测
            if self.kline_x.cur_bi.direction == 1 \
                    and self.kline_x.cur_bi.start == sub_tns.get('bi_start') \
                    and sub_tns.get('zs_low') > float(self.kline_x.cur_bi.high) > sub_tns.get('bi_high') \
                    and check_bi_not_rt(self.kline_x, Direction.LONG):
                self.write_log(f'回调笔继续延长，未进入中枢')
                sub_tns.update({'bi_end': self.kline_x.cur_bi.end, 'bi_high': float(self.kline_x.cur_bi.high)})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> extend",
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)
                self.policy.save()
                return

            # 次级别寻找入场点
            # 次级别下跌线段中形成的背驰点
            if check_bi_not_rt(self.kline_x, Direction.LONG) \
                    and self.kline_y.cur_duan.start >= sub_tns.get('duan_end') \
                    and check_bi_not_rt(self.kline_y, Direction.LONG):
                y_signal_type = None
                # 入场点1：次级别背驰点
                if check_duan_not_rt(self.kline_y, Direction.LONG):
                    # 入场点1.1：5分笔及以上出现顶背驰信号
                    if len(self.kline_y.cur_duan.bi_list) >= 5 \
                            and check_chan_xt(self.kline_y, self.kline_y.cur_duan.bi_list) in DING_BEICHI_SIGNALS:
                        y_signal_type = f'{self.kline_y.name}顶背驰'

                    # 入场点1.2：三分笔，力度背驰
                    elif len(self.kline_y.cur_duan.bi_list) == 3:
                        if self.kline_y.cur_duan.bi_list[0].height > self.kline_y.cur_duan.bi_list[-1].height \
                                and self.kline_y.cur_duan.bi_list[0].atan > self.kline_y.cur_duan.bi_list[-1].atan:
                            y_signal_type = f'{self.kline_y.name}三笔背驰'

                    # 入场点1.3：次级别上涨线段最后两个下跌分笔底分型形成的macd dif底背离
                    if not y_signal_type and self.kline_y.is_fx_macd_divergence(direction=Direction.LONG,
                                                                                cur_duan=self.kline_y.cur_duan):
                        y_signal_type = f'{self.kline_y.name}末三笔DIF底背离'

                # 入场点2：寻找次级别上涨线段被打破后的二卖信号
                elif self.kline_y.bi_list[-2].start == self.kline_y.cur_duan.end:
                    # 入场点2.1：次级别上涨线段具有N个分笔，选择打破上涨线段趋势后的二卖点
                    if len(self.kline_y.cur_duan.bi_list) > 1:
                        second_high = max([bi.low for bi in self.kline_y.cur_duan.bi_list if bi.high != self.kline_y.cur_duan.high])
                        if self.kline_y.bi_list[-2].low > second_high:
                            y_signal_type = f'{self.kline_y.name}上涨段二卖'

                    # 入场点2.2：次级别上涨线段只有一个分笔,并且该上涨线段不能够是回调后的第一个线段，寻找二卖
                    elif self.kline_y.cur_duan.start > sub_tns.get('duan_end'):
                        if self.kline_y.bi_list[-2].atan > self.kline_y.bi_list[-1].atan:
                            y_signal_type = f'{self.kline_y.name}上涨段二卖'

                # 找到入场点，转移状态至开空就绪
                if y_signal_type:
                    status = TNS_STATUS_READY
                    sub_tns.update({"status": status, 'min_low': float(self.kline_x.cur_duan.low)})
                    self.policy.sub_tns.update({signal: sub_tns})
                    self.policy.save()
                    self.write_log(f'{signal} {TNS_STATUS_OBSERVATE} => {TNS_STATUS_READY}')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_OBSERVATE}=> extend",
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)

        # 卖出就绪状态
        if status == TNS_STATUS_READY:

            stop_price = max(sub_tns.get('zs_middle'), sub_tns.get('bi_high'))
            win_price = float(self.kline_x.cur_duan.low - self.kline_x.cur_duan.height)

            # 满足止损价之下，且当前开仓位置，与上一次发生止损时的开仓价，不在同一笔
            if self.cur_99_price < stop_price \
                    and self.kline_x.cur_bi.start != self.policy.last_short_bi_start:
                self.tns_short(signal, stop_price, win_price)
                status = TNS_STATUS_ORDERING
                sub_tns.update({'status': status,
                                'open_bi_start': self.kline_x.cur_bi.start,
                                'last_open_price': self.cur_99_price,
                                'stop_price': stop_price})

                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()

        # 委托状态
        if status == TNS_STATUS_ORDERING:

            # 判断是否已经开仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[SIGNAL_THREE_SHORT])
            if len(open_grids) > 0:
                self.write_log(f'策略已经持有空单，不再开仓')
                self.write_log(f'{signal} 已持仓.  {status} => {TNS_STATUS_OPENED}')
                status = TNS_STATUS_OPENED
                sub_tns.update({"status": status})

                # 记录下当前委托的所在分笔开始时间。用于避免打止损后重复开仓
                self.policy.last_short_bi_start = self.kline_x.cur_bi.start

                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}=> {TNS_STATUS_OPENED}",
                    "signal": SIGNAL_THREE_SHORT
                }
                self.save_dist(d)
                self.policy.save()
                return
            else:
                if len(self.active_orders) == 0 and self.entrust == 0:
                    status = TNS_STATUS_READY
                    self.write_log(f'策略未持有空单，修正状态')
                    sub_tns.update({"status": status})
                    sub_tns.pop('open_bi_start', None)
                    sub_tns.pop('last_open_price', None)
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}=> {TNS_STATUS_READY}",
                        "signal": SIGNAL_THREE_SHORT
                    }
                    self.save_dist(d)
                    self.policy.save()
                    return

        if status == TNS_STATUS_OPENED:
            # 判断是否已经平仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[SIGNAL_THREE_SHORT])
            if len(open_grids) == 0:
                self.write_log(f'当前{SIGNAL_THREE_SHORT}信号退出')
                self.policy.sub_tns.pop(signal, None)
            else:
                # 事务止损价
                stop_price = sub_tns.get('stop_price', None)
                # 更新事务的最低价
                if float(self.kline_x.cur_duan.low) < sub_tns.get('min_low'):
                    sub_tns['min_low'] = float(self.kline_x.cur_duan.low)

                # 识别1：本级别趋势离场（告警）
                if check_duan_not_rt(self.kline_x, Direction.SHORT) \
                        and check_bi_not_rt(self.kline_x, Direction.SHORT) \
                        and self.kline_x.cur_bi.end != sub_tns.get('x_beichi_bi_end'):
                    x_signal_type = None
                    # 根据线段的分笔形态，判断是否有底背驰信号
                    duan_beichi_signals = [check_chan_xt(kline=self.kline_x, bi_list=self.kline_x.duan_list[-n:]) for n
                                           in
                                           range(5, 15, 2)]
                    # 识别1.1：分笔形态顶背驰
                    if ChanSignals.LA0.value in duan_beichi_signals:
                        self.write_log(f'{self.kline_x.name}出现{ChanSignals.LA0.value}，空单告警')
                        if sub_tns['stop_price'] > float(self.kline_x.cur_bi_zs.low):
                            self.write_log(f'修改止损价至{self.kline_x.name}线段中枢顶部{float(self.kline_x.cur_bi_zs.low)}')
                            sub_tns['stop_price'] = float(self.kline_x.cur_bi_zs.low)
                        x_signal_type = f"本级别{ChanSignals.LA0.value}"
                    # 识别1.2：分笔形态双重顶背驰
                    if ChanSignals.LB0.value in duan_beichi_signals:
                        self.write_log(f'{self.kline_x.name}出现{ChanSignals.LB0.value}，空单告警')
                        if sub_tns['stop_price'] > float(self.kline_x.cur_bi_zs.low):
                            self.write_log(f'修改止损价至{self.kline_x.name}线段中枢顶部{float(self.kline_x.cur_bi_zs.low)}')
                            sub_tns['stop_price'] = float(self.kline_x.cur_bi_zs.low)
                        x_signal_type = f"本级别{ChanSignals.LB0.value}"

                    # 识别1.3：判断是否有下跌趋势底背驰一买信号
                    if check_qsbc_1st(big_kline=self.kline_x, small_kline=None, signal_direction=Direction.LONG):
                        self.write_log(f'{self.kline_x.name}出现下跌趋势底背驰一买信号')
                        if sub_tns['stop_price'] > float(self.kline_x.cur_bi_zs.low):
                            self.write_log(f'修改止损价至中枢下沿')
                            sub_tns['stop_price'] = float(self.kline_x.cur_bi_zs.low)
                        x_signal_type = f"本级别下跌趋势底背驰一买"

                    # 识别1.3：线段最后两分笔顶分型MACD DIF顶背离
                    if self.kline_x.is_fx_macd_divergence(
                            direction=Direction.SHORT,
                            cur_duan=self.kline_x.cur_duan):
                        if sub_tns['stop_price'] > float(self.kline_x.cur_duan.bi_list[1].high):
                            self.write_log(f'修改止损价至上涨段第二高点')
                            sub_tns['stop_price'] = float(self.kline_x.cur_duan.bi_list[1].high)
                        x_signal_type = f"本级别下跌线段DIF顶背离"

                    # 识别出所有本级别的告警信号
                    if x_signal_type:
                        # 更新x底背驰的结束时间
                        sub_tns.update({'x_beichi_bi_end': self.kline_x.cur_bi.end})
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": x_signal_type,
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)

                # 识别2：低级别出现底背驰信号，告警
                if self.kline_x.cur_duan.direction == -1\
                        and check_duan_not_rt(self.kline_y, Direction.SHORT) \
                        and check_bi_not_rt(self.kline_y, Direction.SHORT) \
                        and float(self.kline_y.cur_duan.low) == sub_tns['min_low'] \
                        and self.kline_y.cur_duan.start >= self.kline_x.cur_bi_zs.end \
                        and self.kline_y.cur_bi.end != sub_tns.get('y_beichi_bi_end'):
                    y_signal_type = None

                    # 识别2.1：判断是否有底背驰信号
                    exit_signal = check_chan_xt(self.kline_y, self.kline_y.cur_duan.bi_list)
                    if exit_signal in DI_BEICHI_SIGNALS:
                        self.write_log(f'{self.kline_y.name}出现{exit_signal}，空单告警')
                        if sub_tns['stop_price'] > float(self.kline_y.cur_duan.bi_list[1].high):
                            self.write_log(f'修改止损价至{self.kline_y.name}线段第二笔顶部{float(self.kline_y.cur_duan.bi_list[1].high)}')
                            sub_tns['stop_price'] = float(self.kline_y.cur_duan.high)

                        # 更新x底背驰的结束时间
                        sub_tns.update({'y_beichi_bi_end': self.kline_y.cur_bi.end})
                        y_signal_type = f"次级别{exit_signal}"

                    # 识别2.2： 判断是否有下跌趋势底背驰一买信号
                    if check_qsbc_1st(big_kline=self.kline_y, small_kline=None, signal_direction=Direction.LONG):
                        self.write_log(f'{self.kline_y.name}出现下跌趋势底背驰一买信号，空单告警')
                        if sub_tns['stop_price'] > float(self.kline_y.cur_bi_zs.low):
                            self.write_log(f'修改止损价至{self.kline_y.name}中枢上沿{float(self.kline_y.cur_bi_zs.low)}')
                            sub_tns['stop_price'] = float(self.kline_y.cur_bi_zs.low)

                        # 更新x底下跌趋势底背驰一买的结束时间
                        sub_tns.update({'y_beichi_bi_end': self.kline_y.cur_bi.end})
                        y_signal_type = f"次级别下跌趋势底背驰一买"

                    # 识别出次级别的告警信号
                    if y_signal_type:
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": y_signal_type,
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)

                # 离场信号1, 出现x趋势背驰或y底背驰后
                if self.kline_x.cur_duan.end in [sub_tns.get('x_beichi_bi_end'), sub_tns.get('y_beichi_bi_end')] \
                        or self.kline_y.cur_duan.start in [sub_tns.get('x_beichi_bi_end'),
                                                           sub_tns.get('y_beichi_bi_end')]:

                    # 离场信号1.1：次级别中枢后三买信号
                    y_zs_3rd_buy = check_zs_3rd(big_kline=self.kline_y,
                                                small_kline=None,
                                                signal_direction=Direction.LONG,
                                                first_zs=True, all_zs=False)

                    # 离场信号1.2：使用分笔形态判断是否有三买
                    if not y_zs_3rd_buy:
                        # 背驰最高点后，次级别出现下跌线段
                        if float(self.kline_y.cur_duan.low) >= sub_tns['min_low'] \
                                    and ((sub_tns.get('x_beichi_bi_end',None) \
                                        and self.kline_y.cur_duan.start >= sub_tns.get('x_beichi_bi_end')
                                        ) or \
                                    (sub_tns.get('y_beichi_bi_end',None) \
                                        and self.kline_y.cur_duan.start == sub_tns.get('y_beichi_bi_end'))):
                            # 最后一笔向上
                            if self.kline_y.cur_duan.direction == 1 and check_bi_not_rt(self.kline_y, Direction.SHORT):
                                # 利用分笔判断是否存在5笔三买信号
                                chan_3rd_buy_signal = [check_chan_xt(self.kline_y, self.kline_y.bi_list[-n:]) for n in [5,9]]
                                if ChanSignals.LI0.value in chan_3rd_buy_signal:
                                    y_zs_3rd_buy = True

                    # 找到离场信号
                    if y_zs_3rd_buy and sub_tns['stop_price'] > float(self.kline_y.cur_bi.high):
                        self.write_log(f'{self.kline_y.name}出现三买信号，下跌趋势结束，空单离场')

                        # 放入策略事务，记录次级别三买信号的分笔位置
                        self.policy.last_3rd_buy_y_bi_end = self.kline_y.cur_bi.end

                        if self.kline_x.cur_duan.direction == -1:
                            # 待再次打破才离场
                            sub_tns.update({'stop_price': float(self.kline_y.cur_bi.high)})
                        else:
                            # 直接离场
                            sub_tns.update({'stop_price': float(self.kline_y.cur_bi.low)})

                        # 更新空单的开仓笔开始时间，避免马上重新开仓
                        self.policy.last_short_bi_start = self.kline_x.cur_bi.start

                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": f"次级别三买离场",
                            "signal": SIGNAL_THREE_SHORT
                        }
                        self.save_dist(d)
                        return

    def tns_buy(self, signal, stop_price, win_price, first_open=True):
        """处理Ordering状态的tns买入处理"""
        if not (self.inited and self.trading):
            return
        if self.entrust != 0:
            return

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
            return

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
            return

        # 未开仓
        # 开仓手数
        open_volume = self.tns_get_open_volume(stop_price)

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
                sub_tns.update({"last_open_price": self.cur_mi_price})
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
        else:
            self.write_error(f'[事务开多] {signal} 委托失败,开仓价：{grid.open_price}，数量：{grid.volume}'
                             f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')

    def tns_short(self, signal, stop_price, win_price, first_open=True):
        """处理Ordering状态的tns开空处理"""
        if not (self.inited and self.trading):
            return
        if self.entrust != 0:
            return

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
            return

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
            return

        # 未开仓
        # 开仓手数
        open_volume = self.tns_get_open_volume(stop_price)

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
                sub_tns.update({"last_open_price": self.cur_mi_price})
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
        else:
            self.write_error(f'[事务开空] {signal} 委托失败,开仓价：{grid.open_price}，数量：{grid.volume}'
                             f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')

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
            lost_unit = int(lost_money / (abs(self.cur_99_price - stop_price) * self.symbol_size))
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
                    # 离场时也要更新，避免再次重复开仓
                    self.policy.last_long_bi_start = self.kline_x.cur_bi.start

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

                if self.tns_close_long_pos(g):
                    self.write_log(u'多单止盈/止损委托成功')
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
                    # 离场时也要更新，避免再次重复开仓
                    self.policy.last_short_bi_start = self.kline_x.cur_bi.start

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

                if self.tns_close_short_pos(g):
                    self.write_log(u'空单止盈/止损委托成功')
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


class S153_Policy(CtaPolicy):
    """S153策略配套得事务"""

    def __init__(self, strategy):
        super().__init__(strategy)
        self.last_long_bi_start = ""
        self.last_short_bi_start = ""
        self.last_3rd_buy_y_bi_end = ""
        self.last_3rd_short_y_bi_end = ""
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
        j['last_3rd_buy_y_bi_end'] = self.last_3rd_buy_y_bi_end
        j['last_3rd_short_y_bi_end'] = self.last_3rd_short_y_bi_end

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
        self.last_3rd_buy_y_bi_end = json_data.get('last_3rd_buy_y_bi_end', "")
        self.last_3rd_short_y_bi_end = json_data.get('last_3rd_short_y_bi_end', "")

    def clean(self):
        """清除"""

        self.sub_tns = {}
