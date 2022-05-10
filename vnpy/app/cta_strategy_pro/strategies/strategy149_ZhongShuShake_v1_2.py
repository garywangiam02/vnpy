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
from vnpy.component.chanlun.pyChanlun import ChanDuan, ChanBi, ChanBiZhongShu, ChanDuanZhongShu, ChanFenXing
from vnpy.component.cta_utility import *
from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date, append_data

SIGNAL_SHAKE_LONG = 'shake_long'  # 震荡做多
SIGNAL_SHAKE_SHORT = 'shake_short'  # 震荡做空

STATUS_NORMAL = 'normal'  # 【做多为例】开仓后，Zn 在Z上方
STATUS_CHALLENGE = 'challenge'  # 【做多为例】开仓后，Zn 在Z下方，B点上方
STATUS_DANGER = 'danger'  # 【做多为例】开仓后，Zn 在B下方


#######################################################################
class Strategy149_ZhongShuShake_V1_2(CtaProFutureTemplate):
    """
    缠论策略系列-中枢震荡策略
    开仓方向：两个连续中枢的方向，中枢不能是包含关系
    中枢类型为平台balance或防守型defend，按照2中枢的前进方向开仓。
    V1.2:
    - 当前级别判断[做多为例]：
    - 两个连续上涨中枢, 下跌一线段，线段含三分笔以上，线段动量弱，线段分笔底背驰或skd底背驰
    - 进场位置：次级别分笔为多；价格上破当前级别[-3]分笔底部，或者当前级别分笔形成低分型
    - 盈亏比：2， 止损布林下轨- 标准差
    - 主动止盈价 => 中枢上轨 或布林上轨 或 次级别上涨线段+顶背驰
    - 收盘前离场
    - 信号2: 三角形收敛；信号3：单中枢离开；信号4：布林圆弧
    """

    author = u'大佳'

    bar_names = "S15-M5"  # 次级别K线，当前级别K线
    export_csv = []  # 回测时，输出到K线csv文件，全空白时，全输出；有指定时，为白名单

    single_lost_rate = None  # 单此投入亏损率, 0 ~0.1
    win_lost_rate = 2  # 盈亏比计算
    force_leave_times = ['1450', '0210', '2250']  # 主动离场小时+分钟

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "single_lost_rate", "win_lost_rate", "export_csv","force_leave_times",
        "bar_names", "backtesting", "cancel_seconds"]

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

        self.policy = S149_Policy(self)  # 执行策略
        self.kline_x = None  # 次级别K线
        self.kline_y = None  # 当前级别K线
        self.klines = {}
        self.init_past_3_4 = False  # 初始化经过3/4时间

        self.volume_tick = 1
        self.last_minute = None

        if setting:
            self.update_setting(setting)

            volume_tick = self.cta_engine.get_volume_tick(self.vt_symbol)
            if volume_tick != self.volume_tick:
                self.volume_tick = volume_tick
                self.write_log(f'{self.vt_symbol}的最小成交数量是{self.volume_tick}')

            # 创建X K线
            x_name, y_name = self.bar_names.split('-')
            line_x_setting = {}
            kline_class, interval_num = get_cta_bar_type(x_name)
            line_x_setting['name'] = x_name  # k线名称
            line_x_setting['bar_interval'] = interval_num  # X K线得周期
            line_x_setting['para_pre_len'] = 60
            line_x_setting['para_ma1_len'] = 55
            line_x_setting['para_ma2_len'] = 89
            # line_x_setting['para_macd_fast_len'] = 12
            # line_x_setting['para_macd_slow_len'] = 26
            # line_x_setting['para_macd_signal_len'] = 9
            line_x_setting['para_active_chanlun'] = True
            line_x_setting['para_active_skd'] = True
            line_x_setting['price_tick'] = self.price_tick  # 合约最小跳动
            line_x_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()  # 短合约

            self.kline_x = kline_class(self, self.on_bar_x, line_x_setting)
            self.kline_x.max_hold_bars = 500  # 提高性能，小周期不需要那么多K线
            self.klines.update({self.kline_x.name: self.kline_x})

            # 创建的Y K线
            line_y_setting = {}
            line_y_setting['name'] = y_name
            kline_class, interval_num = get_cta_bar_type(y_name)
            line_y_setting['bar_interval'] = interval_num
            line_y_setting['para_boll_len'] = 20  # 布林线
            line_y_setting['para_ma1_len'] = 55  # 缠论双均线
            line_y_setting['para_ma2_len'] = 89
            line_y_setting['para_macd_fast_len'] = 12  # macd
            line_y_setting['para_macd_slow_len'] = 26
            line_y_setting['para_macd_signal_len'] = 9
            line_y_setting['para_active_chanlun'] = True  # 激活缠论
            line_y_setting['para_active_skd'] = True  # 激活摆动指标
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
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ema1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ema2', 'type_': 'list'},

                {'name': 'sk', 'source': 'line_bar', 'attr': 'line_sk', 'type_': 'list'},
                {'name': 'sd', 'source': 'line_bar', 'attr': 'line_sd', 'type_': 'list'},

            ]
            if kline.para_boll_len > 0:
                kline.export_fields.extend(
                    [
                        {'name': 'upper', 'source': 'line_bar', 'attr': 'line_boll_upper', 'type_': 'list'},
                        {'name': 'middle', 'source': 'line_bar', 'attr': 'line_boll_middle', 'type_': 'list'},
                        {'name': 'lower', 'source': 'line_bar', 'attr': 'line_boll_lower', 'type_': 'list'}
                    ]
                )
            if kline.para_pre_len > 0:
                kline.export_fields.extend(
                    [
                        {'name': 'pre_high', 'source': 'line_bar', 'attr': 'line_pre_high', 'type_': 'list'},
                        {'name': 'pre_low', 'source': 'line_bar', 'attr': 'line_pre_low', 'type_': 'list'}
                    ]
                )
            if kline.para_macd_fast_len > 0:
                kline.export_fields.extend(
                    [
                        {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                        {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                        {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'}
                    ]
                )

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

        if tick.vt_symbol == self.idx_symbol or self.backtesting:
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

        self.kline_y.on_tick(tick)
        self.kline_x.on_tick(tick)

        # 4、交易逻辑
        self.tns_process_sub()

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime)

        # 检查止损
        self.grid_check_stop()

        # 实盘每分钟执行一次得逻辑
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute
            if not self.backtesting:
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
            self.kline_y.add_bar(copy.copy(bar))
            # 如果次级别为1分钟
            if self.kline_x.bar_interval == 1 and self.kline_x.interval == Interval.MINUTE:
                self.kline_x.add_bar(bar, bar_is_completed=True)
            else:
                self.kline_x.add_bar(bar)

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
        日K线数据
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
        if len(self.kline_y.duan_list) == 0 or len(self.kline_y.bi_zs_list) == 0 or len(self.kline_y.bi_list) == 0:
            return

        if self.cur_datetime.strftime('%Y-%m-%d') in ['2016-02-22', '2016-05-12', '2017-05-10', '2018-05-07']:
            a = 1

        # 如果没有做多信号时，就去发现是否有做多信号
        if SIGNAL_SHAKE_LONG not in self.policy.sub_tns:
            self.tns_discover_shake_long()

        # 如果没有做空信号时，就去发现是否有做空信号
        if SIGNAL_SHAKE_SHORT not in self.policy.sub_tns:
            self.tns_discover_shake_short()

    def tns_discover_shake_long(self):
        """
        发掘震荡做多信号
        # 第二个中枢出现
        :return:
        """
        # 必须具有2个以上笔中枢
        if len(self.kline_y.bi_zs_list) < 2:
            return

        # 连续上涨的两个笔中枢
        two_long_sz = self.kline_y.bi_zs_list[-1].high > self.kline_y.bi_zs_list[-2].high \
                      and self.kline_y.bi_zs_list[-1].low > self.kline_y.bi_zs_list[-2].low

        # 信号1：必须满足下跌线段+下跌分笔,下跌段的动量不能超过0.8；
        if two_long_sz \
                and check_duan_not_rt(self.kline_y, Direction.SHORT) \
                and self.kline_y.bi_list[-3].high > self.kline_y.bi_list[-1].high \
                and self.kline_y.bi_list[-3].low > self.kline_y.bi_list[-1].low \
                and self.kline_y.cur_bi.start < self.kline_y.cur_bi_zs.end:

            # 下跌段，最后三笔形成的动量
            last_three_bi_momentum = (self.kline_y.bi_list[-3].high - self.kline_y.bi_list[-1].low) / sum(
                bi.height for bi in self.kline_y.bi_list[-3:])
            if last_three_bi_momentum < 0.8:

                # 线段的两个下跌分笔，macd 底背驰
                is_macd_diff_div = self.kline_y.is_fx_macd_divergence(
                    direction=Direction.SHORT,
                    cur_duan=self.kline_y.cur_duan
                )

                is_skd_div = False
                # 判断是否skd背驰
                if not is_macd_diff_div:
                    if self.kline_y.cur_skd_divergence == 1 \
                            and self.kline_y.cur_bi.low < self.kline_y.bi_list[-3].low \
                            and self.cur_99_price > float(self.kline_y.cur_duan.second_low):
                        is_skd_div = True

                # 满足背驰条件后，当前下跌笔破下轨，价格已经回抽到下轨之上
                if (is_macd_diff_div or is_skd_div) \
                        and self.kline_y.cur_bi_zs.max_high > self.cur_99_price > self.kline_y.line_boll_lower[-1] > min(self.kline_y.low_array[-3:]== self.kline_y.cur_bi.low):
                    sub_tns = {
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
                        'stop_price': self.kline_y.line_boll_lower[-1] - self.kline_y.line_boll_std[-1],
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': 'two_zs'
                    }
                    self.policy.sub_tns[SIGNAL_SHAKE_LONG] = sub_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"MACD底背驰" if is_macd_diff_div else 'SKD底背驰',
                        "signal": SIGNAL_SHAKE_LONG
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_LONG, d.get('operation')))

        # 信号2：拐点；收敛三角形结构
        if self.kline_y.cur_duan.direction == 1 \
                and self.kline_y.cur_duan.end == self.kline_y.bi_list[-3].start \
                and check_bi_not_rt(self.kline_y,Direction.SHORT) \
                and self.kline_y.bi_list[-3].start > self.kline_y.cur_bi_zs.end \
                and 0.5 * self.kline_y.cur_duan.height > self.kline_y.bi_list[-3].height > self.kline_y.bi_list[
            -2].height > self.kline_y.cur_bi.height \
                and self.kline_y.cur_bi.low < self.kline_y.line_boll_lower[-1] < self.cur_99_price:
            sub_tns = {
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
                'stop_price': self.kline_y.line_boll_lower[-1] - self.kline_y.line_boll_std[-1],
                'duan_start': self.kline_y.cur_duan.start,
                'bi_start': self.kline_y.cur_bi.start,
                'signal_name': 'trangle'
            }
            self.policy.sub_tns[SIGNAL_SHAKE_LONG] = sub_tns
            self.policy.save()
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"下跌收敛三角",
                "signal": SIGNAL_SHAKE_LONG
            }
            self.save_dist(d)
            self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_LONG, d.get('operation')))

        # 信号三：脱离中枢后，反抽，高于中枢中轨，低于布林下轨站稳
        if self.kline_y.cur_duan.direction == 1 \
                and self.kline_y.cur_bi.direction == -1 \
                and self.kline_y.bi_list[-2].start <= self.kline_y.cur_bi_zs.end \
                and self.kline_y.cur_bi.low > self.kline_y.cur_bi_zs.middle \
                and self.kline_y.cur_bi.low < self.kline_y.line_boll_lower[-1] < self.cur_99_price:
            sub_tns = {
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
                'stop_price': self.kline_y.line_boll_lower[-1] - self.kline_y.line_boll_std[-1],
                'duan_start': self.kline_y.cur_duan.start,
                'bi_start': self.kline_y.cur_bi.start,
                'signal_name': 'zs_break'
            }
            self.policy.sub_tns[SIGNAL_SHAKE_LONG] = sub_tns
            self.policy.save()
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"脱离中枢首碰下轨",
                "signal": SIGNAL_SHAKE_LONG
            }
            self.save_dist(d)
            self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_LONG, d.get('operation')))

        # 信号四：类二买点得震荡回归. 类1卖点创新低，反弹回调形成类二卖点，不创新低，布林向上收口
        if self.kline_y.cur_duan.direction == -1 \
                and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start \
                and self.kline_y.bi_list[-3].height > 1.3 * self.kline_y.bi_height_ma() \
                and self.kline_y.cur_bi.low == min (self.kline_y.low_array[-3:]) < self.kline_y.line_boll_lower[-1] < self.cur_99_price:

            # 进一步判断
            # 线段低点到现在得bar数量
            pre_n = self.kline_y.bi_list[-2].bars + self.kline_y.bi_list[-1].bars - 1
            # 反抽笔到现在得bar数量
            mid_n = self.kline_y.bi_list[-1].bars - 1
            pre_boll_lower = self.kline_y.line_boll_lower[-pre_n]
            mid_boll_lower = self.kline_y.line_boll_lower[-mid_n]

            if mid_boll_lower < pre_boll_lower and mid_boll_lower < self.kline_y.line_boll_lower[-1]:
                sub_tns = {
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
                    'stop_price': self.kline_y.line_boll_lower[-1] - self.kline_y.line_boll_std[-1],
                    'duan_start': self.kline_y.cur_duan.start,
                    'bi_start': self.kline_y.cur_bi.start,
                    'signal_name': 'boll_cycle'
                }
                self.policy.sub_tns[SIGNAL_SHAKE_LONG] = sub_tns
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"布林底圆弧",
                    "signal": SIGNAL_SHAKE_LONG
                }
                self.save_dist(d)
                self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_LONG, d.get('operation')))

    def tns_discover_shake_short(self):
        """
        发掘震荡卖出信号
        :return:
        """
        # 必须具有2个以上笔中枢
        if len(self.kline_y.bi_zs_list) < 2:
            return

        # 优先考虑移除观测信号
        short_tns = self.policy.sub_tns.get(SIGNAL_SHAKE_SHORT, None)
        if short_tns:
            if short_tns.get('status', None) == TNS_STATUS_OBSERVATE:
                # 价格向下运动至中枢中轨，还没开仓成功
                if short_tns.get('signal_name') == 'two_zs' \
                        and short_tns.get('zs_middle') and self.cur_99_price < short_tns.get('zs_middle'):
                    self.write_log(f'回抽中枢中点，还没进场，当前空单观测信号移除')
                    self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                    return

                if short_tns.get('stop_price') and self.cur_99_price > short_tns.get('stop_price'):
                    self.write_log(f'价格上破止损价，还没进场，当前空单观测信号移除')
                    self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                    return

                if short_tns.get('win_price') and self.cur_99_price < short_tns.get('win_price'):
                    self.write_log(f'价格下破止盈价价，还没进场，当前空单观测信号移除')
                    self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                    return

                if self.cur_99_price < self.kline_y.line_boll_lower[-1]:
                    self.write_log(f'价格下破止盈价价，还没进场，当前{SIGNAL_SHAKE_LONG}信号退出')
                    self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                    return

                if short_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == 1:
                    self.write_log(f'新一笔出现，还没进场，当前空单观测信号移除')
                    self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                    return

            return

        # 必须是连续下跌的两个笔中枢
        two_short_zs = self.kline_y.bi_zs_list[-1].high < self.kline_y.bi_zs_list[-2].high \
                       and self.kline_y.bi_zs_list[-1].low < self.kline_y.bi_zs_list[-2].low

        # 信号1：必须满足上涨线段 + 上涨分笔，
        if two_short_zs \
                and check_duan_not_rt(self.kline_y, Direction.LONG) \
                and self.kline_y.cur_bi.start > self.kline_y.cur_bi_zs.end \
                and self.kline_y.bi_list[-3].low < self.kline_y.bi_list[-1].low \
                and self.kline_y.bi_list[-3].high < self.kline_y.bi_list[-1].high:
            last_three_bi_momentum = (self.kline_y.bi_list[-1].high - self.kline_y.bi_list[-3].low) / sum(
                [bi.height for bi in self.kline_y.bi_list[-3:]])
            if last_three_bi_momentum < 0.8:

                # 线段的两个上涨分笔，macd 顶背驰
                is_macd_diff_div = self.kline_y.is_fx_macd_divergence(
                    direction=Direction.LONG,
                    cur_duan=self.kline_y.cur_duan
                )

                is_skd_div = False
                # 判断是否skd顶背驰
                if not is_macd_diff_div:
                    if self.kline_y.cur_skd_divergence == -1 \
                            and self.kline_y.cur_bi.high > self.kline_y.bi_list[-3].high \
                            and self.cur_99_price < float(self.kline_y.cur_duan.second_high):
                        is_skd_div = True

                if (is_macd_diff_div or is_skd_div) \
                        and self.kline_y.cur_bi_zs.min_low < self.cur_99_price < self.kline_y.line_boll_upper[-1]< self.kline_y.cur_bi.high == max(self.kline_y.high_array[-3:]):
                    sub_tns = {
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
                        'stop_price': self.kline_y.line_boll_upper[-1] + self.kline_y.line_boll_std[-1],
                        'duan_start': self.kline_y.cur_duan.start,
                        'bi_start': self.kline_y.cur_bi.start,
                        'signal_name': 'two_zs'
                    }
                    self.policy.sub_tns[SIGNAL_SHAKE_SHORT] = sub_tns
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"MACD顶背驰" if is_macd_diff_div else 'SKD顶背驰',
                        "signal": SIGNAL_SHAKE_SHORT
                    }
                    self.save_dist(d)
                    self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_SHORT, d.get('operation')))

        # 信号2：三角型收敛
        if self.kline_y.cur_duan.direction == -1 \
                and self.kline_y.cur_duan.end == self.kline_y.bi_list[-3].start \
                and check_bi_not_rt(self.kline_y, Direction.LONG) \
                and self.kline_y.bi_list[-3].start > self.kline_y.cur_bi_zs.end \
                and 0.5 * self.kline_y.cur_duan.height > self.kline_y.bi_list[-3].height > self.kline_y.bi_list[
            -2].height > self.kline_y.cur_bi.height \
                and self.kline_y.cur_bi.high > self.kline_y.line_boll_upper[-1] > self.cur_99_price:
            sub_tns = {
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
                'stop_price': self.kline_y.line_boll_upper[-1] + self.kline_y.line_boll_std[-1],
                'duan_start': self.kline_y.cur_duan.start,
                'bi_start': self.kline_y.cur_bi.start,
                'signal_name': 'trangle'
            }
            self.policy.sub_tns[SIGNAL_SHAKE_SHORT] = sub_tns
            self.policy.save()
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"上涨三角收敛",
                "signal": SIGNAL_SHAKE_SHORT
            }
            self.save_dist(d)
            self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_SHORT, d.get('operation')))

        # 信号三：脱离中枢后，反抽，低于中枢中轨，高于布林上轨站稳
        if self.kline_y.cur_duan.direction == -1 \
                and self.kline_y.cur_bi.direction == 1 \
                and self.kline_y.bi_list[-2].start <= self.kline_y.cur_bi_zs.end \
                and self.kline_y.cur_bi.high < self.kline_y.cur_bi_zs.middle \
                and self.kline_y.cur_bi.high == max(self.kline_y.high_array[-3:]) > self.kline_y.line_boll_upper[-1] > self.cur_99_price:
            sub_tns = {
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
                'stop_price': self.kline_y.line_boll_upper[-1] + self.kline_y.line_boll_std[-1],
                'duan_start': self.kline_y.cur_duan.start,
                'bi_start': self.kline_y.cur_bi.start,
                'signal_name': 'zs_break'
            }
            self.policy.sub_tns[SIGNAL_SHAKE_SHORT] = sub_tns
            self.policy.save()
            d = {
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": f"脱离中枢首碰上轨",
                "signal": SIGNAL_SHAKE_SHORT
            }
            self.save_dist(d)
            self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_SHORT, d.get('operation')))

        # 信号四：类二卖点得震荡回归，类一卖点，比类二卖点高，且两个点之间的布林上轨，形成一个圆弧收敛形状
        if self.kline_y.cur_duan.direction == 1 \
                and self.kline_y.cur_duan.end == self.kline_y.bi_list[-2].start \
                and self.kline_y.bi_list[-3].height > 1.3 * self.kline_y.bi_height_ma() \
                and self.kline_y.cur_bi.high == max(self.kline_y.high_array[-3:]) > self.kline_y.line_boll_upper[-1] > self.cur_99_price:

            # 进一步判断
            # 线段高点到现在得bar数量
            pre_n = self.kline_y.bi_list[-2].bars + self.kline_y.bi_list[-1].bars - 1
            # 反抽笔到现在得bar数量
            mid_n = self.kline_y.bi_list[-1].bars - 1
            pre_boll_upper = self.kline_y.line_boll_upper[-pre_n]
            mid_boll_upper = self.kline_y.line_boll_upper[-mid_n]

            if mid_boll_upper > pre_boll_upper and mid_boll_upper > self.kline_y.line_boll_upper[-1]:
                sub_tns = {
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
                    'stop_price': self.kline_y.line_boll_upper[-1] + self.kline_y.line_boll_std[-1],
                    'duan_start': self.kline_y.cur_duan.start,
                    'bi_start': self.kline_y.cur_bi.start,
                    'signal_name': 'two_zs'
                }
                self.policy.sub_tns[SIGNAL_SHAKE_SHORT] = sub_tns
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"布林顶圆弧",
                    "signal": SIGNAL_SHAKE_SHORT
                }
                self.save_dist(d)
                self.write_log('发现新信号:{}[{}]'.format(SIGNAL_SHAKE_SHORT, d.get('operation')))

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

    def tns_process_sub(self):
        """处理各种子事务，"""
        if len(self.kline_y.duan_list) == 0 or len(self.kline_y.bi_zs_list) == 0 or len(self.kline_y.bi_list) == 0:
            return

        for signal in list(self.policy.sub_tns.keys()):

            # 震荡做多
            if signal == SIGNAL_SHAKE_LONG:
                self.tns_proces_close_long(signal)
                self.tns_process_open_long(signal)
                continue

            # 震荡做空
            if signal == SIGNAL_SHAKE_SHORT:
                self.tns_process_close_short(signal)
                self.tns_process_open_short(signal)
                continue

    def tns_process_open_long(self, signal):
        """
        处理震荡买入事务
        满足盈亏比，当前次级别分笔为多
        """
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')

        # 买入观测状态，
        if status == TNS_STATUS_OBSERVATE:
            # 动态计算次级别K线摆动指标是否金叉
            self.kline_x.rt_count_skd()
            # 计算盈亏比
            win_price = min(sub_tns.get('zs_high'), self.kline_y.line_boll_upper[-1]) if sub_tns.get('signal_name') == 'two_zs' else self.kline_y.line_boll_upper[-1]
            stop_price = max(sub_tns.get('stop_price'), self.kline_y.line_boll_lower[-1] - self.kline_y.line_boll_std[-1])
            win_lost_rate = (win_price - self.cur_99_price) / (self.cur_99_price - stop_price)

            # 满足 实时金叉，价格在止损线上方，盈亏比也满足  and self.kline_y.line_boll_std[-1] * 4 >  0.5 * float(self.kline_y.bi_height_ma()) \
            if (self.kline_x.cur_skd_count > 0 or self.kline_x.rt_skd_count > 0) \
                    and check_bi_not_rt(self.kline_y, Direction.SHORT)\
                    and self.kline_y.line_boll_middle[-1] > self.cur_99_price > stop_price \
                    and win_lost_rate >= self.win_lost_rate:
                status = TNS_STATUS_READY
                sub_tns.update({"status": status, 'win_price': win_price, 'stop_price': stop_price})
                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()
                self.write_log(f'{signal} {TNS_STATUS_OBSERVATE} => {TNS_STATUS_READY}')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}",
                    "signal": SIGNAL_SHAKE_LONG
                }
                self.save_dist(d)

            # 如果在观测状态下,出现上涨一笔，且上涨笔的高度，超过中枢中线Z，则退出该tns
            if sub_tns.get('signal_name') == 'two_zs' and self.cur_99_price > sub_tns.get('zs_middle'):
                self.write_log(f'当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price < sub_tns.get('stop_price'):
                self.write_log(f'当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price > self.kline_y.line_boll_upper[-1]:
                self.write_log(f'当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(signal)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == -1:
                self.write_log(f'新一笔出现，还没进场，当前多单观测信号移除')
                self.tns_remove_signal(signal)
                return

        # 买入就绪状态
        if status == TNS_STATUS_READY:
            win_price = sub_tns.get('win_price')
            stop_price = sub_tns.get('stop_price')
            # 如果在就绪状态下,出现上涨一笔，且上涨笔的高度，超过中枢中线Z，则退出该tns
            if sub_tns.get('signal_name') == 'two_zs' and self.cur_99_price > sub_tns.get('zs_middle'):
                self.write_log(f'当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price < sub_tns.get('stop_price'):
                self.write_log(f'当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price > self.kline_y.line_boll_upper[-1]:
                self.write_log(f'当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(signal)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == -1:
                self.write_log(f'新一笔出现，还没进场，当前多单观测信号移除')
                self.tns_remove_signal(signal)
                return

            # 开始进入委托下单阶段
            if self.cur_99_price > sub_tns.get('stop_price') \
                    and check_bi_not_rt(self.kline_y, Direction.SHORT) \
                    and not self.is_entry_close_time():

                if self.tns_buy(signal, stop_price, win_price):
                    status = TNS_STATUS_ORDERING
                    sub_tns.update({'status': status,
                                    'open_bi_start': self.kline_y.cur_bi.start,
                                    'last_open_price': self.cur_99_price,
                                    'stop_price': stop_price,
                                    'win_price': win_price})

                    self.policy.sub_tns.update({signal: sub_tns})
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}",
                        "signal": SIGNAL_SHAKE_LONG
                    }
                    self.save_dist(d)

        # 委托状态
        if status == TNS_STATUS_ORDERING:

            # 判断是否已经开仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[SIGNAL_SHAKE_LONG])
            if len(open_grids) > 0:
                self.write_log(f'策略已经持有多单，不再开仓')
                self.write_log(f'{signal} 已持仓.  {status} => {TNS_STATUS_OPENED}')
                status = TNS_STATUS_OPENED
                sub_tns.update({"status": status})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}=> {TNS_STATUS_OPENED}",
                    "signal": SIGNAL_SHAKE_LONG
                }
                self.save_dist(d)
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
                        "signal": SIGNAL_SHAKE_LONG
                    }
                    self.save_dist(d)
                    self.policy.save()
                    return

    def tns_process_open_short(self, signal):
        """
         处理震荡开空事务
        满足盈亏比，当前次级别分笔为空
        :param signal:
        :return:
        """
        sub_tns = self.policy.sub_tns.get(signal, None)
        if sub_tns is None:
            return

        # 当前事务的状态[观测/就绪/准备开仓/已经开仓/关闭]
        status = sub_tns.get('status')

        # 卖出观测状态，
        if status == TNS_STATUS_OBSERVATE:
            # 动态计算次级别周期摆动指标是否死叉
            self.kline_x.rt_count_skd()
            # 计算盈亏比
            win_price = max(sub_tns.get('zs_low'), self.kline_y.line_boll_lower[-1]) if sub_tns.get('signal_name') == 'two_zs' else self.kline_y.line_boll_lower[-1]
            stop_price = min(sub_tns.get('stop_price'), self.kline_y.line_boll_upper[-1] + self.kline_y.line_boll_std[-1])
            win_lost_rate = (self.cur_99_price - win_price) / (stop_price - self.cur_99_price)

            # 实时死叉，价格在止损线下，满足盈亏比  and self.kline_y.line_boll_std[-1] * 4 > 0.5 * float(self.kline_y.bi_height_ma())\
            if (self.kline_x.cur_skd_count < 0 or self.kline_x.rt_skd_count < 0) \
                    and check_bi_not_rt(self.kline_y, Direction.LONG)\
                    and self.kline_y.line_boll_middle[-1] < self.cur_99_price < stop_price \
                    and win_lost_rate >= self.win_lost_rate:
                status = TNS_STATUS_READY
                sub_tns.update({"status": status, 'win_price': win_price, 'stop_price':stop_price})
                self.policy.sub_tns.update({signal: sub_tns})
                self.policy.save()
                self.write_log(f'{signal} {TNS_STATUS_OBSERVATE} => {TNS_STATUS_READY}')
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}",
                    "signal": SIGNAL_SHAKE_SHORT
                }
                self.save_dist(d)

            # 如果在观测状态下,价格超过中枢中线Z，则退出该tns
            if sub_tns.get('signal_name') == 'two_zs' and self.cur_99_price < sub_tns.get('zs_middle'):
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return
            if self.cur_99_price > stop_price:
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price < self.kline_y.line_boll_lower[-1]:
                self.write_log(f'价格下破止盈价价，还没进场，当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == 1:
                self.write_log(f'新一笔出现，还没进场，当前空单观测信号移除')
                self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                return

        # 卖出就绪状态
        if status == TNS_STATUS_READY:
            win_price = sub_tns.get('win_price')
            stop_price = sub_tns.get('stop_price')

            # 如果在就绪状态下,价格超过中枢中线Z，则退出该tns
            if sub_tns.get('signal_name') == 'two_zs' and self.cur_99_price < sub_tns.get('zs_middle'):
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return
            if self.cur_99_price > stop_price:
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price < self.kline_y.line_boll_lower[-1]:
                self.write_log(f'价格下破止盈价价，还没进场，当前{SIGNAL_SHAKE_LONG}信号退出')
                self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                return

            if sub_tns.get('bi_start') != self.kline_y.cur_bi.start and self.kline_y.cur_bi.direction == 1:
                self.write_log(f'新一笔出现，还没进场，当前空单观测信号移除')
                self.tns_remove_signal(SIGNAL_SHAKE_SHORT)
                return

            if self.cur_99_price < stop_price \
                    and check_bi_not_rt(self.kline_y,Direction.LONG) \
                    and not self.is_entry_close_time():
                if self.tns_short(signal, stop_price, win_price):
                    status = TNS_STATUS_ORDERING
                    sub_tns.update({'status': status,
                                    'open_bi_start': self.kline_y.cur_bi.start,
                                    'last_open_price': self.cur_99_price,
                                    'stop_price': stop_price,
                                    'win_price': win_price})
                    self.policy.sub_tns.update({signal: sub_tns})
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": f"{TNS_STATUS_ORDERING}",
                        "signal": SIGNAL_SHAKE_SHORT
                    }
                    self.save_dist(d)

        # 委托状态
        if status == TNS_STATUS_ORDERING:

            # 判断是否已经开仓
            open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[SIGNAL_SHAKE_SHORT])
            if len(open_grids) > 0:
                self.write_log(f'策略已经持有空单，不再开仓')
                self.write_log(f'{signal} 已持仓.  {status} => {TNS_STATUS_OPENED}')
                status = TNS_STATUS_OPENED
                sub_tns.update({"status": status})
                d = {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"{TNS_STATUS_READY}=> {TNS_STATUS_OPENED}",
                    "signal": SIGNAL_SHAKE_SHORT
                }
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
                        "signal": SIGNAL_SHAKE_SHORT
                    }
                    self.save_dist(d)
                    self.policy.save()
                    return

    def is_entry_close_time(self):
        """是否进入收盘时间(更加精确得方式，是每个品种独立设置自己得收盘时间"""
        hh = self.cur_datetime.strftime('%H')
        hhmm = self.cur_datetime.strftime('%H%M')
        for _hhmm in self.force_leave_times:
            if hh == _hhmm[0:2] and hhmm >= _hhmm:
                return True

        return False

    def tns_proces_close_long(self, signal):
        """
        处理多单离场
        - 安全目标=>当前级别布林中轨=> 提升保本
        - 主动止盈价 => 中枢上轨 或布林上轨 或 次级别上涨线段+顶背驰
        - 收盘前离场

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
        # 判断是否已经平仓
        open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[signal])
        if len(open_grids) == 0:
            self.write_log(f'当前{signal}信号退出')
            self.tns_remove_signal(signal)
            return

        elif last_open_price and stop_price:

            # 日内收盘前离场，根据参数配置的离场时间来判断
            if self.is_entry_close_time() and win_price > self.cur_99_price:
                self.tns_update_grid(direction=Direction.LONG,
                                     grid_type=signal,
                                     win_price=self.cur_99_price)
                self.write_log(f'收盘前{signal}主动离场')
                self.tns_remove_signal(signal)
                return

            # 布林上轨发生移动，比原止盈目标低，主动离场
            if self.cur_99_price > self.kline_y.line_boll_upper[-1] \
                    and self.kline_y.line_boll_upper[-1] < win_price:
                sub_tns.update({'win_price': self.cur_99_price})
                self.tns_update_grid(direction=Direction.LONG,
                                     grid_type=signal,
                                     win_price=self.cur_99_price)
                self.write_log(f'布林上轨比原止盈目标低，{signal}主动离场')
                self.tns_remove_signal(signal)
                return

            # # 触碰布林中轨时，提升止损价
            # if self.cur_99_price >= self.kline_y.line_boll_middle[-1]\
            #         and self.cur_99_price > last_open_price + self.kline_y.line_boll_std[-1]\
            #         and stop_price < last_open_price:
            #     self.write_log(f'触碰布林中轨，提高多单止损价保本{stop_price} => {last_open_price + self.price_tick}')
            #     stop_price = last_open_price + self.price_tick
            #     sub_tns.update({'stop_price': stop_price})
            #     self.tns_update_grid(direction=Direction.LONG,
            #                          grid_type=signal,
            #                          stop_price=stop_price)
            #     self.policy.sub_tns.update({signal: sub_tns})
            #     return

            # 小周期多单顶背驰离场

    def tns_process_close_short(self, signal):
        """

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
        # 判断是否已经平仓
        open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[signal])
        if len(open_grids) == 0:
            self.write_log(f'当前{signal}信号退出')
            self.tns_remove_signal(signal)
            return

        elif last_open_price and stop_price:

            if self.is_entry_close_time() and win_price < self.cur_99_price:
                self.tns_update_grid(direction=Direction.SHORT,
                                     grid_type=SIGNAL_SHAKE_SHORT,
                                     win_price=self.cur_99_price)
                self.write_log(f'收盘前{signal}主动离场')
                self.tns_remove_signal(signal)
                return

            # 布林下轨发生移动，比原止盈目标高，主动离场
            if self.cur_99_price < self.kline_y.line_boll_lower[-1] \
                    and self.kline_y.line_boll_lower[-1] > sub_tns.get('win_price'):
                self.tns_update_grid(direction=Direction.SHORT,
                                     grid_type=SIGNAL_SHAKE_SHORT,
                                     win_price=self.cur_99_price)
                self.write_log(f'布林下轨比原止盈目标高，{signal}主动离场')
                self.tns_remove_signal(signal)
                return

            # # 触碰布林中轨时，提升止损价
            # if self.cur_99_price <= self.kline_y.line_boll_middle[-1] \
            #         and self.cur_99_price < last_open_price - self.kline_y.line_boll_std[-1] \
            #         and stop_price > last_open_price:
            #     self.write_log(f'触碰布林中轨，提高空单止损价保本{stop_price} => {last_open_price - self.price_tick}')
            #     stop_price = last_open_price - self.price_tick
            #     self.tns_update_grid(direction=Direction.SHORT,
            #                          grid_type=SIGNAL_SHAKE_SHORT,
            #                          stop_price=stop_price)
            #
            #     sub_tns.update({'stop_price': stop_price})
            #     self.policy.sub_tns.update({signal: sub_tns})
            #     return

            # 小周期空单底背驰离场

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
                else:
                    self.write_error(u'多单止盈委托失败')

            # 碰到止损价格
            if g.stop_price > 0 and g.stop_price > self.cur_99_price \
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
                    self.tns_remove_signal(g.type)
                else:
                    self.write_error(u'多单止损委托失败')

        # 空单网格止损检查
        short_grids = self.gt.get_opened_grids_without_types(direction=Direction.SHORT, types=[LOCK_GRID])
        for g in short_grids:

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
                else:
                    self.write_error(u'委托空单平仓失败')

            if g.stop_price > 0 and g.stop_price < self.cur_99_price \
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
                    self.tns_remove_signal(g.type)
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

        self.write_log(u'当前Policy:{}'.format(self.policy.sub_tns))


class S149_Policy(CtaPolicy):
    """S102策略配套得事务"""

    def __init__(self, strategy):
        super().__init__(strategy)

        self.sub_tns = {} # {信号名:{信号的逻辑数据等}}

    def to_json(self):
        """
        将数据转换成dict
        :return:
        """
        j = super().to_json()

        j['sub_tns'] = self.sub_tns

        return j

    def from_json(self, json_data):
        """
        将dict转化为属性
        :param json_data:
        :return:
        """
        super().from_json(json_data)

        self.sub_tns = json_data.get('sub_tns', {})

    def clean(self):
        """清除"""

        self.sub_tns = {}
