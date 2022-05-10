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
from vnpy.component.cta_renko_bar import CtaRenkoBar, Color
from vnpy.component.cta_utility import *
from vnpy.component.chanlun.pyChanlun import ChanDuan, ChanBi, ChanBiZhongShu, ChanDuanZhongShu, ChanFenXing
from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date, append_data

SIGNAL_RENKO_BUY = 'RENKO_BUY'
SIGNAL_RENKO_SHORT = 'RENKO_SHORT'

#######################################################################
class Strategy161_Renko(CtaProFutureTemplate):
    """
    砖图策略
    开仓：连续两个相同方向的砖块，带有长下影线，突破砖块上沿时进场。
    离场：有效跌破YB线时离场
    """

    author = u'大佳'

    # 时间序列K线（S10:10秒K线; M1:1分钟K线; M5:5分钟K线; H1:小时K线)
    x_name = "M1"  # 一分钟K线
    # 砖图K线(P5：固定5个跳，K3：最新价格的千分之3, R5: 动态ATR，开始时5个跳，后续根据x k线的atr20作为波动，不低于5个跳)
    r_name = "K3"  # 例如 K3，K5， P10， R10

    # 回测时，输出到K线csv文件，全空白时，全输出；有指定时，为白名单
    export_csv = []

    win_lost_rate = 2  # 盈亏比
    single_lost_rate = None  # 单此投入亏损率, 0 ~0.1, 计算亏损时按照止损价进行亏损计算
    force_leave_times = ['1450', '0210', '2250']  # 主动离场小时+分钟

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "single_lost_rate", "win_lost_rate",
        "x_name",'r_name',
        "force_leave_times", "export_csv",
        "backtesting"]

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

        # 记录所有on_tick传入最新tick
        self.tick_dict = {}

        # 基础组件系列
        # 仓位状态
        self.position = CtaPosition(self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头
        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)
        # 执行策略
        self.policy = S161_Policy(self)

        # K线系列
        self.kline_x = None  # 时间序列K线
        self.kline_r = None  # 砖图序列K线
        self.renko_height = 5  # 缺省最小高度
        self.klines = {}
        self.init_past_3_4 = False  # 初始化经过3/4时间

        # 最小下单手数
        self.volume_tick = 1
        # 最新的分钟数，用于on_tick里面每分钟定时检查事宜
        self.last_minute = None

        # 回测时调试，如果日期满足当前日期，进入调试
        self.debug_dates = ["2019-12-11","2020-01-16"]

        if setting:
            self.update_setting(setting)

            volume_tick = self.cta_engine.get_volume_tick(self.vt_symbol)
            if volume_tick != self.volume_tick:
                self.volume_tick = volume_tick
                self.write_log(f'{self.vt_symbol}的最小成交数量是{self.volume_tick}')

            # 创建X 时间序列K线
            line_x_setting = {}
            kline_class, interval_num = get_cta_bar_type(self.x_name)
            line_x_setting['name'] = self.x_name  # k线名称
            line_x_setting['bar_interval'] = interval_num  # X K线得周期
            line_x_setting['para_atr1_len'] = 20  # 副图指标: 平均波动率，仅用于range renko bar模式
            line_x_setting['para_ma1_len'] = 55   # 主图指标: 短均线
            line_x_setting['para_ma2_len'] = 89   # 主图指标: 短均线
            line_x_setting['para_macd_fast_len'] = 12   # 副图指标: macd
            line_x_setting['para_macd_slow_len'] = 26   # 副图指标:
            line_x_setting['para_macd_signal_len'] = 9  # 副图指标:
            line_x_setting['para_active_chanlun'] = True
            line_x_setting['para_active_chan_xt'] = True  # 激活缠论形态
            line_x_setting['para_active_skd'] = True  # 副图指标: 摆动指标
            line_x_setting['price_tick'] = self.price_tick  # 合约最小跳动
            line_x_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()  # 短合约
            self.kline_x = kline_class(self, self.on_bar_x, line_x_setting)
            self.kline_x.max_hold_bars = 1000  # 为了减少缠论的计算，最多只保留1000根Bar
            self.klines.update({self.kline_x.name: self.kline_x})

            # 创建的R 砖图K线
            line_r_setting = {}
            line_r_setting['name'] = self.r_name

            # 使用跳数 或 动态
            if self.r_name.startswith('P') or self.r_name.startswith('R'):
                self.renko_height = int(self.r_name.replace("P", "").replace('R',""))
                if not self.renko_height > 0:
                    self.write_error(f'r_name配置不正确:{self.r_name}')
                    raise Exception(f'r_name配置不正确:{self.r_name}')
                line_r_setting['height'] = self.renko_height * self.price_tick

            # 使用价格比率
            elif self.r_name.startswith('K'):
                kilo_height = int(self.r_name.replace('K', ''))
                if not kilo_height > 0:
                    self.write_error(f'r_name配置不正确:{self.r_name}')
                    raise Exception(f'r_name配置不正确:{self.r_name}')
                line_r_setting['kilo_height'] = kilo_height * self.price_tick

            else:
                self.write_error(f'r_name配置不正确:{self.r_name}')
                raise Exception(f'r_name配置不正确:{self.r_name}')

            line_r_setting['para_boll_len'] = 26      # 主图指标: 布林通道
            line_r_setting['para_ma1_len'] = 5        # 主图指标: 快速均线
            line_r_setting['para_active_yb'] = True   # 主图指标: 激活重心线
            line_r_setting['para_yb_len'] = 20        # 主图指标: 重心线长度
            line_r_setting['para_active_skd'] = True  # 副图指标: 摆动线
            line_r_setting['price_tick'] = self.price_tick
            line_r_setting['underly_symbol'] = get_underlying_symbol(vt_symbol.split('.')[0]).upper()
            self.kline_r = CtaRenkoBar(strategy=self, cb_on_bar=self.on_bar_r, setting=line_r_setting)
            self.klines.update({self.kline_r.name: self.kline_r})

        if self.backtesting:
            # 输出K线
            self.export_klines()
            self.on_init()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

        # X K线输出csv设置
        if len(self.export_csv) == 0 or self.x_name in self.export_csv:
            # 写入文件
            import os
            self.kline_x.export_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}.csv'.format(self.strategy_name, self.kline_x.name)))

            self.kline_x.export_fields = [
                {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
                {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
                {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
                {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
                {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
                {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
                {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
                {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
                {'name': f'ma{self.kline_x.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': f'ma{self.kline_x.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                {'name': 'sk', 'source': 'line_bar', 'attr': 'line_sk', 'type_': 'list'},
                {'name': 'sd', 'source': 'line_bar', 'attr': 'line_sd', 'type_': 'list'},

            ]
            if self.kline_x.para_active_chanlun:
                # 自动输出分笔csv
                self.kline_x.export_bi_filename = os.path.abspath(
                    os.path.join(self.cta_engine.get_logs_path(),
                                 u'{}_{}_bi.csv'.format(self.strategy_name, self.kline_x.name)))

                # 自动输出笔中枢csv
                self.kline_x.export_zs_filename = os.path.abspath(
                    os.path.join(self.cta_engine.get_logs_path(),
                                 u'{}_{}_zs.csv'.format(self.strategy_name, self.kline_x.name)))

                # 自动输出段csv
                self.kline_x.export_duan_filename = os.path.abspath(
                    os.path.join(self.cta_engine.get_logs_path(),
                                 u'{}_{}_duan.csv'.format(self.strategy_name, self.kline_x.name)))

        if len(self.export_csv) == 0 or self.r_name in self.export_csv:
            # 写入文件
            import os
            self.kline_r.export_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}.csv'.format(self.strategy_name, self.kline_r.name)))

            self.kline_r.export_fields = [
                {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
                {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
                {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
                {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
                {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
                {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
                {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
                {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
                {'name': 'boll_upper', 'source': 'line_bar', 'attr': 'line_boll_upper', 'type_': 'list'},
                {'name': 'boll_middle', 'source': 'line_bar', 'attr': 'line_boll_middle', 'type_': 'list'},
                {'name': 'boll_lower', 'source': 'line_bar', 'attr': 'line_boll_lower', 'type_': 'list'},
                {'name': f'ma{self.kline_r.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': 'sk', 'source': 'line_bar', 'attr': 'line_sk', 'type_': 'list'},
                {'name': 'sd', 'source': 'line_bar', 'attr': 'line_sd', 'type_': 'list'},
                {'name': 'yb', 'source': 'line_bar', 'attr': 'line_yb', 'type_': 'list'},
            ]

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
                # 砖图添加分时数据， bar => tick => on_tick
                if self.kline_r.cur_datetime is None or bar.datetime > self.kline_r.cur_datetime:
                    self.kline_r.on_tick(self.bar_to_tick(bar))
                if self.kline_x.cur_datetime is None or bar.datetime > self.kline_x.cur_datetime:
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

        self.kline_r.on_tick(copy.copy(tick))
        self.kline_x.on_tick(copy.copy(tick))

        # 4、交易逻辑
        # 处理信号子事务，进一步发掘开仓
        self.tns_process_sub()

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime)

        # 检查止损
        self.grid_check_stop()

        # 实盘每分钟执行一次得逻辑
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute
            self.write_log(f'[心跳] {self.cur_datetime} process_sub_tns & check grids')
            self.display_tns()

            # 更换合约检查
            if tick.datetime.minute >= 5:
                if self.position.long_pos > 0 and len(self.tick_dict) > 2:
                    # 有多单，且订阅的tick为两个以上
                    self.write_log(f'检查多单移仓换月')
                    self.tns_switch_long_pos(open_new=False)
                if self.position.short_pos < 0 and len(self.tick_dict) > 2:
                    self.write_log(f'检查空单移仓换月')
                    # 有空单，且订阅的tick为两个以上
                    self.tns_switch_short_pos(open_new=False)

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
            self.kline_r.on_tick(self.bar_to_tick(bar))
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

    def bar_to_tick(self, bar):
        """ 回测时，通过bar计算tick数据 """

        tick = TickData(
            gateway_name='backtesting',
            symbol=bar.symbol,
            exchange=bar.exchange,
            datetime=bar.datetime
        )
        tick.date = bar.date
        tick.time = bar.time + '.000'
        tick.trading_day = bar.trading_day
        tick.volume = bar.volume
        tick.open_interest = bar.open_interest
        tick.last_price = bar.close_price
        tick.last_volume = bar.volume
        tick.limit_up = 0
        tick.limit_down = 0
        tick.open_price = 0
        tick.high_price = 0
        tick.low_price = 0
        tick.pre_close = 0
        tick.bid_price_1 = bar.close_price
        tick.ask_price_1 = bar.close_price
        tick.bid_volume_1 = bar.volume
        tick.ask_volume_1 = bar.volume
        return tick

    def on_bar_x(self, bar):
        """
        时间序列K线on_bar事件
        :return:
        """
        if not self.backtesting:
            if self.inited:
                self.write_log(self.kline_x.get_last_bar_str())

        # 如果是range renko,动态检查ATR与高度
        if self.kline_x.cur_atr1 and self.kline_x.cur_atr1 >= self.renko_height:
            if abs(int(self.kline_x.cur_atr1) - self.kline_r.height) > self.price_tick:
                new_renko_height = max(self.renko_height, int(self.kline_x.cur_atr1))
                self.write_log(f'更新{self.kline_r.name} 砖块高度:{self.kline_r.height}=>{new_renko_height}')
                self.kline_r.height = new_renko_height

    def on_bar_r(self, *args, **kwargs):
        """
        砖图K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        bar = None
        if len(args) > 0:
            bar = args[0]
        elif 'bar' in kwargs:
            bar = kwargs.get('bar')
        if bar is None:
            return

        if not self.backtesting:
            if self.inited:
                self.write_log(self.kline_r.get_last_bar_str())

        self.tns_discover_signals()

    def is_entry_close_time(self):
        """是否进入收盘时间(更加精确得方式，是每个品种独立设置自己得收盘时间"""
        hh = self.cur_datetime.strftime('%H')
        hhmm = self.cur_datetime.strftime('%H%M')
        for _hhmm in self.force_leave_times:
            if hh == _hhmm[0:2] and hhmm >= _hhmm:
                return True

        return False

    def tns_discover_signals(self):
        """事务发现信号"""

        # 临时调试
        if self.cur_datetime.strftime('%Y-%m-%d') in self.debug_dates:
            a = 1

        # 如果没有砖图做多信号，就尝试去挖掘
        if SIGNAL_RENKO_BUY not in self.policy.sub_tns:
            self.tns_discover_renko_buy_signal()

        # 如果没有砖图做空信号，尝试挖掘
        if SIGNAL_RENKO_SHORT not in self.policy.sub_tns:
            self.tns_discover_renko_short_signal()

    def tns_discover_renko_buy_signal(self):
        """
        发现砖图买入信号
        :return:
        """
        if self.kline_r.bar_len < 20:
            return

        tre_bar, pre_bar = self.kline_r.line_bar[-2:]

        # 连根连续的红砖，均有下影线
        if tre_bar.color == Color.RED\
            and pre_bar.color == Color.RED\
            and tre_bar.low_price <= tre_bar.open_price - self.kline_r.height\
            and pre_bar.low_price <= pre_bar.open_price - self.kline_r.height:

            long_tns = {
                "status": TNS_STATUS_OBSERVATE,
                "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'stop_price': tre_bar.low_price
            }
            self.policy.sub_tns[SIGNAL_RENKO_BUY] = long_tns
            self.policy.save()

            self.save_dist({
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": SIGNAL_RENKO_BUY,
                "signal": SIGNAL_RENKO_BUY
            })
            self.write_log(f'发现新信号:{SIGNAL_RENKO_BUY}')
            return

    def tns_discover_renko_short_signal(self):
        """
        发掘砖图做空信号
        :return:
        """
        if self.kline_r.bar_len < 20:
            return

        tre_bar, pre_bar = self.kline_r.line_bar[-2:]

        # 连根连续的蓝砖，均有上影线
        if tre_bar.color == Color.BLUE\
            and pre_bar.color == Color.BLUE\
            and tre_bar.high_price >= tre_bar.open_price + self.kline_r.height\
            and pre_bar.high_price >= pre_bar.open_price + self.kline_r.height:

            short_tns = {
                "status": TNS_STATUS_OBSERVATE,
                "create_time": self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'stop_price': tre_bar.high_price,
            }
            self.policy.sub_tns[SIGNAL_RENKO_SHORT] = short_tns
            self.policy.save()

            self.save_dist({
                "datetime": self.cur_datetime,
                "price": self.cur_99_price,
                "operation": SIGNAL_RENKO_SHORT,
                "signal": SIGNAL_RENKO_SHORT
            })
            self.write_log(f'发现新信号:{SIGNAL_RENKO_SHORT}')
            return

    def tns_process_sub(self):
        """处理各种子事务，"""
        if not self.kline_x.pre_duan:
            return

        for signal in list(self.policy.sub_tns.keys()):

            # 三买做多信号
            if signal == SIGNAL_RENKO_BUY:
                self.tns_proces_close_long(signal)
                self.tns_process_open_long(signal)
                continue

            # 三卖做空信号
            if signal == SIGNAL_RENKO_SHORT:
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
        处理砖图开多子事务
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

            # 动态计算次级别K线摆动指标是否金叉
            self.kline_x.rt_count_skd()
            if 0 < self.kline_x.cur_skd_count < 3:
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
            stop_price = sub_tns.get('stop_price')

            if self.cur_99_price < sub_tns.get('stop_price'):
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            # 开始进入委托下单阶段
            if self.cur_99_price > stop_price \
                    and not self.is_entry_close_time():

                if self.tns_buy(signal=signal, stop_price=stop_price, win_price=sys.maxsize):
                    status = TNS_STATUS_ORDERING
                    sub_tns.update({'status': status,
                                    'touch_yb': self.kline_r.line_bar[-1].close_price > self.kline_r.line_yb[-1],
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
        - 止损 =》 触碰grid的止损价，止损离场
        - 未触碰yb => 触碰yb，更新touch_yb
        - touch_yb == True => 连根连续蓝砖离场

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
        touch_yb = sub_tns.get('touch_yb',False)

        # 判断是否已经平仓
        open_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[signal])
        if len(open_grids) == 0:
            self.write_log(f'当前{signal}信号退出')
            self.tns_remove_signal(signal)
            return

        elif last_open_price and stop_price:

            # 日内收盘前离场，根据参数配置的离场时间来判断，一般kline_r的周期在3分钟内的
            if self.is_entry_close_time():
                self.tns_update_grid(direction=Direction.LONG,
                                     grid_type=signal,
                                     win_price=self.cur_99_price - self.price_tick * 2)
                self.write_log(f'收盘前{signal}主动离场')
                self.tns_remove_signal(signal)
                self.save_dist({
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"收盘前离场",
                    "signal": signal
                })
                return

            tre_bar, pre_bar = self.kline_r.line_bar[-2:]
            # touch_yb : False => True
            if not touch_yb and pre_bar.color == Color.RED \
                and pre_bar.close_price > self.kline_r.line_yb[-1]:
                sub_tns.update({"touch_yb": True})
                self.policy.sub_tns.update({signal: sub_tns})

            if touch_yb and tre_bar.color == Color.BLUE \
                    and pre_bar.color == Color.BLUE\
                    and self.cur_99_price < self.kline_r.line_yb[-1]\
                    and stop_price < pre_bar.close_price - self.kline_r.height:
                self.tns_update_grid(direction=Direction.LONG,
                                     grid_type=SIGNAL_RENKO_BUY,
                                     win_price=pre_bar.close_price,
                                     stop_price=pre_bar.close_price - self.kline_r.height)
                self.write_log(f'{signal}更新保护止盈止损')
                sub_tns.update({"stop_price":pre_bar.close_price - self.kline_r.height})
                self.policy.sub_tns.update({signal: sub_tns})
                self.save_dist({
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"更新止盈止损",
                    "signal": signal
                })
                return

    def tns_process_open_short(self, signal):
        """处理做空子事务"""
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

            # 动态计算次级别周期摆动指标是否死叉
            self.kline_x.rt_count_skd()

            if -3 < self.kline_x.cur_skd_count < 0:

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
            stop_price = sub_tns.get('stop_price')

            if self.cur_99_price > stop_price:
                self.write_log(f'当前{signal}信号退出')
                self.tns_remove_signal(signal)
                return

            if self.cur_99_price < stop_price \
                    and not self.is_entry_close_time():
                if self.tns_short(signal=signal,
                                  stop_price=stop_price,
                                  win_price=-sys.maxsize):
                    status = TNS_STATUS_ORDERING
                    sub_tns.update({'status': status,
                                    'touch_yb': self.kline_r.line_bar[-1].close_price < self.kline_r.line_yb[-1],
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
        touch_yb = sub_tns.get('touch_yb', False)
        # 判断是否已经平仓
        open_grids = self.gt.get_opened_grids_within_types(direction=Direction.SHORT, types=[signal])
        if len(open_grids) == 0:
            self.write_log(f'当前{signal}信号退出')
            self.tns_remove_signal(signal)
            return

        elif last_open_price and stop_price:

            # 日内离场逻辑
            if self.is_entry_close_time():
                self.tns_update_grid(direction=Direction.SHORT,
                                     grid_type=signal,
                                     win_price=self.cur_99_price + 2 * self.price_tick)
                self.write_log(f'收盘前{signal}主动离场')

                self.save_dist( {
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"收盘前离场",
                    "signal": signal
                })
                return

            tre_bar, pre_bar = self.kline_r.line_bar[-2:]
            # touch_yb : False => True
            if not touch_yb and pre_bar.color == Color.BLUE \
                    and pre_bar.close_price < self.kline_r.line_yb[-1]:
                sub_tns.update({"touch_yb": True})
                self.policy.sub_tns.update({signal: sub_tns})

            if touch_yb and tre_bar.color == Color.RED \
                    and pre_bar.color == Color.RED \
                    and self.cur_99_price > self.kline_r.line_yb[-1] \
                    and stop_price > pre_bar.close_price + self.kline_r.height:
                self.tns_update_grid(direction=Direction.SHORT,
                                     grid_type=SIGNAL_RENKO_SHORT,
                                     win_price=pre_bar.close_price,
                                     stop_price=pre_bar.close_price + self.kline_r.height)

                self.write_log(f'{signal}更新保护止盈止损')
                sub_tns.update({"stop_price": pre_bar.close_price + self.kline_r.height})
                self.policy.sub_tns.update({signal: sub_tns})
                self.save_dist({
                    "datetime": self.cur_datetime,
                    "price": self.cur_99_price,
                    "operation": f"更新止盈止损",
                    "signal": signal
                })
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

        self.write_log(u'当前Policy:{}'.format(self.policy.sub_tns))


class S161_Policy(CtaPolicy):
    """S161策略配套得事务"""

    def __init__(self, strategy):
        super().__init__(strategy)

        self.sub_tns = {}

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
