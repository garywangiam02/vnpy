# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
from datetime import datetime, timedelta, time, date
import copy
import traceback
import numpy as np
from collections import OrderedDict
import csv

# 然后是自己编写的模块
from vnpy.trader.utility import round_to
from vnpy.app.cta_strategy_pro.template import (CtaProFutureTemplate, Direction, get_underlying_symbol,Exchange, Interval, \
                                                TickData, BarData, OrderType, Offset, Status, TradeData, OrderData)
from vnpy.component.cta_policy import (
    CtaPolicy, TNS_STATUS_OBSERVATE, TNS_STATUS_READY, TNS_STATUS_ORDERING, TNS_STATUS_OPENED, TNS_STATUS_CLOSED
)
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid, LOCK_GRID
from vnpy.component.cta_line_bar import CtaMinuteBar, CtaHourBar, CtaDayBar, CtaLineBar, get_cta_bar_type
from vnpy.component.cta_utility import *

from vnpy.trader.util_wechat import send_wx_msg

from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date


SIGNAL_BREAK_BUY = 'break_buy'         # 突破做多
SIGNAL_BREAK_SHORT = 'break_short'     # 突破做空


#######################################################################
class Strategy154_ChannelBreak_v5(CtaProFutureTemplate):
    """
    唐其安通道突破策略
    # 突破上轨，开始做多，20均线有效下破止损
    # 突破下轨，开始做空，20均线有效下破，止损
    V3:
      突破上轨添加约束条件：
      出现上升线段时，才做多
    V4:
      调整止损为固定止损。
      做多为例：开仓时，固定止损在下轨；触碰均线时，记录触碰（假离场）；如果再次触碰上轨.(假开仓），重新更新止损位置。
      出现反向做空时、多单离场
    v5:
      出现背驰分笔后，触碰均线时，除了记录触碰（假离场）外，还调整止损价位置

    """

    author = u'---'

    para_windows = 20
    para_ma_len = 20
    kline_name = 'M5'      # K 线名称， 例如M5，就是五分钟K线， M30，就是30分钟K线； S10， 10秒K线；H2，2小时K线
    base_kline_name = 'M1' # 基础K线，如M1
    single_lost_rate = None  # 亏损比率，一般0.02，即按照亏损1%与止损价进行计算仓位

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate","single_lost_rate",
        "para_windows", "para_ma_len",
        "kline_name","base_kline_name", "backtesting"]

    def __init__(self, cta_engine,
                 strategy_name,
                 vt_symbol,
                 setting=None):
        """Constructor"""
        super().__init__(cta_engine=cta_engine,
                         strategy_name=strategy_name,
                         vt_symbol=vt_symbol,
                         setting=setting)
        # rb2010.SHFE => rb2010, SHFE
        self.symbol, self.exchange = extract_vt_symbol(vt_symbol)

        # 主力、指数、换月合约
        self.tick_dict = {}  # 记录所有onTick传入最新tick

        # 仓位状态
        self.position = CtaPosition(self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        #
        self.policy = S154_Policy(self)  # 执行策略
        self.kline_x = None   # K 线对象，通过kline_name 识别并生成
        self.kline_y = None   # K 线对象，一分钟K线。
        self.klines = {}    # 所有K线对象（

        # 最小交易数量
        self.volume_tick = 1
        self.last_minute = None

        if setting:
            # 从配置传入参数中，更新
            self.update_setting(setting)
            if self.kline_name == self.base_kline_name:
                raise Exception(f'主/基础K线不能相同,主:{self.kline_name},基础{self.base_kline_name}')
                return

            volume_tick = self.cta_engine.get_volume_tick(self.vt_symbol)
            if volume_tick != self.volume_tick:
                self.volume_tick = volume_tick
                self.write_log(f'{self.vt_symbol}的最小成交数量是{self.volume_tick}')

            # 创建X分钟K线
            kline_setting = {}
            kline_class, kline_bar_interval = get_cta_bar_type(self.kline_name)
            kline_setting['name'] = self.kline_name  # k线名称
            kline_setting['bar_interval'] = kline_bar_interval  # X分钟K线, X秒K线，X小时K线
            kline_setting['para_pre_len'] = self.para_windows
            kline_setting['para_atr1_len'] = self.para_windows  # ATR
            kline_setting['para_ma1_len'] = self.para_ma_len
            kline_setting['para_macd_fast_len'] = 12
            kline_setting['para_macd_slow_len'] = 26
            kline_setting['para_macd_signal_len'] = 9
            kline_setting['price_tick'] = self.price_tick  # 合约最小跳动
            kline_setting['para_active_chanlun'] = True  #
            kline_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()  # 短合约
            self.kline_x = kline_class(self, self.on_bar_x, kline_setting)
            self.klines.update({self.kline_x.name: self.kline_x})

            # 创建Y分钟K线
            kline_setting = {}
            kline_class, kline_bar_interval = get_cta_bar_type(self.base_kline_name)
            kline_setting['name'] = self.base_kline_name  # k线名称
            kline_setting['bar_interval'] = kline_bar_interval  # X分钟K线, X秒K线，X小时K线
            # kline_setting['para_pre_len'] = self.para_windows
            # kline_setting['para_atr1_len'] = self.para_windows  # ATR
            # kline_setting['para_ma1_len'] = self.para_ma_len
            # kline_setting['para_macd_fast_len'] = 12
            # kline_setting['para_macd_slow_len'] = 26
            # kline_setting['para_macd_signal_len'] = 9
            kline_setting['price_tick'] = self.price_tick  # 合约最小跳动
            # kline_setting['para_active_chanlun'] = True  #
            kline_setting['underly_symbol'] = get_underlying_symbol(self.symbol).upper()  # 短合约
            self.kline_y = kline_class(self, self.on_bar_y, kline_setting)
            self.klines.update({self.kline_y.name: self.kline_y})

        # 回测时，自动启动初始化
        if self.backtesting:
            self.on_init()

        if self.backtesting:
            self.export_klines()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return
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
            {'name': 'pre_high', 'source': 'line_bar', 'attr': 'line_pre_high', 'type_': 'list'},
            {'name': 'pre_low', 'source': 'line_bar', 'attr': 'line_pre_low', 'type_': 'list'},
            {'name': f'ma{self.kline_x.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1',
             'type_': 'list'},
           # {'name': f'upper', 'source': 'line_bar', 'attr': 'line_boll_upper',
           #  'type_': 'list'},
           # {'name': f'middle', 'source': 'line_bar', 'attr': 'line_boll_middle',
           #  'type_': 'list'},
           # {'name': f'lower', 'source': 'line_bar', 'attr': 'line_boll_lower',
           #  'type_': 'list'},
            {'name': 'atr', 'source': 'line_bar', 'attr': 'line_atr1', 'type_': 'list'},
        ]
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
        send_wx_msg(content="{}Strategy154_ChannelBreak_v5 启动成功".format(self.symbol))

    def init_data(self):
        """初始化数据"""

        return self.init_data_from_tdx()

    def init_data_from_tdx(self):
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
                start_dt = datetime.now() - timedelta(days=30)

            # 通达信返回得bar，datetime属性是bar的结束时间，所以不能使用callback函数自动推送Bar
            # 这里可以直接取5分钟，也可以取一分钟数据
            result, min1_bars = tdx.get_bars(symbol=self.vt_symbol, period='1min', callback=None, bar_freq=1,
                                             start_dt=start_dt)

            if not result:
                self.write_error(u'未能取回数据')
                return False

            for bar in min1_bars:
                if last_bar_dt and bar.datetime < last_bar_dt:
                    continue
                self.cur_datetime = bar.datetime
                bar.datetime = bar.datetime - timedelta(minutes=1)
                bar.time = bar.datetime.strftime('%H:%M:%S')
                self.cur_99_price = bar.close_price
                self.kline_x.add_bar(bar, bar_freq=1)

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
            self.cur_99_price = tick.last_price
        else:
            return

        # 更新策略执行的时间（用于回测时记录发生的时间）
        self.cur_datetime = tick.datetime

        self.kline_x.on_tick(copy.copy(tick))
        self.kline_y.on_tick(copy.copy(tick))

        # 4、交易逻辑

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime)

        # 检查止损
        self.grid_check_stop()

        # 实盘每分钟执行一次得逻辑
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute
            self.write_log(f'[心跳] {self.cur_datetime} process_sub_tns & check grids')

            # 更换合约检查
            if tick.datetime.minute >= 5:
                if self.position.long_pos > 0 and len(self.tick_dict) > 1:
                    # 有多单，且订阅的tick为两个以上
                    self.tns_switch_long_pos()
                if self.position.short_pos < 0 and len(self.tick_dict) > 1:
                    # 有空单，且订阅的tick为两个以上
                    self.tns_switch_short_pos()

        #self.tns_calculate_signals()

        self.tns_process_open()

    # ----------------------------------------------------------------------
    def on_bar(self, bar):
        """
        1分钟K线数据,仅用于：
            - 回测时，从策略外部调用， 由一分钟生成x分钟，或者x小时等K线)
        :param bar:
        :return:
        """
        if self.backtesting:
            new_dt = bar.datetime + timedelta(seconds=60)
            if self.cur_datetime and new_dt < self.cur_datetime:
                return
            self.cur_datetime = new_dt

            self.cur_open_price = bar.open_price
            self.cur_mi_price = bar.close_price
            self.cur_99_price = bar.close_price

            if self.inited:
                self.tns_cancel_logic(dt=self.cur_datetime)

                # 检查止损
                self.grid_check_stop()

        if bar.datetime.strftime("%Y%m%d%H") in ['2018040209']:
            test = 1

        # 推送bar到大周期K线
        try:
            if self.kline_x.bar_interval == 1 and self.kline_x.interval== Interval.MINUTE:
                self.kline_x.add_bar(bar, bar_is_completed=True)
            else:
                self.kline_x.add_bar(bar)

            if self.kline_y.bar_interval == 1 and self.kline_y.interval== Interval.MINUTE:
                self.kline_y.add_bar(bar, bar_is_completed=True)
            else:
                self.kline_y.add_bar(bar)

        except Exception as ex:
            self.write_error(u'{},{}'.format(str(ex), traceback.format_exc()))

        self.tns_process_open()

    def on_bar_x(self, bar):
        """
        x分钟K线OnBar事件
        :return:
        """
        # 分析缠论
        self.tns_analysis_x_chan()

        if not self.inited:
            return

        if not self.backtesting:
            self.write_log(self.kline_x.get_last_bar_str())

    def on_bar_y(self, bar):

        pass

    def tns_analysis_x_chan(self):
        """分析本级别缠论信号"""

        # 存在分型，且分型是非实时，才进行分析
        if not self.kline_x.cur_fenxing or self.kline_x.cur_fenxing.is_rt:
            return

        # 获取本级别的往次分析结果
        x_signal = self.policy.chan_signals.get(self.kline_x.name, {})

        # 当前是顶分型
        if self.kline_x.cur_fenxing.direction == 1:

            # 本级别上涨趋势顶背驰
            if check_duan_not_rt(self.kline_x, Direction.LONG) \
                    and check_bi_not_rt(self.kline_x, Direction.LONG) \
                    and self.kline_x.cur_bi.end != x_signal.get('bi_end'):
                x_signal_type = None
                # 根据本级别的线段形态（5~13段），判断是否有顶背驰信号
                duan_beichi_signals = [check_chan_xt(kline=self.kline_x,
                                                     bi_list=self.kline_x.duan_list[-n:]) for n
                                       in
                                       range(5, 15, 2)]
                # 识别1.1.1 顶背驰
                if ChanSignals.SA0.value in duan_beichi_signals:
                    x_signal_type = ChanSignals.SA0.value

                # 识别1.1.2 双重顶背驰
                if ChanSignals.SB0.value in duan_beichi_signals:
                    x_signal_type = ChanSignals.SB0.value

                # 根据本级别的分笔形态（5~13笔），判断是否有顶背驰信号
                bi_beichi_signals = [check_chan_xt(kline=self.kline_x,
                                                   bi_list=self.kline_x.bi_list[-n:]) for n
                                     in
                                     range(5, 15, 2)]
                # 识别1.2.1 顶背驰
                if ChanSignals.SA0.value in bi_beichi_signals:
                    x_signal_type = ChanSignals.SA0.value

                # 识别1.2.2 双重顶背驰
                if ChanSignals.SB0.value in bi_beichi_signals:
                    x_signal_type = ChanSignals.SB0.value

                # 识别1.3 判断是否有上涨趋势顶背驰一卖信号
                if check_qsbc_1st(big_kline=self.kline_x,
                                  small_kline=None,
                                  signal_direction=Direction.SHORT):
                    x_signal_type = f"上涨趋势顶背驰一卖"

                # 识别1.4 判断本级别的线段后两个分笔顶分型，是否有MACD DIF值顶背离
                if self.kline_x.is_fx_macd_divergence(
                        direction=Direction.LONG,
                        cur_duan=self.kline_x.cur_duan):
                    x_signal_type = f"上涨线段DIF顶背离"

                if x_signal_type:
                    # 更新本级别信号底背驰的结束时间
                    x_signal.update({'bi_end': self.kline_x.cur_bi.end,
                                     'ding_bi_end': self.kline_x.cur_bi.end,
                                     'ding_price': float(self.kline_x.cur_bi.high),
                                     'signal_type': x_signal_type})
                    self.policy.chan_signals.update({self.kline_x.name: x_signal})
                    d = {
                        "datetime": datetime.strptime(self.kline_x.cur_bi.end, "%Y-%m-%d %H:%M:%S"),
                        "price": float(self.kline_x.cur_bi.high),
                        "operation": f'{self.kline_x.name}.{x_signal_type}',
                        "signal": self.kline_x.name
                    }
                    self.save_dist(d)

            # 识别是否有二卖信号
            if self.kline_x.cur_duan \
                    and self.kline_x.cur_duan.direction == 1 \
                    and check_bi_not_rt(self.kline_x, Direction.LONG) \
                    and self.kline_x.cur_duan.end != self.kline_x.cur_bi.end \
                    and self.kline_x.cur_bi.end != x_signal.get('2nd_short_bi_end'):

                if check_qsbc_2nd(big_kline=self.kline_x,
                                  small_kline=None,
                                  signal_direction=Direction.SHORT):
                    self.write_log(f'{self.kline_x.name}出现下跌趋势二卖信号')
                    x_signal.update({'2nd_short_bi_end': self.kline_x.cur_bi.end,'2nd_short_bi_price': float(self.kline_x.cur_bi.high)})
                    self.policy.chan_signals.update({self.kline_x.name: x_signal})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": float(self.kline_x.cur_bi.high),
                        "operation": f'本级别二卖',
                        "signal": self.kline_x.name
                    }
                    self.save_dist(d)

        # 当前是底分型
        else:
            # 识别1：本级别趋势离场（告警）
            if check_duan_not_rt(self.kline_x, Direction.SHORT) \
                    and check_bi_not_rt(self.kline_x, Direction.SHORT) \
                    and self.kline_x.cur_bi.end != x_signal.get('bi_end'):
                x_signal_type = None
                # 根据线段的段形态，判断是否有底背驰信号
                duan_beichi_signals = [check_chan_xt(kline=self.kline_x, bi_list=self.kline_x.duan_list[-n:]) for n
                                       in
                                       range(5, 15, 2)]
                # 识别1.1.1：线段形态顶背驰
                if ChanSignals.LA0.value in duan_beichi_signals:
                    x_signal_type = ChanSignals.LA0.value
                # 识别1.1.2：线段形态双重顶背驰
                if ChanSignals.LB0.value in duan_beichi_signals:
                    x_signal_type = ChanSignals.LB0.value

                # 根据线段的分笔形态，判断是否有底背驰信号
                bi_beichi_signals = [check_chan_xt(kline=self.kline_x, bi_list=self.kline_x.bi_list[-n:]) for n
                                     in
                                     range(5, 15, 2)]
                # 识别1.2.1：分笔形态顶背驰
                if ChanSignals.LA0.value in bi_beichi_signals:
                    x_signal_type = ChanSignals.LA0.value
                # 识别1.2.2：分笔形态双重顶背驰
                if ChanSignals.LB0.value in duan_beichi_signals:
                    x_signal_type = ChanSignals.LB0.value

                # 识别1.3：判断是否有下跌趋势底背驰一买信号
                if check_qsbc_1st(big_kline=self.kline_x, small_kline=None, signal_direction=Direction.LONG):
                    x_signal_type = f"下跌趋势底背驰一买"

                # 识别1.4：线段最后两分笔顶分型MACD DIF底背离
                if self.kline_x.is_fx_macd_divergence(
                        direction=Direction.SHORT,
                        cur_duan=self.kline_x.cur_duan):
                    x_signal_type = f"下跌线段DIF底背离"

                # 识别出所有本级别的告警信号
                if x_signal_type:
                    # 更新本级别底背驰的结束时间
                    x_signal.update({'bi_end': self.kline_x.cur_bi.end,
                                     'di_bi_end': self.kline_x.cur_bi.end,
                                     'di_price': float(self.kline_x.cur_bi.low),
                                     'signal_type': x_signal_type})
                    self.policy.chan_signals.update({self.kline_x.name: x_signal})
                    d = {
                        "datetime": datetime.strptime(self.kline_x.cur_bi.end, "%Y-%m-%d %H:%M:%S"),
                        "price": float(self.kline_x.cur_bi.low),
                        "operation": f'{self.kline_x.name}.{x_signal_type}',
                        "signal": self.kline_x.name
                    }
                    self.save_dist(d)

            # 识别是否有二买信号
            if self.kline_x.cur_duan \
                    and self.kline_x.cur_duan.direction == -1 \
                    and check_bi_not_rt(self.kline_x, Direction.SHORT) \
                    and self.kline_x.cur_duan.end != self.kline_x.cur_bi.end \
                    and self.kline_x.cur_bi.end != x_signal.get('2nd_buy_bi_end'):
                if check_qsbc_2nd(big_kline=self.kline_x,
                                  small_kline=None,
                                  signal_direction=Direction.LONG):
                    self.write_log(f'{self.kline_x.name}出现下跌趋势二买信号')

                    x_signal.update({'2nd_buy_bi_end': self.kline_x.cur_bi.end,'2nd_buy_bi_price':float(self.kline_x.cur_bi.low)})
                    self.policy.chan_signals.update({self.kline_x.name: x_signal})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": float(self.kline_x.cur_bi.low),
                        "operation": f'本级别二买',
                        "signal": self.kline_x.name
                    }
                    self.save_dist(d)


    def tns_process_open(self):
        """处理各类信号的开仓"""

        if not self.trading:
            return

        if len(self.kline_x.line_pre_high) == 0 or len(self.kline_x.line_pre_low) == 0:
            return

        x_signal = self.policy.chan_signals.get(self.kline_x.name, {})

        # 处理开多事件
        if self.position.long_pos == 0:
            # 当前线段是背驰一买信号开始
            di_bi_end = x_signal.get('di_bi_end', None)
            di_price = x_signal.get('di_price', None)
            di_type = x_signal.get('signal_type',None)
            is_1st = di_bi_end and di_price and di_type == '下跌趋势底背驰一买'\
                    and di_bi_end == self.kline_x.cur_duan.start\
                    and di_price == float(self.kline_x.cur_duan.low)\
                    and self.kline_x.cur_bi.direction == 1

            # 当前分笔是二买点开始
            second_buy_bi_end = x_signal.get('2nd_buy_bi_end', None)
            second_buy_bi_price = x_signal.get('2nd_buy_bi_price', None)
            is_2nd = second_buy_bi_end and second_buy_bi_price \
                    and second_buy_bi_end == self.kline_x.cur_bi.start\
                    and second_buy_bi_price == float(self.kline_x.cur_bi.low) \
                    and self.kline_x.cur_bi.direction == 1

            if self.kline_x.line_pre_high[-1] and self.cur_99_price >= self.kline_x.line_pre_high[-1] \
                    and (self.kline_x.is_duan(direction=Direction.LONG) or is_2nd or is_1st):
                self.tns_buy(SIGNAL_BREAK_BUY)

        if self.position.short_pos == 0:
            # 当前线段是背驰一卖信号开始
            ding_bi_end = x_signal.get('ding_bi_end', None)
            ding_price = x_signal.get('ding_price', None)
            ding_type = x_signal.get('signal_type', None)
            is_1st = ding_bi_end and ding_price and ding_type == '上涨趋势顶背驰一卖' \
                     and ding_bi_end == self.kline_x.cur_duan.start \
                     and ding_price == float(self.kline_x.cur_duan.high) \
                     and self.kline_x.cur_bi.direction == -1

            # 当前分笔是二卖点开始
            second_short_bi_end = x_signal.get('2nd_short_bi_end', None)
            second_short_bi_price = x_signal.get('2nd_short_bi_price', None)
            is_2nd = second_short_bi_end and second_short_bi_price \
                    and second_short_bi_end == self.kline_x.cur_bi.start \
                    and second_short_bi_price == float(self.kline_x.cur_bi.high) \
                    and self.kline_x.cur_bi.direction == -1

            if self.kline_x.line_pre_low[-1] and self.cur_99_price <= self.kline_x.line_pre_low[-1] \
                    and (self.kline_x.is_duan(direction=Direction.SHORT) or is_2nd or is_1st):
                self.tns_short(SIGNAL_BREAK_SHORT)

    def tns_get_open_volume(self, stop_price=0):
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
        # msg = u'{}开仓价格：{}'.format(self.vt_symbol, self.cur_mi_price * self.symbol_size * self.margin_rate)
        # send_wx_msg(msg)
        if self.max_invest_pos > 0:
            max_unit = min(max_unit, self.max_invest_pos)

        avaliable_unit = int(avaliable / (self.cur_mi_price * self.symbol_size * self.margin_rate))
        self.write_log(u'投资资金总额{}允许的开仓数量：{},剩余资金允许得开仓数：{}，当前已经开仓手数:{}'
                       .format(invest_money, max_unit,
                               avaliable_unit,
                               self.position.long_pos + abs(self.position.short_pos)))

        if stop_price and self.single_lost_rate is not None and self.single_lost_rate < 0.1:
            # 损失金额
            lost_money = balance * self.single_lost_rate
            # 亏损金额=> 手数
            lost_unit = int(lost_money / (abs(self.cur_99_price - stop_price) * self.symbol_size))
            self.write_log(f'投资资金总额{balance}，亏损比率:{self.single_lost_rate}=> 亏损金额{lost_money} =》亏损手数:{lost_unit}')
        else:
            lost_unit = max_unit
        return min(max_unit, avaliable_unit, lost_unit)

    def tns_buy(self, signal):
        """处理Ordering状态的tns买入处理"""
        if self.entrust != 0:
            return

        # 未开仓
        # 开仓手数
        open_volume = self.tns_get_open_volume(stop_price=self.kline_x.line_pre_low[-1])

        # 创建一个持仓组件，记录指数合约开仓开仓价格，主力开仓价格
        # 不设置止损价
        grid = CtaGrid(direction=Direction.LONG,
                       vt_symbol=self.vt_symbol,
                       open_price=self.cur_99_price,
                       close_price=self.cur_99_price * 2,
                       stop_price=self.kline_x.line_pre_low[-1],
                       volume=open_volume,
                       type=signal,
                       snapshot={'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})

        # 使用主力合约发单,如果是股指，则激活对锁方式
        # order_type, 如果是FAK，会自动追单，如果是缺省LIMIT限价单，超时撤单
        order_ids = self.buy(price=self.cur_mi_price,
                             volume=grid.volume,
                             order_time=self.cur_datetime,
                             order_type=self.order_type,
                             lock=self.exchange==Exchange.CFFEX,
                             grid=grid)
        if len(order_ids) > 0:
            self.write_log(f'[事务开多] {signal} 委托成功{order_ids},开仓价：{grid.open_price}，数量：{grid.volume}'
                           f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')
            self.gt.dn_grids.append(grid)
            self.gt.save()

            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "price": self.cur_mi_price,
                "volume": open_volume,
                "operation": f"send buy order",
                "signal": signal
            }
            self.save_dist(d)
        else:
            self.write_error(f'[事务开多] {signal} 委托失败,开仓价：{grid.open_price}，数量：{grid.volume}'
                           f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')

    def tns_short(self, signal):
        """处理Ordering状态的tns开空处理"""
        if self.entrust != 0:
            return

        # 未开仓
        # 开仓手数
        open_volume = self.tns_get_open_volume(stop_price=self.kline_x.line_pre_high[-1])

        #  创建一个持仓组件，记录指数合约开仓开仓价格，主力开仓价格
        #  不设置止损价
        grid = CtaGrid(direction=Direction.SHORT,
                       vt_symbol=self.vt_symbol,
                       open_price=self.cur_99_price,
                       close_price=self.cur_99_price / 2,
                       stop_price=self.kline_x.line_pre_high[-1],
                       volume=open_volume,
                       type=signal,
                       snapshot={'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})

        # 使用主力合约发单
        # lock，如果当前合约是股指，使用锁仓模式
        # order_type, FAK或者限价单
        order_ids = self.short(price=self.cur_mi_price,
                               volume=grid.volume,
                               order_time=self.cur_datetime,
                               order_type=self.order_type,
                               lock=self.exchange == Exchange.CFFEX,
                               grid=grid)
        if len(order_ids) > 0:
            self.write_log(f'[事务开空] {signal} 委托成功{order_ids},开仓价：{grid.open_price}，数量：{grid.volume}'
                           f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')
            self.gt.up_grids.append(grid)
            self.gt.save()

            d = {
                "datetime": self.cur_datetime,
                "symbol": self.vt_symbol,
                "price": self.cur_mi_price,
                "volume": open_volume,
                "operation": f"send short order",
                "signal": signal
            }
            self.save_dist(d)
        else:
            self.write_error(f'[事务开空] {signal} 委托失败,开仓价：{grid.open_price}，数量：{grid.volume}'
                           f'，止损价:{grid.stop_price}，止盈价:{grid.close_price}')

    def grid_check_stop(self):
        """
        网格逐一止损/止盈检查 (根据主力价格进行止损止盈）
        :return:
        """
        if self.entrust != 0:
            return

        if not self.trading:
            if not self.backtesting:
                self.write_error(u'当前不允许交易')
            return

        x_signal = self.policy.chan_signals.get(self.kline_x.name,{})

        # 多单网格逐一止损/止盈检查：
        long_grids = self.gt.get_opened_grids(direction=Direction.LONG)

        for g in long_grids:

            # 持仓数量为0，或者处于委托状态的，不处理
            if g.volume == 0 or g.order_status:
                continue

            # 是否下跌均线,记录假信号
            if g.snapshot.get('ma_break', None) is None and len(self.kline_x.line_ma1) > 0:
                if self.kline_x.line_ma1[-1] > self.kline_x.close_array[-1] > self.cur_99_price:
                    self.write_log(f'多下破均线，假止损')
                    g.snapshot.update({'ma_break': self.kline_x.line_ma1[-1]})

                    ding_bi_end = x_signal.get('ding_bi_end',None)
                    if ding_bi_end and ding_bi_end in [self.kline_x.cur_bi.end , self.kline_x.cur_bi.start]:
                        self.write_log(f'顶背离后下坡均线，提高止损价')
                        g.stop_price = self.kline_x.line_pre_low[-1]

                    second_short_bi_end = x_signal.get('2nd_short_bi_end', None)
                    second_short_bi_price = x_signal.get('2nd_short_bi_price', None)
                    if second_short_bi_end and second_short_bi_price \
                            and second_short_bi_end in [self.kline_x.cur_bi.end , self.kline_x.cur_bi.start]\
                            and second_short_bi_price ==  self.kline_x.line_pre_high[-1] :
                        self.write_log(f'二卖后下坡均线，直接离场')
                        g.stop_price = self.kline_x.line_pre_high[-1]

            # 在记录假信号中轨离场后，再次突破上轨，提升止损价格
            if g.snapshot.get('ma_break', None) and self.cur_99_price >= self.kline_x.line_pre_high[-1]:
                self.write_log(f'多下破均线后，再次突破')
                g.stop_price = self.kline_x.line_pre_low[-1]
                g.snapshot.pop('ma_break', None)

            # 满足离场条件，或者碰到止损价格
            if g.stop_price > 0 and g.stop_price > self.kline_x.close_array[-1]:
                dist_record = dict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.vt_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_mi_price
                dist_record['operation'] = 'stop leave'
                dist_record['signals'] = '{}<{}'.format(self.cur_mi_price, g.stop_price)
                # 止损离场
                self.write_log(u'{} {}主力价:{} 触发多单止损线{},开仓价:{}, v：{}'.
                               format(self.cur_datetime,  self.vt_symbol, self.cur_mi_price, g.stop_price,
                                      g.open_price, g.volume))
                self.save_dist(dist_record)

                if self.tns_close_long_pos(g):
                    self.write_log(u'多单止盈/止损委托成功')

                else:
                    self.write_error(u'多单止损委托失败')

        # 空单网格止损检查
        short_grids = self.gt.get_opened_grids(direction=Direction.SHORT)
        for g in short_grids:
            # 持仓数量为0，或者处于委托状态的，不处理
            if g.volume == 0 or g.order_status:
                continue

            # 是否上破均线,记录假信号
            if g.snapshot.get('ma_break', None) is None and len(self.kline_x.line_ma1) > 0:
                if self.kline_x.line_ma1[-1] > self.kline_x.close_array[-1] > self.cur_99_price:
                    self.write_log(f'空上破均线，假止损')
                    g.snapshot.update({'ma_break': self.kline_x.line_ma1[-1]})

                    di_bi_end = x_signal.get('di_bi_end', None)
                    if di_bi_end and di_bi_end in [self.kline_x.cur_bi.end, self.kline_x.cur_bi.start]:
                        self.write_log(f'底背离后下破均线，降低止损价')
                        g.stop_price = self.kline_x.line_pre_high[-1]

                    second_buy_bi_end = x_signal.get('2nd_buy_bi_end', None)
                    second_buy_bi_price = x_signal.get('2nd_buy_bi_price', None)
                    if second_buy_bi_end and second_buy_bi_price \
                            and second_buy_bi_end in [self.kline_x.cur_bi.end, self.kline_x.cur_bi.start]\
                            and second_buy_bi_price == self.kline_x.line_pre_low[-1]:

                        self.write_log(f'二卖后下坡均线，直接离场')
                        g.stop_price = self.kline_x.line_pre_low[-1]

            # 在记录假信号中轨离场后，再次突破下轨，降低止损价格
            if g.snapshot.get('ma_break', None) and self.cur_99_price <= self.kline_x.line_pre_low[-1]:
                self.write_log(f'空上破均线后，再次突破')
                g.stop_price = self.kline_x.line_pre_high[-1]
                g.snapshot.pop('ma_break', None)

            if g.stop_price > 0 and g.stop_price < self.kline_x.close_array[-1] :
                dist_record = dict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.vt_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_mi_price
                dist_record['operation'] = 'stop leave'
                dist_record['signals'] = '{}<{}'.format(self.cur_99_price, g.stop_price)
                # 网格止损
                self.write_log(u'{} {} 价:{} 触发空单止损线:{},开仓价:{},,v：{}'.
                               format(self.cur_datetime,  self.vt_symbol,self.cur_mi_price, g.stop_price,
                                      g.open_price, g.volume))
                self.save_dist(dist_record)

                if self.tns_close_short_pos(g):
                    self.write_log(u'空单止盈/止损委托成功')

                else:
                    self.write_error(u'委托空单平仓失败')


class S154_Policy(CtaPolicy):
    """S154策略配套得事务"""

    def __init__(self, strategy):
        super().__init__(strategy)
        self.last_long_bi_start = ""
        self.last_short_bi_start = ""
        self.chan_signals = {}

    def to_json(self):
        """
        将数据转换成dict
        :return:
        """
        j = super().to_json()


        j['last_long_bi_start'] = self.last_long_bi_start
        j['last_short_bi_start'] = self.last_short_bi_start

        j['chan_signals'] = self.chan_signals
        return j

    def from_json(self, json_data):
        """
        将dict转化为属性
        :param json_data:
        :return:
        """
        super().from_json(json_data)

        self.last_long_bi_start = json_data.get('last_long_bi_start', "")
        self.last_short_bi_start = json_data.get('last_short_bi_start', "")
        self.chan_signals = json_data.get('chan_signals', {})

    def clean(self):
        """清除"""
        self.chan_signals = {}

