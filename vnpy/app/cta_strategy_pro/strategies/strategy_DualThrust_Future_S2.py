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

class TripleMa_Policy(CtaPolicy):
    """
    v1 重构海龟策略执行例子
    v2 增加多单持仓/空单持仓数量；增加限制正向加仓次数
    """

    def __init__(self, strategy):
        super().__init__(strategy)
        self.pos = 0
        self.entryPrice = 0

    def to_json(self):
        j = super(TripleMa_Policy, self).to_json()

        j['pos'] = self.strategy.pos
        j['entryPrice'] = self.strategy.entryPrice

        return j

    def from_json(self, json_data):
        super().from_json(json_data)

        self.strategy.pos = json_data.get('pos', 0)
        self.strategy.entryPrice = json_data.get('entryPrice', 0)

    def clean(self):
        self.pos = 0
        self.entryPrice = 0


class Strategy_DualThrust_Future_S2(CtaProFutureTemplate):
    """
    策略逻辑：日内突破可以获得盈利
    策略内容：计算前N天的最高价－收盘价和收盘价－最低价。然后取这2N个价差的最大值，乘以k值。
    把结果称为触发值。在今天的开盘，记录开盘价，然后在价格超过上轨（开盘＋触发值）时马上买入，或者价格低于下轨（开盘－触发值）时马上卖空。
    没有明确止损。这个系统是反转系统，也就是说，如果在价格超过（开盘＋触发值）时手头有空单，则平空开多。
    同理，如果在价格低于（开盘－触发值）时手上有多单，则平多开空。

    """
    author = u'何文峰Sam'

    single_invest_pos = 1  # 单次固定开仓手数
    invest_margin = 1

    x_day = 1  # K线分钟数
    x_ma3_len = 20

    k = 0.5
    volume = 2

    #取前面X天K形成通道
    pre_bar_num=6

    # 进场价
    entryPrice = 0
    current_open=None
    buy_line = 0
    sell_line=0

    # 外部参数设置清单
    parameters = [
        "x_day",
        "k",
        "pre_bar_num",
        "x_ma3_len",
        "pre_bar_num",
        "backtesting",
        "volume"
    ]

    # 显示在界面上得变量
    variables = ["entryPrice"]

    # ----------------------------------------------------------------------
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting=None):
        """Constructor"""
        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        self.kline_D1 = None  # x分钟K线
        self.last_minute = None
        # 创建一个策略规则
        self.policy = TripleMa_Policy(strategy=self)

        self.display_bars = True

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 创建的M5 K线(使用分钟bar）
            kline_setting = {}
            kline_setting['name'] = u'D{}'.format(self.x_day)  # k线名称
            kline_setting['bar_interval'] = self.x_day  # K线的Bar时长
            kline_setting['para_ma3_len']=self.x_ma3_len
            kline_setting['price_tick'] = self.price_tick
            kline_setting['is_7x24'] = False
            self.kline_D1 = CtaDayBar(self, self.on_bar_D1, kline_setting)
            self.klines.update({self.kline_D1.name: self.kline_D1})

            kline1_setting = {}
            kline1_setting['name'] = u'M5'  # 1分钟k线名称
            kline1_setting['bar_interval'] = 2  # K线的Bar时长
            kline1_setting['price_tick'] = self.price_tick
            kline1_setting['is_7x24'] = False
            self.kline_m5 = CtaMinuteBar(self, self.on_bar_M5, kline1_setting)
            self.klines.update({self.kline_m5.name: self.kline_m5})

            self.export_klines()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

        # 写入文件
        import os
        self.kline_m5.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.kline_m5.name)))

        self.kline_m5.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
        ]

    # ----------------------------------------------------------------------
    def on_init(self):
        """初始化 """
        self.write_log(u'策略初始化')
        if self.inited:
            self.write_log(u'已经初始化过，不再执行')
            return

        self.pos = 0  # 初始化持仓
        self.entrust = 0  # 初始化委托状态
        if not self.backtesting:

            # 这里是使用gateway历史数据
            if self.init_data():
                self.inited = True
            else:
                self.write_error(u'初始数据失败')
                return

            # 从本地持久化json文件中，恢复policy的记录数据
            self.policy.load()

            # 从本地化网格json文件中，恢复所有持仓
            self.init_position()

            msg = u'{}初始化,{} 多{}手,空:{}手'.format(self.strategy_name, self.vt_symbol, self.position.long_pos,
                                                self.position.short_pos)
            self.write_log(msg)
        else:
            self.inited = True

        self.put_event()
        self.write_log(u'策略初始化完成')

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
                self.kline_D1.add_bar(bar, bar_freq=1)

            return True

        except Exception as ex:
            self.write_error(u'init_data_from_tdx Exception:{},{}'.format(str(ex), traceback.format_exc()))
            return False

    def init_position(self):
        """
        #tosam
        继承宇模板，模板从网格获取position类对象， 策略直接从json中获取
        因为币安永续合约设置净仓模式。 所以不需要考虑同时持有多空仓位的情况
        :return:
        """
        if self.position.pos != self.pos and self.pos != 0:
            self.write_log("通过policy持仓恢复Position持仓")
            self.position.pos = self.pos
            if self.pos > 0:
                self.position.long_pos = self.pos
            if self.pos < 0:
                self.position.short_pos = self.pos

            self.write_log(
                "恢复position对象：pos:{},long_pos：{}，short_pos：{}".format(self.position.pos, self.position.long_pos,
                                                                      self.position.short_pos))

    def get_positions(self):
        """
        To sam  去除网格化进行时
        获取策略当前持仓(重构，使用主力合约）
        由持仓对比模块由引擎调用
        :return: [{'vt_symbol':symbol,'direction':direction,'volume':volume]
        """
        if not self.position:
            return []
        pos_list = []
        if self.position.long_pos > 0:
            pos_list.append({'vt_symbol': self.vt_symbol,
                             'direction': 'long',
                             'volume': self.position.long_pos,
                             'price': self.entryPrice})

        if abs(self.position.short_pos) > 0:
            pos_list.append({'vt_symbol': self.vt_symbol,
                             'direction': 'short',
                             'volume': abs(self.position.short_pos),
                             'price': self.entryPrice})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'{}当前持仓:{}'.format(self.strategy_name, pos_list))
        return pos_list


    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'保存k线缓存数据')
            self.save_klines_to_cache()

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()

    def on_start(self):
        """启动策略（必须由用户继承实现）"""
        self.write_log(u'启动')
        self.trading = True
        self.put_event()

    # ----------------------------------------------------------------------
    def on_stop(self):
        """停止策略（必须由用户继承实现）"""
        self.active_orders.clear()
        self.pos = 0
        self.entrust = 0

        self.write_log(u'停止')
        self.put_event()

    # ----------------------------------------------------------------------
    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},OnTrade(),当前持仓：{} '.format(self.cur_datetime, self.position.pos))

        dist_record = OrderedDict()
        if self.backtesting:
            dist_record['datetime'] = trade.datetime
        else:
            dist_record['datetime'] = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')

        dist_record['volume'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['symbol'] = trade.vt_symbol

        if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'buy'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            self.entryPrice = trade.price

        if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'short'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            self.entryPrice = trade.price

        if trade.direction == Direction.LONG and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'cover'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            self.entryPrice = 0

        if trade.direction == Direction.SHORT and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'sell'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos
            self.entryPrice = 0

        self.save_dist(dist_record)
        self.pos = self.position.pos

        self.sync_data()

    # ----------------------------------------------------------------------
    def on_order(self, order: OrderData):
        """报单更新"""
        self.write_log(
            u'OnOrder()报单更新:{}'.format(order.__dict__))

        if order.vt_orderid in self.active_orders:
            # 全部成交
            if order.status == Status.ALLTRADED:
                self.on_order_all_traded(order)

            # 撤单(含部分成交后拒单）/拒单
            elif order.status in [Status.CANCELLED, Status.REJECTED]:
                if order.status == Status.REJECTED:
                    self.send_wechat(f'委托单被拒:{order.__dict__}')

                if order.offset == Offset.OPEN:
                    self.on_order_open_canceled(order)
                else:
                    self.on_order_close_canceled(order)
        else:
            self.write_error(f'委托单{order.vt_orderid}不在本策略的活动订单列表中')

        if len(self.active_orders) == 0:
            self.entrust = 0

        self.put_event()  # 更新监控事件


    def on_tick(self, tick: TickData):
        """行情更新（实盘运行，从tick导入）
        :type tick: object
        """

        # print(tick.last_price)

        # 首先检查是否是实盘运行还是数据预处理阶段
        if not (self.inited):
            return

        # 更新tick 到dict
        self.tick_dict.update({tick.vt_symbol: tick})

        if tick.vt_symbol == self.vt_symbol:
            # 设置为当前主力tick
            self.cur_mi_tick = tick
            self.cur_mi_price = tick.last_price
        else:
            # 所有非主力的tick，都直接返回
            return

        # 更新策略执行的时间
        self.cur_datetime = tick.datetime
        # self.write_log(f'{tick.__dict__}')
        # self.write_log(f'{tick.datetime}：{tick.symbol}-->{tick.last_price},inited:{self.inited},tranding:{self.trading}')


        # 推送Tick到kline_x
        self.kline_D1.on_tick(tick)
        self.kline_m5.on_tick(tick)

        # self.tns_update_price()

        # 执行撤单逻辑
        self.tns_cancel_logic(dt=self.cur_datetime,reopen=True)

        # 实盘这里是每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute


    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """分钟K线数据更新（仅用于回测时，从策略外部调用)"""

        # 更新策略执行的时间（用于回测时记录发生的时间）
        # 回测数据传送的bar.datetime，为bar的开始时间，所以，到达策略时，当前时间为bar的结束时间
        # 本策略采用1分钟bar回测
        self.cur_datetime = bar.datetime + timedelta(minutes=1)
        self.cur_mi_price = bar.close_price
        # 推送bar到x分钟K线
        # print(f"On_bar() bartime:{bar.datetime},cur_datetime:{self.cur_datetime}")
        # self.kline_D11.add_bar(bar,True,1)
        # 推送bar到x分钟K线
        self.kline_D1.add_bar(bar)
        self.kline_m5.add_bar(bar)
        # 执行撤单逻辑
        self.tns_cancel_logic(self.cur_datetime, reopen=True)
        self.put_event()





    def on_bar_D1(self, bar: BarData):
        """  分钟K线数据更新，实盘时，由self.kline_D1的回调"""

        if self.display_bars and not self.backtesting:
            # 调用kline_x的显示bar内容
            self.write_log(self.kline_D1.get_last_bar_str())

        # 交易逻辑
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited or not self.trading:
            return

    def on_bar_M5(self, bar: BarData):
        """  分钟K线数据更新，实盘时，由self.kline_D1的回调"""


        if self.display_bars and not self.backtesting:
            # 调用kline_x的显示bar内容
            self.write_log(self.kline_m5.get_last_bar_str())

        # 交易逻辑
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited or not self.trading:
            print(f"inited:{self.inited},trading:{self.trading}")
            return

        if len(self.kline_D1.line_ma3)<3:
            print(f"len_ma:{len(self.kline_D1.line_ma3)}")
            return


        if self.current_open==None and not self.backtesting:
            temp_time= bar.datetime.strftime("%H:%M:%S")
            if temp_time != '09:00:00':
                self.current_open = self.kline_D1.line_bar[-2].close_price

        if bar.datetime.strftime("%H:%M:%S") == '09:00:00':
            # print(f'获取到开盘价：{bar.open_price}')
            self.current_open = bar.open_price

        if self.current_open == None:
            self.write_error("self.current_open == None")
            return

        #获取前几天
        HH = self.kline_D1.high_array[-self.pre_bar_num:].max()
        HC = self.kline_D1.close_array[-self.pre_bar_num:].max()
        LC = self.kline_D1.close_array[-self.pre_bar_num:].min()
        LL = self.kline_D1.low_array[-self.pre_bar_num:].min()

        range = max(HH - LC, HC - LL)
        self.buy_line = self.current_open + range * self.k  # 上轨
        self.sell_line = self.current_open - range * self.k  # 下轨

        self.write_log(f"Price:{self.cur_mi_price},buy_line:{self.buy_line},sell_line:{self.sell_line},Ma:{self.kline_D1.line_ma3[-1]}")

        if self.pos == 0 and self.entrust == 0:

            if self.cur_mi_price > self.buy_line and self.cur_mi_price> self.kline_D1.line_ma3[-1]:

                self.write_log(
                    u'永续{}开多{}手 价格：{}'.format(self.vt_symbol, self.volume, bar.close_price - self.price_tick))
                order_ids = self.buy(price=bar.close_price + self.price_tick,
                                     volume=self.volume,
                                     vt_symbol=self.vt_symbol,
                                     order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}开多失败'.format(self.vt_symbol))

            # 开空
            if self.cur_mi_price < self.sell_line and self.cur_mi_price< self.kline_D1.line_ma3[-1]:

                self.write_log(
                    u'永续{}开空{}手 价格：{}'.format(self.vt_symbol, self.volume, bar.close_price - self.price_tick))
                order_ids = self.short(price=bar.close_price - self.price_tick,
                                       volume=self.volume,
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}开空失败'.format(self.vt_symbol))

        else:
            # 平多
            if self.pos > 0 \
                    and self.cur_mi_price < self.sell_line \
                    and self.entrust == 0:
                self.write_log(
                    u'永续{}平多{}手 价格：{}'.format(self.vt_symbol, self.volume, bar.close_price - self.price_tick))
                order_ids = self.sell(price=bar.close_price - self.price_tick,
                                      volume=abs(self.pos),
                                      vt_symbol=self.vt_symbol,
                                      order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}平多失败'.format(self.vt_symbol))

            # 平空
            if self.pos < 0 \
                    and self.cur_mi_price > self.buy_line \
                    and self.entrust == 0:
                self.write_log(
                    u'永续{}平空{}手 价格：{}'.format(self.vt_symbol, self.volume, bar.close_price + self.price_tick))
                order_ids = self.cover(price=bar.close_price + self.price_tick,
                                       volume=abs(self.pos),
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}平空失败'.format(self.vt_symbol))


