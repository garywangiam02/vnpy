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
from vnpy.trader.util_dingding import *


class Strategy_Boll_cross_Policy(CtaPolicy):
    """
    v1 重构海龟策略执行例子
    v2 增加多单持仓/空单持仓数量；增加限制正向加仓次数
    """

    def __init__(self, strategy):
        super().__init__(strategy)

        self.pos = 0
        self.entryPrice = 0

    def to_json(self):
        j = super(Strategy_Boll_cross_Policy, self).to_json()

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


class Strategy_Boll_cross(CtaProFutureTemplate):
    """15分钟级别、三均线策略
    策略：
    10，20，120均线，120均线做多空过滤
    MA120之上
        MA10 上穿 MA20，金叉，做多
        MA10 下穿 MA20，死叉，平多
    MA120之下
        MA10 下穿 MA20，死叉，做空
        MA10 上穿 MA20，金叉，平空

    # 回测要求：
    使用1分钟数据回测
    # 实盘要求：
    使用tick行情

    币安合约专用版

    """
    author = u'何文峰Sam'

    max_invest_percent = 30  # 最大投资仓位%， 0~100，
    single_lost_percent = 10  # 单次投入冒得风险比率,例如1%， 就是资金1%得亏损风险
    single_invest_pos = 1  # 单次固定开仓手数

    x_minute = 1  # K线分钟数

    x2_minute=15
    x2_ma1_len=222*4
    x2_para_boll_len=222
    x2_para_boll_std_rate=1.2

    volume = 2
    price_tick = 1
    invest_margin = 1

    # 进场价
    entryPrice = 0
    atr_value = 0  # K线得ATR均值

    #仓位相关
    base_symbol = 'RB99'  # 现货代码
    quote_symbol = 'RB99'  # 参照资金币代码

    #损失金额 是多少U
    eval_lost_money = 100

    base_pos=None
    quote_pos=None

    # 外部参数设置清单
    parameters = [
        "x_minute",
        "x2_minute",
        "x2_ma1_len",
        "x2_para_boll_len",
        "x2_para_boll_std_rate",
        "invest_margin",
        "volume",
        "price_tick",
        "backtesting"
    ]

    # 显示在界面上得变量
    variables = ["entryPrice"]

    # ----------------------------------------------------------------------
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting=None):
        """Constructor"""
        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        self.kline_x = None  # x分钟K线
        self.kline_x2 = None

        self.last_minute = None
        # 创建一个策略规则
        self.policy = Strategy_Boll_cross_Policy(strategy=self)

        self.display_bars = True

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 创建的M5 K线(使用分钟bar）
            kline_setting = {}
            kline_setting['name'] = u'M{}'.format(self.x_minute)  # k线名称
            kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
            kline_setting['price_tick'] = self.price_tick
            kline_setting['is_7x24'] = False
            self.kline_x = CtaMinuteBar(self, self.on_bar_x, kline_setting)
            self.klines.update({self.kline_x.name: self.kline_x})

            # 创建的M5 K线(使用分钟bar）
            kline_setting2 = {}
            kline_setting2['name'] = u'M{}'.format(self.x2_minute)  # k线名称
            kline_setting2['bar_interval'] = self.x2_minute  # K线的Bar时长
            kline_setting2['para_ma1_len'] = self.x2_ma1_len  # 第1条均线
            kline_setting2['para_boll_len'] = self.x2_para_boll_len  #第2条均线
            kline_setting2['para_boll_std_rate'] = self.x2_para_boll_std_rate  # 第3条均线
            kline_setting2['price_tick'] = self.price_tick
            kline_setting2['is_7x24'] = False
            self.kline_x2 = CtaHourBar(self, self.on_bar_x2, kline_setting2)
            self.klines.update({self.kline_x2.name: self.kline_x2})

            self.export_klines()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

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
            # {'name': f'ma{self.kline_x.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
            # {'name': f'ma{self.kline_x.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
            # {'name': f'ma{self.kline_x.para_ma3_len}', 'source': 'line_bar', 'attr': 'line_ma3', 'type_': 'list'},
        ]

        self.kline_x2.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.kline_x2.name)))

        self.kline_x2.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
            {'name': f'bollup', 'source': 'line_bar', 'attr': 'line_boll_upper', 'type_': 'list'},
            {'name': f'bollmid', 'source': 'line_bar', 'attr': 'line_boll_middle', 'type_': 'list'},
            {'name': f'bolldown', 'source': 'line_bar', 'attr': 'line_boll_lower', 'type_': 'list'},
            {'name': f'ma{self.kline_x.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
        ]


    def dingding(self,str1):
        dingding(self.strategy_name+":\n"+str1+".",backtesting=self.backtesting)


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
            balance, avaliable, percent, limit = self.cta_engine.get_account()

            msg = u'初始化{}成功.\n持仓：多{}手,空:{}手\n帐户资金:{}\n可用:{}'.format(self.vt_symbol, self.position.long_pos,
                                                self.position.short_pos,balance,avaliable)
            self.write_log(msg)
            self.dingding(msg)
            time.sleep(3)
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
                self.kline_x.add_bar(bar, bar_freq=1)

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
        #

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

        self.dingding(u'{},OnTrade(),traded_price:{},当前持仓：{} '.format(self.cur_datetime,trade.price, self.position.pos))


        self.sync_data()

    # ----------------------------------------------------------------------
    def on_order(self, order: OrderData):
        """报单更新"""
        # 未执行的订单中，存在是异常，删除
        self.write_log(u'报单更新，gateway:{0},orderID:{1}'.format(order.gateway_name,order.orderid))
        orderkey = order.gateway_name + u'.' + str(order.orderid)
        # if orderkey in self.uncompletedOrders:
        if orderkey in  self.active_orders:
            if order.volume == order.traded:
                self.__onOrderAllTraded(order)

            elif order.traded > 0 \
                    and not order.volume == order.traded and\
                    order.status not in [Status.CANCELLED, Status.REJECTED]:
                # 委托单部分成交
                self.__onOrderPartTraded(order)

            elif order.offset == Offset.OPEN and order.status in [Status.CANCELLED, Status.REJECTED]:
                # 开仓委托单被撤销
                self.__onOpenOrderCanceled(order)

            elif order.offset == Offset.CLOSE and order.status in [Status.CANCELLED, Status.REJECTED]:
                # 平仓委托单被撤销
                self.__onCloseOrderCanceled(order)

            else:
                self.write_log(u'委托单未完成,total:{0},tradeStatus:{1}'
                                 .format(order.volume, order.traded, ))
        pass

    def __onOrderAllTraded(self, order):
        """
        订单全部成交
        :param order:
        :return:
        """
        self.write_log(u'onOrderAllTraded(),{0},委托单全部完成'.format(order.datetime))
        orderkey = order.gateway_name + u'.' + str(order.orderid)

        # 开空仓完成(short)
        if self.active_orders[orderkey]['direction'] == Direction.SHORT and order.offset == Offset.OPEN:
            self.write_log(u'{0}开空仓完成(short)'.format(order.vt_symbol))
            self.policy.pos -= order.traded
            # self.policy.pos = 0
            self.pos=self.policy.pos
            self.policy.save()

        # 开多仓完成(Long)
        if self.active_orders[orderkey]['direction'] == Direction.LONG and order.offset == Offset.OPEN:
            self.write_log(u'{}开多单完成(Buy)'.format(order.vt_symbol))
            # 通过orderID，找到对应的网格
            self.policy.pos += order.traded
            self.pos=self.policy.pos
            self.policy.save()

        # 平空仓完成(cover)
        if self.active_orders[orderkey]['direction'] == Direction.LONG and order.offset == Offset.CLOSE:
            self.write_log(u'{0}平空完成(cover)'.format(order.vt_symbol))
            self.policy.pos += order.traded
            self.pos = self.policy.pos
            self.policy.save()

        # 平多仓完成(sell)
        if self.active_orders[orderkey]['direction'] == Direction.SHORT and order.offset == Offset.CLOSE:
            self.write_log(u'{0}平多完成(sell)'.format(order.vt_symbol))
            self.policy.pos -= order.traded
            self.pos = self.policy.pos
            self.policy.save()

        self.entrust = 0
        try:
            del self.active_orders[orderkey]
        except Exception as ex:
            self.write_log(u'onOrder uncompletedOrders中找不到{0}'.format(orderkey))

    def __onOrderPartTraded(self, order):
        """订单部分成交"""
        self.write_log(u'onOrderPartTraded(),{0},委托单部分完成'.format(order.datetime ))
        orderkey = order.gateway_name + u'.' + order.orderid
        o = self.active_orders.get(orderkey,None)
        if o is not None:
            self.write_log(u'更新订单{}部分完成:{}=>{}'.format(o,o.get('traded',0.0),order.traded))
            self.active_orders[orderkey]['traded'] = order.traded
            if self.active_orders[orderkey]['direction'] == Direction.LONG:
                self.pos+=order.traded
                self.policy.pos = self.pos
            if self.active_orders[orderkey]['direction'] == Direction.SHORT:
                self.pos-=order.traded
                self.policy.pos = self.pos

            self.policy.save()
        else:
            self.write_log(u'异常，找不到委托单:{0}'.format(orderkey))

    def __onOpenOrderCanceled(self, order):
        """
        委托开仓单撤销

        :param order:
        :return:
        """
        self.write_log(
            u'__onOpenOrderCanceled(),{},{} {} 委托开仓单已撤销'.format(order.datetime, order.direction, order.vt_symbol))

        orderkey = order.gateway_name + u'.' + order.orderid
        self.policy.pos += order.traded

        del self.active_orders[orderkey]
        self.entrust = 0

        #进入开仓追单
        if order.direction == Direction.SHORT:
            order_volume = order.volume - order.traded
            shortPrice = self.cur_price - 2 * self.price_tick
            ref = self.short(price=shortPrice, volume=order_volume)
            if ref:
                msg = u'{}策略追单做空ＢＴＣ永续：{}手，价格：{}'.format(self.strategy_name, order_volume, self.cur_price)
                self.write_log(msg)
                if not self.backtesting:
                    self.dingding(msg)

                order = self.active_orders.get(ref[0], None)
                if order:
                    order['Canceled'] = False

        elif order.direction == Direction.LONG:
            order_volume=order.volume-order.traded
            buyPrice = self.cur_price + 2*self.price_tick
            ref = self.buy(price=buyPrice, volume=order_volume)
            if ref:
                msg = u'{}策略追单做多ＢＴＣ永续：{}手，价格：{}'.format(self.strategy_name,order_volume, self.cur_price)
                self.write_log(msg)
                if not self.backtesting:
                    self.dingding(msg)

                order = self.active_orders.get(ref[0], None)
                if order:
                    order['Canceled'] = False

    def __onCloseOrderCanceled(self, order):
        """委托平仓单撤销"""
        self.write_log(u'{},{}委托平仓单已撤销，委托数:{},成交数:{},未成交:{}'
                         .format(order.datetime, order.vt_symbol,
                                 order.volume, order.traded,
                                 order.volume - order.traded))

        orderkey = order.gateway_name + u'.' + order.orderid


        # if self.policy.pos < 0:
        #     self.policy.pos = 0

        del self.active_orders[orderkey]
        self.entrust = 0

        # 进入平仓追单
        if order.direction == Direction.LONG:
            self.policy.pos += order.traded
            order_volume = order.volume - order.traded
            coverPrice = self.cur_price + 2 * self.price_tick
            ref = self.cover(price=coverPrice, volume=order_volume)
            if ref:
                msg = u'{}策略追单平空ＢＴＣ永续：{}手，价格：{}'.format(self.strategy_name, order_volume, self.cur_price)
                self.write_log(msg)
                self.dingding(msg)


        if order.direction == Direction.SHORT:
            self.policy.pos -= order.traded
            order_volume = order.volume - order.traded
            sellPrice = self.cur_price - 2 * self.price_tick
            ref = self.sell(price=sellPrice, volume=order_volume)
            if ref:
                msg = u'{}策略追单平多ＢＴＣ永续：{}手，价格：{}'.format(self.strategy_name,order_volume, self.cur_price)
                self.write_log(msg)
                self.dingding(msg)

    # ----------------------------------------------------------------------
    def on_tick(self, tick: TickData):
        """行情更新（实盘运行，从tick导入）
        :type tick: object
        """
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not (self.inited):
            return

        try:
            tick.datetime = tick.datetime.replace(tzinfo=None)
        except:
            print("去除时区有问题,兄弟你是跑了币安不是火币吧?")
        # 更新tick 到dict
        self.tick_dict.update({tick.vt_symbol: tick})

        if tick.vt_symbol == self.vt_symbol:
            # 设置为当前主力tick
            self.cur_tick = tick
            self.cur_price = tick.last_price
        else:
            # 所有非主力的tick，都直接返回
            return

        # 更新策略执行的时间

        self.cur_datetime = tick.datetime

        # 推送Tick到kline_x
        self.kline_x.on_tick(tick)
        self.kline_x2.on_tick(tick)



        # 执行撤单逻辑
        self.tns_cancel_logic(dt=self.cur_datetime)

        # 实盘这里是每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute

        # self.base_pos = self.cta_engine.posBufferDict.get(self.base_symbol, None)
        self.base_pos = self.cta_engine.get_position(self.base_symbol)
        self.quote_pos = self.cta_engine.get_position(self.quote_symbol)


    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """分钟K线数据更新（仅用于回测时，从策略外部调用)"""

        # 更新策略执行的时间（用于回测时记录发生的时间）
        # 回测数据传送的bar.datetime，为bar的开始时间，所以，到达策略时，当前时间为bar的结束时间
        # 本策略采用1分钟bar回测
        self.cur_datetime = bar.datetime + timedelta(minutes=1)
        self.cur_price = bar.close_price
        # 推送bar到x分钟K线
        self.kline_x.add_bar(bar)
        self.kline_x2.add_bar(bar)
        # 执行撤单逻辑
        self.tns_cancel_logic(dt=self.cur_datetime)

        #
        # self.ma1 = self.kline_x.line_ma1[-1]
        # self.ma2 = self.kline_x.line_ma2[-1]
        # self.ma3 = self.kline_x.line_ma3[-1]

        self.put_event()

        # 执行事务逻辑判断
        # self.tns_update_price()
        #
        # if self.position.pos != 0:
        #     self.tns_check_stop()
        #     self.tns_add_logic()
        # else:
        #     self.tns_open_logic()

    def on_bar_x(self, bar: BarData):
        """  分钟K线数据更新，实盘时，由self.kline_x的回调"""

        if self.display_bars and not self.backtesting:
            # 调用kline_x的显示bar内容
            self.write_log(self.kline_x.get_last_bar_str())

        # 交易逻辑
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited or not self.trading:
            return

        """
                趋势逻辑
                长均线向上，价格在长均线上方时，空趋势/无趋势-》多趋势
                长均线向下，价格在长均线下方时，多趋势/无趋势-》空趋势
                """

        if len(self.kline_x2.line_boll_middle) < 2:
            # print("line_boll_middle")
            return


        # print("Close:{},M15Ma:{},M15Boll:{},{},{}".format(bar.close_price,self.kline_x2.line_ma1[-1],\
        #                                                   self.kline_x2.line_boll_upper[-1],self.kline_x2.line_boll_middle[-1],self.kline_x2.line_boll_lower[-1]))

        if self.pos == 0 and self.entrust == 0:



            # 开多
            if bar.close_price>self.kline_x2.line_boll_upper[-1]\
                and bar.close_price>self.kline_x2.line_ma1[-1]:
                # and self.kline_x.line_ma3[-1] > self.kline_x.line_ma3[-2] \
                # and self.kline_x.line_ma1[-1] > self.kline_x.line_ma2[-1]:



                #确定要下单。 计算仓位
                trade_volume = 1

                self.write_log(
                    u'永续{}开多{}手 价格：{}'.format(self.vt_symbol, trade_volume, bar.close_price - self.price_tick))
                self.dingding( u'永续{}开多{}手 价格：{}'.format(self.vt_symbol, trade_volume, bar.close_price - self.price_tick))
                order_ids = self.buy(price=bar.close_price + self.price_tick,
                                     volume=trade_volume,
                                     vt_symbol=self.vt_symbol,
                                     order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}开多失败'.format(self.vt_symbol))
                    self.dingding(u'永续{}开多失败'.format(self.vt_symbol))

            # 开空
            if bar.close_price < self.kline_x2.line_boll_lower[-1] \
                    and bar.close_price < self.kline_x2.line_ma1[-1]:

                # 确定要下单。 计算仓位
                trade_volume = 1

                self.write_log(
                    u'永续{}开空{}手 价格：{}'.format(self.vt_symbol, trade_volume, bar.close_price - self.price_tick))
                self.dingding(u'永续{}开空{}手 价格：{}'.format(self.vt_symbol, trade_volume, bar.close_price - self.price_tick))
                order_ids = self.short(price=bar.close_price -self.price_tick,
                                       volume=trade_volume,
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}开空失败'.format(self.vt_symbol))
                    self.dingding(u'永续{}开空失败'.format(self.vt_symbol))

        else:
            # 平多
            if self.pos > 0 \
                    and bar.close_price < self.kline_x2.line_boll_middle[-1] \
                    and self.entrust == 0:
                self.write_log(
                    u'永续{}平多{}手 价格：{}'.format(self.vt_symbol, abs(self.pos), bar.close_price - self.price_tick))
                self.dingding(u'永续{}平多{}手 价格：{}'.format(self.vt_symbol, abs(self.pos), bar.close_price - self.price_tick))
                order_ids = self.sell(price=bar.close_price -self.price_tick,
                                      volume=abs(self.pos),
                                      vt_symbol=self.vt_symbol,
                                      order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}平多失败'.format(self.vt_symbol))
                    self.dingding(u'永续{}平多失败'.format(self.vt_symbol))

            # 平空
            if self.pos < 0 \
                    and bar.close_price > self.kline_x2.line_boll_middle[-1] \
                    and self.entrust == 0:
                self.write_log(
                    u'永续{}平空{}手 价格：{}'.format(self.vt_symbol, abs(self.pos), bar.close_price + self.price_tick))
                self.dingding(u'永续{}平空{}手 价格：{}'.format(self.vt_symbol, abs(self.pos), bar.close_price + self.price_tick))
                order_ids = self.cover(price=bar.close_price + self.price_tick,
                                       volume=abs(self.pos),
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}平空失败'.format(self.vt_symbol))
                    self.dingding(u'永续{}平空失败'.format(self.vt_symbol))

    def on_bar_x2(self,bar: BarData):

        if self.display_bars and not self.backtesting:
            # 调用kline_x的显示bar内容
            self.write_log(self.kline_x2.get_last_bar_str())


        # 交易逻辑
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited or not self.trading:
            return

        if len(self.kline_x2.line_boll_middle) < 2:
            return

        self.sync_data()


