# encoding: UTF-8

# 首先写系统内置模块
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta

# 其次，导入vnpy的基础模块
from vnpy.app.cta_crypto import (
    CtaFutureTemplate,
    StopOrder,
    Direction,
    Offset,
    Status,
    Interval,
    TickData,
    BarData,
    TradeData,
    OrderData
)
from vnpy.component.cta_grid_trade import CtaGrid
from vnpy.component.cta_line_bar import CtaMinuteBar
from vnpy.component.cta_policy import CtaPolicy
from vnpy.trader.util_wechat import send_wx_msg
from vnpy.trader.utility import get_underlying_symbol


class TripleMa_Policy(CtaPolicy):
    """
    v1 重构海龟策略执行例子
    v2 增加多单持仓/空单持仓数量；增加限制正向加仓次数
    """

    def __init__(self, strategy):
        super().__init__(strategy)

        # 多/空
        # self.tns_direction = None

        # 增加观测信号
        # self.sub_tns = {}

        # # 事务开启后，最高价/最低价
        # self.tns_high_price = 0
        # self.tns_low_price = 0
        #
        # # 事务首次开仓价
        # self.tns_open_price = 0
        # # 最后一次顺势加仓价格
        # self.last_open_price = 0
        # # 最后一次逆势加仓价格
        # self.last_under_open_price = 0
        #
        # # 事务止损价
        # self.tns_stop_price = 0
        #
        # # 高位回落或低位回升x跳,离场
        # self.tns_rtn_pips = 0

        # # 允许加仓
        # self.allow_add_pos = False
        # # 顺势可加仓次数
        # self.add_pos_count_above_first_price = 0
        # # 逆势可加仓次数
        # self.add_pos_count_under_first_price = 0

        self.pos=0
        self.entryPrice = 0

    def to_json(self):
        j = super(TripleMa_Policy, self).to_json()

        j['pos'] = self.strategy.pos
        j['entryPrice'] = self.strategy.entryPrice

        return j

    def from_json(self, json_data):
        super().from_json(json_data)

        self.strategy.pos=json_data.get('pos',0)
        self.strategy.entryPrice = json_data.get('entryPrice', 0)


    def clean(self):
        self.pos = 0
        self.entryPrice = 0



class Strategy_Simple3Ma_binance(CtaFutureTemplate):
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

    max_invest_percent = 50  # 最大投资仓位%， 0~100，
    single_lost_percent = 1  # 单次投入冒得风险比率,例如1%， 就是资金1%得亏损风险
    single_invest_pos = 1  # 单次固定开仓手数


    x_minute = 60  # K线分钟数
    x_ma1_len = 17
    x_ma2_len = 24
    x_ma3_len = 78
    volume = 2

    #进场价
    entryPrice=0

    atr_value = 0  # K线得ATR均值


    # 外部参数设置清单
    parameters = [
        "x_minute",
        "x_ma1_len",
        "x_ma2_len",
        "x_ma3_len",
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
        self.last_minute = None
        # 创建一个策略规则
        self.policy = TripleMa_Policy(strategy=self)

        self.display_bars = True

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 创建的M5 K线(使用分钟bar）
            kline_setting = {}
            kline_setting['name'] = u'M{}'.format(self.x_minute)  # k线名称
            kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
            kline_setting['para_ma1_len'] = self.x_ma1_len  # 第1条均线
            kline_setting['para_ma2_len'] = self.x_ma2_len  # 第2条均线
            kline_setting['para_ma3_len'] = self.x_ma3_len  # 第3条均线
            kline_setting['price_tick'] = self.price_tick
            kline_setting['is_7x24'] = True
            self.kline_x = CtaMinuteBar(self, self.on_bar_x, kline_setting)
            self.klines.update({self.kline_x.name: self.kline_x})

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
            {'name': f'ma{self.kline_x.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
            {'name': f'ma{self.kline_x.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
            {'name': f'ma{self.kline_x.para_ma3_len}', 'source': 'line_bar', 'attr': 'line_ma3', 'type_': 'list'},
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


            #从本地化网格json文件中，恢复所有持仓
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
                if last_bar_dt and bar.datetime < last_bar_dt:
                    return
                self.cur_price = bar.close_price
                self.cur_datetime = bar.datetime
                if self.cur_datetime > dt_now - timedelta(days=1) and not self.display_bars:
                    self.display_bars = True
                self.kline_x.add_bar(bar)

            self.cta_engine.load_bar(vt_symbol=self.vt_symbol,
                                     days=load_days,
                                     interval=Interval.MINUTE,
                                     callback=on_bar_cb)
            return True

        except Exception as ex:
            self.write_error(u'init_data Exception:{},{}'.format(str(ex), traceback.format_exc()))
            return False

    def init_position(self):
        """
        #tosam
        继承宇模板，模板从网格获取position类对象， 策略直接从json中获取
        因为币安永续合约设置净仓模式。 所以不需要考虑同时持有多空仓位的情况
        :return:
        """
        if self.position.pos != self.pos and self.pos !=0:
            self.write_log("通过policy持仓恢复Position持仓")
            self.position.pos = self.pos
            if self.pos>0:
                self.position.long_pos = self.pos
            if self.pos<0:
                self.position.short_pos= self.pos

            self.write_log("恢复position对象：pos:{},long_pos：{}，short_pos：{}".format(self.position.pos,self.position.long_pos,self.position.short_pos))

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

    # def on_order_all_traded(self, order: OrderData):
    #     """委托单全部成交"""
    #     order_info = self.active_orders.get(order.vt_orderid)
    #     grid = order_info.get('grid', None)
    #     if grid:
    #         # 移除grid的委托单中order_id
    #         if order.vt_orderid in grid.order_ids:
    #             grid.order_ids.remove(order.vt_orderid)
    #
    #         # 网格的所有委托单已经执行完毕
    #         if len(grid.order_ids) == 0:
    #             grid.order_status = False
    #             grid.traded_volume = 0
    #
    #             # 平仓完毕（cover， sell）
    #             if order.offset != Offset.OPEN:
    #                 grid.open_status = False
    #                 grid.close_status = True
    #
    #                 self.write_log(f'{grid.direction.value}单已平仓完毕,order_price:{order.price}'
    #                                + f',volume:{order.volume}')
    #
    #                 self.write_log(f'移除网格:{grid.to_json()}')
    #                 self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])
    #
    #             # 开仓完毕( buy, short)
    #             else:
    #                 grid.open_status = True
    #                 msg = f'{grid.direction.value}单已开仓完毕,order_price:{order.price},volume:{order.volume}'
    #                 self.write_log(msg=msg)
    #                 self.send_wechat(msg=msg)
    #
    #         # 网格的所有委托单部分执行完毕
    #         else:
    #             old_traded_volume = grid.traded_volume
    #             grid.traded_volume += order.volume
    #
    #             self.write_log(f'{grid.direction.value}单部分{order.offset}仓，'
    #                            + f'网格volume:{grid.volume}, traded_volume:{old_traded_volume}=>{grid.traded_volume}')
    #
    #             self.write_log(f'剩余委托单号:{grid.order_ids}')
    #
    #     # 在策略得活动订单中，移除
    #     self.active_orders.pop(order.vt_orderid, None)

    # def on_order_open_canceled(self, order: OrderData):
    #     """开仓委托单撤单/部分成交/拒单"""
    #     self.write_log(f'委托单{order.status.value}')
    #
    #     order_info = self.active_orders.get(order.vt_orderid)
    #     grid = order_info.get('grid', None)
    #     if grid:
    #         # 移除grid的委托单中order_id
    #         if order.vt_orderid in grid.order_ids:
    #             self.write_log(f'网格移除开仓委托单号{order.vt_orderid}')
    #             grid.order_ids.remove(order.vt_orderid)
    #
    #         # 网格的所有委托单已经执行完毕
    #         if len(grid.order_ids) == 0:
    #             grid.order_status = False
    #         else:
    #             self.write_log(f'网格剩余开仓委托单号:{grid.order_ids}')
    #
    #         # 撤单得部分成交
    #         if order.traded > 0:
    #             self.write_log(f'网格{grid.direction.value}单，'
    #                            + f'计划开仓{grid.volume}'
    #                            + f'已开仓:{grid.traded_volume} =》{grid.traded_volume + order.traded}')
    #             grid.traded_volume += order.traded
    #
    #         if len(grid.order_ids) == 0 and grid.order_status is False and grid.traded_volume == 0:
    #             self.gt.remove_grids_by_ids(direction=grid.direction, ids=[grid.id])
    #
    #     # 在策略得活动订单中，移除
    #     self.active_orders.pop(order.vt_orderid, None)

    # def on_order_close_canceled(self, order: OrderData):
    #     """"平委托单撤单/部分成交/拒单"""
    #     self.write_log(f'委托单{order.status.value}')
    #
    #     order_info = self.active_orders.get(order.vt_orderid)
    #     grid = order_info.get('grid', None)
    #     if grid:
    #         # 移除grid的委托单中order_id
    #         if order.vt_orderid in grid.order_ids:
    #             self.write_log(f'网格移除平仓委托单号{order.vt_orderid}')
    #             grid.order_ids.remove(order.vt_orderid)
    #
    #         # 网格的所有委托单已经执行完毕
    #         if len(grid.order_ids) == 0:
    #             grid.order_status = False
    #         else:
    #             self.write_log(f'网格剩余平仓委托单号:{grid.order_ids}')
    #
    #         # 撤单得部分成交
    #         if order.traded > 0:
    #             self.write_log(f'网格{grid.direction.value}单，'
    #                            + f'计划平仓{grid.volume}'
    #                            + f'已平仓:{grid.traded_volume} =》{grid.traded_volume + order.traded}')
    #             grid.traded_volume += order.traded
    #
    #     # 在策略得活动订单中，移除
    #     self.active_orders.pop(order.vt_orderid, None)

    # ----------------------------------------------------------------------
    # def on_stop_order(self, stop_order: StopOrder):
    #     """停止单更新"""
    #     self.write_log(u'{},停止单触发，{}'.format(self.cur_datetime, stop_order.__dict__))
    #     pass

    # ----------------------------------------------------------------------
    def on_tick(self, tick: TickData):
        """行情更新（实盘运行，从tick导入）
        :type tick: object
        """
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not (self.inited):
            return

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
        # self.write_log(f'{tick.__dict__}')
        # 推送Tick到kline_x
        self.kline_x.on_tick(tick)

        # self.tns_update_price()

        # 执行撤单逻辑
        self.tns_cancel_logic(dt=self.cur_datetime)

        # 实盘这里是每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute



            # self.display_grids()
            # self.display_tns()
        # if self.position.pos != 0:
        #     self.tns_check_stop()
        #     self.tns_add_logic()
        # else:
        #     self.tns_open_logic()

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


        if len(self.kline_x.line_ma3) < 2:
            return

        if self.pos ==0 and self.entrust==0:
            #开多
            if self.kline_x.line_ma3[-1] > self.kline_x.line_ma3[-2] \
                and self.kline_x.line_ma1[-1] >self.kline_x.line_ma2[-1]:

                self.write_log(u'永续{}开多{}手 价格：{}'.format(self.vt_symbol,self.volume,bar.close_price - self.price_tick))
                order_ids = self.buy(price=bar.close_price + self.price_tick,
                                       volume=self.volume,
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}开多失败'.format(self.vt_symbol))


            #开空
            if self.kline_x.line_ma3[-1] < self.kline_x.line_ma3[-2] \
                    and self.kline_x.line_ma1[-1] < self.kline_x.line_ma2[-1]:

                self.write_log(u'永续{}开空{}手 价格：{}'.format(self.vt_symbol,self.volume,bar.close_price - self.price_tick))
                order_ids = self.short(price=bar.close_price - self.price_tick,
                                     volume=self.volume,
                                     vt_symbol=self.vt_symbol,
                                     order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}开空失败'.format(self.vt_symbol))

        else:
            #平多
            if self.pos>0 \
                    and self.kline_x.line_ma1[-1]<self.kline_x.line_ma2[-1] \
                    and self.entrust==0:
                self.write_log(
                    u'永续{}平多{}手 价格：{}'.format(self.vt_symbol, self.volume, bar.close_price - self.price_tick))
                order_ids = self.sell(price=bar.close_price - self.price_tick,
                                       volume=abs(self.pos),
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}平多失败'.format(self.vt_symbol))

            #平空
            if self.pos<0 \
                    and self.kline_x.line_ma1[-1]>self.kline_x.line_ma2[-1] \
                    and self.entrust==0:
                self.write_log(
                    u'永续{}平空{}手 价格：{}'.format(self.vt_symbol, self.volume, bar.close_price + self.price_tick))
                order_ids = self.cover(price=bar.close_price + self.price_tick,
                                       volume=abs(self.pos),
                                       vt_symbol=self.vt_symbol,
                                       order_time=self.cur_datetime)
                if len(order_ids) == 0:
                    self.write_error(u'永续{}平空失败'.format(self.vt_symbol))


    def tns_get_volume(self, stop_price: float = 0, invest_percent: float = None):
        """获取事务开仓volume
        :param stop_price:存在止损价时,按照最大亏损比例,计算可开仓手数
        :param invest_percent: 当次投资资金比例
        """

        if stop_price == 0 and invest_percent is None:
            return self.single_invest_pos

        volume = 0

        # 从策略引擎获取当前净值，可用资金，当前保证金比例，账号使用资金上限
        balance, avaliable, percent, percent_limit = self.cta_engine.get_account()

        if invest_percent is None:
            invest_percent = self.max_invest_percent

        if invest_percent > self.max_invest_percent:
            invest_percent = self.max_invest_percent

        # 计算当前策略实例，可使用的资金
        invest_money = float(balance * invest_percent / 100)
        invest_money = min(invest_money, avaliable)

        self.write_log(u'账号净值:{},可用:{},仓位:{},上限:{}%,策略投入仓位:{}%'
                       .format(balance, avaliable, percent, percent_limit, invest_percent))

        symbol_size = self.cta_engine.get_size(self.vt_symbol)
        symbol_margin_rate = self.cta_engine.get_margin_rate(self.vt_symbol)
        self.write_log(f'symbol_size:{symbol_size},symbol_margin_rate:{symbol_size}')
        self.send_wechat(f'symbol_size:{symbol_size},symbol_margin_rate:{symbol_size},当前价格:{self.cur_price},当前时间{self.cur_datetime},当前Tick:{self.cur_tick},invest_money:{invest_money}')
        # 投资资金总额允许的开仓数量
        max_unit = round(float(invest_money / (self.cur_price * symbol_margin_rate)), 3)
        self.write_log(u'投资资金总额{}允许的开仓数量：{},当前已经开仓手数:{}'
                       .format(invest_money, max_unit,
                               self.position.long_pos + abs(self.position.short_pos)))
        volume = max_unit

        if stop_price > 0 and stop_price != self.cur_price:
            eval_lost_money = balance * self.single_lost_percent / 100
            eval_lost_per_volume = abs(self.cur_price - stop_price) * symbol_size
            eval_lost_volume = round(float(eval_lost_money / eval_lost_per_volume), 2)
            new_volume = min(volume, eval_lost_volume)
            if volume != new_volume:
                self.write_log(
                    u'止损 {}% 限制金额:{},最多可使用{}手合约'.format(self.single_lost_percent, eval_lost_money, new_volume))
                volume = new_volume

        return volume





    def tns_add_logic(self):
        """
        加仓逻辑
        # 海龟加仓
        """

        if not self.policy.allow_add_pos:
            return

        if self.entrust != 0 or not self.trading:
            return

        # 加仓策略使用特定pip间隔（例如海龟的N）
        # 根据 ATR更新N
        self.policy.add_pos_on_pips = int(self.atr_value / (2 * self.price_tick))

        # 加多仓
        if self.position.long_pos > 0:
            # 还有允许加多单的额度,价格超过指最后的加仓价格+加仓价格幅度
            if self.policy.add_pos_count_above_first_price > 0 and \
                    self.cur_price >= (self.policy.last_open_price + self.policy.add_pos_on_pips * self.price_tick):

                # 这里可以根据风险，来评估你加仓数量，到达止损后，亏损多少
                # 设置新开仓价-2ATR为止损价
                new_stop_price = max(self.policy.tns_stop_price, self.policy.last_open_price - 2 * self.atr_value)

                if self.tns_buy():
                    # 更新开仓价格
                    self.policy.last_open_price = self.cur_price
                    self.policy.add_pos_count_above_first_price -= 1

                    self.write_log(u'更新止损价:{}->{}'.format(self.policy.tns_stop_price, new_stop_price))
                    self.policy.tns_stop_price = new_stop_price
                    self.policy.save()
                    self.display_tns()
                return

            # 还有允许逆势加多单的额度,价格低于过指最后的逆势加仓价格- 加仓价格幅度，并且不低于止损价
            if self.policy.add_pos_count_under_first_price > 0 \
                    and self.cur_price <= (
                    self.policy.last_under_open_price - self.policy.add_pos_on_pips * self.price_tick) \
                    and self.cur_price > self.policy.tns_stop_price:

                if self.tns_buy():
                    # 更新开仓价格
                    self.policy.last_under_open_price = self.cur_price
                    self.policy.add_pos_count_under_first_price -= 1
                    self.policy.save()
                    self.display_tns()
                return

        if self.position.short_pos < 0:
            # 还有允许加空单的额度,价格低于指最后的加仓价格 - 加仓价格幅度
            #
            if self.policy.add_pos_count_above_first_price and \
                    self.cur_price <= (self.policy.last_open_price - self.policy.add_pos_on_pips * self.price_tick):
                # 设置新开仓价-2ATR为止损价
                new_stop_price = max(self.policy.tns_stop_price, self.policy.last_open_price + 2 * self.atr_value)

                if self.tns_short():
                    # 更新开仓价格
                    self.policy.last_open_price = self.cur_price
                    self.write_log(u'更新止损价:{}->{}'.format(self.policy.tns_stop_price, new_stop_price))
                    self.policy.tns_stop_price = new_stop_price
                    self.policy.save()
                    self.display_tns()
                return

            # 还有允许逆势加空单的额度,价格高于过指最后的逆势加仓价格 + 加仓价格幅度，并且不低于止损价
            if self.policy.add_pos_count_under_first_price > 0 \
                    and self.cur_price >= (
                    self.policy.last_under_open_price + self.policy.add_pos_on_pips * self.price_tick) \
                    and self.cur_price < self.policy.tns_stop_price:

                if self.tns_short():
                    # 更新开仓价格
                    self.policy.last_under_open_price = self.cur_price
                    self.policy.add_pos_count_under_first_price -= 1
                    self.policy.save()
                    self.display_tns()
                return

    def tns_check_stop(self):
        """检查持仓止损或"""

        if self.entrust != 0 or not self.trading:
            return

        if self.position.long_pos == 0 and self.position.short_pos == 0:
            return

        if self.position.long_pos > 0:
            # MA10下穿MA20，Ma20拐头，多单离场
            if self.kline_x.ma12_count < 0 and self.kline_x.line_ma2[-1] < self.kline_x.line_ma2[-2]:
                self.write_log(u'{},平仓{}多单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.pos),
                                       self.cur_price))
                self.tns_sell()
                return

            # 转空事务
            if self.policy.tns_direction != Direction.LONG:
                self.write_log(u'{},事务与持仓不一致，平仓{}多单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.pos),
                                       self.cur_price))
                self.tns_sell()
                return

            # policy 跟随止损
            follow_stop_price = self.policy.tns_high_price - self.policy.tns_rtn_pips * self.price_tick
            if self.policy.tns_rtn_pips > 0 \
                    and self.cur_price < follow_stop_price <= self.policy.tns_stop_price:
                self.write_log(u'{},跟随止损，平仓{}多单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.long_pos),
                                       self.cur_price))
                self.tns_sell()
                return

            # 固定止损
            if self.policy.tns_stop_price > self.cur_price:
                self.write_log(u'{},固定止损，平仓{}多单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.long_pos),
                                       self.cur_price))
                self.tns_sell()
                return

        if abs(self.position.short_pos) > 0:
            # MA10上穿MA20，MA20拐头，空单离场
            if self.kline_x.ma12_count > 0 and self.kline_x.line_ma2[-1] > self.kline_x.line_ma2[-2]:
                self.write_log(u'{},平仓{}空单{}手,价格:{}'
                    .format(
                    self.cur_datetime,
                    self.vt_symbol,
                    abs(self.position.short_pos),
                    self.cur_price))
                self.tns_cover()
                return

            # 转多事务
            if self.policy.tns_direction != Direction.SHORT:
                self.write_log(u'{},事务与持仓不一致，平仓{}空单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.short_pos),
                                       self.cur_price))
                self.tns_cover()
                return

            # 跟随止损
            follow_stop_price = self.policy.tns_low_price + self.policy.tns_rtn_pips * self.price_tick
            if self.policy.tns_rtn_pips > 0 \
                    and self.cur_price > follow_stop_price > self.policy.tns_stop_price:
                self.write_log(u'{},跟随止损，平仓{}空单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.pos),
                                       self.cur_price))
                self.tns_cover()

                return

            # 固定止损
            if self.cur_price > self.policy.tns_stop_price > 0:
                self.write_log(u'{},固定止损，平仓{}空单{}手,价格:{}'
                               .format(self.cur_datetime,
                                       self.vt_symbol,
                                       abs(self.position.pos),
                                       self.cur_price))
                self.tns_cover()
                return

    def tns_buy(self):
        """事务开多"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        # 计算开仓数量
        total_open_count = self.add_pos_under_price_count + self.add_pos_above_price_count + 1
        first_open_volume = self.tns_get_volume(stop_price=self.kline_x.line_pre_low[-1],
                                                invest_percent=self.max_invest_percent / (total_open_count)) # stop_price

        self.write_log(u'{},开仓多单{}手,价格:{}'
                       .format(self.cur_datetime, first_open_volume, self.cur_price))

        # 创建一个持仓网格， 价格数据以主力合约为准
        grid = CtaGrid(
            direction=Direction.LONG,
            vt_symbol=self.vt_symbol,
            open_price=self.cur_price,
            stop_price=self.kline_x.line_pre_low[-1],
            close_price=self.cur_price * 2,
            volume=first_open_volume,
            # open_status=True
        )

        # 发出委托
        order_ids = self.buy(price=self.cur_price + self.price_tick,
                             volume=first_open_volume,
                             order_time=self.cur_datetime,
                             vt_symbol=self.vt_symbol,
                             grid=grid)
        if len(order_ids) > 0:
            # 委托成功后，添加至做多队列
            self.gt.dn_grids.append(grid)
            self.gt.save()
            return True

        return False

    def tns_sell(self):
        """事务平多仓"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        for grid in self.gt.get_opened_grids(direction=Direction.LONG):
            # 检查1，检查是否为已委托状态
            if grid.order_status:
                continue

            sell_symbol = grid.vt_symbol or self.vt_symbol
            sell_price = self.cta_engine.get_price(sell_symbol) - self.price_tick
            sell_volume = grid.volume - grid.traded_volume

            # 修正持仓
            if sell_volume != grid.volume:
                self.write_log(f'网格多单持仓:{grid.volume},已成交:{grid.traded_volume}, 修正为:{sell_volume}')
                grid.volume = sell_volume
                grid.traded_volume = 0

            # 进一步检查
            if grid.volume == 0:
                grid.open_status = False
                continue

            order_ids = self.sell(price=sell_price - self.price_tick,
                                  volume=grid.volume,
                                  vt_symbol=sell_symbol,
                                  order_time=self.cur_datetime, grid=grid)
            if len(order_ids) == 0:
                self.write_error(f'sell失败:{grid.__dict__}')

        return True

    def tns_short(self):
        """事务开空"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        # 计算开仓数量( 总次数：逆势加仓数+ 顺势加仓数 + 首笔）
        total_open_count = self.add_pos_under_price_count + self.add_pos_above_price_count + 1
        first_open_volume = self.tns_get_volume(stop_price=self.kline_x.line_pre_high[-1],
                                                invest_percent=self.max_invest_percent / (total_open_count))

        self.write_log(u'{},开仓空单{}手,价格:{}'
                       .format(self.cur_datetime, first_open_volume, self.cur_price))

        # 创建一个持仓网格， 价格数据以主力合约为准
        grid = CtaGrid(
            direction=Direction.SHORT,
            vt_symbol=self.vt_symbol,
            open_price=self.cur_price,
            stop_price=self.kline_x.line_pre_high[-1],
            close_price=0,
            volume=first_open_volume,
            # open_status=True
        )

        # 发出委托
        order_ids = self.short(price=self.cur_price - self.price_tick,
                               volume=first_open_volume,
                               order_time=self.cur_datetime,
                               vt_symbol=self.vt_symbol,
                               grid=grid)
        if len(order_ids) > 0:
            # 委托成功后，添加至做多队列
            self.gt.up_grids.append(grid)
            self.gt.save()
            return True

        return False

    def tns_cover(self):
        """事务平空仓"""
        if not self.inited or not self.trading:
            return False

        if self.entrust != 0:
            return False

        for grid in self.gt.get_opened_grids(direction=Direction.SHORT):
            # 检查1，检查是否为已委托状态
            if grid.order_status:
                continue

            cover_symbol = grid.vt_symbol or self.vt_symbol
            cover_price = self.cta_engine.get_price(cover_symbol) + self.price_tick
            cover_volume = grid.volume - grid.traded_volume

            # 修正持仓
            if cover_volume != grid.volume:
                self.write_log(f'网格空单持仓:{grid.volume},已成交:{grid.traded_volume}, 修正为:{cover_volume}')
                grid.volume = cover_volume
                grid.traded_volume = 0

            # 进一步检查
            if grid.volume == 0:
                grid.open_status = False
                continue

            order_ids = self.cover(price=cover_price + self.price_tick,
                                   volume=grid.volume,
                                   vt_symbol=cover_symbol,
                                   order_time=self.cur_datetime, grid=grid)
            if len(order_ids) == 0:
                self.write_error(f'cover失败:{grid.__dict__}')

        return True
