# encoding: UTF-8

from __future__ import division
from collections import OrderedDict

import os
import traceback
import copy
from datetime import datetime

from vnpy.trader.constant import (Direction, Offset, Status, Exchange, OrderType)
from vnpy.trader.object import TradeData, OrderData, TickData

from vnpy.event import Event
from vnpy.trader.event import (EVENT_ORDER, EVENT_TRADE)
from vnpy.trader.utility import get_underlying_symbol, get_folder_path, append_data, extract_vt_symbol
from vnpy.app.algo_trading.template import AlgoTemplate


########################################################################
class SpreadAlgoV2(AlgoTemplate):
    """
    价差价比交易算法，用于品种对的交易，支持场景：
     1、Cta_strategy_pro=》发送自定义合约的下单指令=》main_engine=>algo_engine=>SpreadAlgo分别委托下单=》成交/委托回报 =》 Cta_strategy_pro策略
     2、vn_trade界面，直接下自定义合约的下单指令=》》main_engine=>algo_engine=>SpreadAlgo分别委托下单=》成交/委托回报 =》 界面
    # 增加下单前判断是否涨停和跌停。
    # 采用FAK方式下单 或普通限价单方式下单(5秒撤单）
    #
    """

    display_name = u'价差价比交易算法'

    # ----------------------------------------------------------------------
    def __init__(self, algo_engine, algo_name, setting):
        """Constructor"""
        super().__init__(algo_engine, algo_name, setting)

        self.write_log(u'配置参数:\n{}'.format(setting))
        # 配置参数
        self.strategy_name = str(setting['strategy_name'])  # 来自调用的策略实例的名称
        self.spd_vt_symbol = str(setting['order_vt_symbol'])  # 价差/价比合约的名称 j1905-1-rb1905-5-BJ.SPD
        self.spd_symbol, self.exchange = extract_vt_symbol(self.spd_vt_symbol)
        self.gateway_name = setting.get('gateway_name', "")
        self.spd_direction = setting.get('order_direction')  # 交易方向
        self.spd_offset = setting.get('order_offset')  # 开平
        self.spd_req_price = float(setting['order_price'])  # 委托价差
        self.spd_req_volume = float(setting['order_volume'])  # 委托数量
        self.timer_interval = int(setting['timer_interval'])  # 检查成交的时间间隔（秒)
        self.spd_status = Status.SUBMITTING
        self.order_type = setting.get('order_type', OrderType.FAK)
        self.cancel_seconds = setting.get('cancel_seconds', 5)  # 使用限价单时，撤单时间
        # 初始化
        self.act_traded_volume = 0  # 主动腿的成交数量
        self.pas_traded_volume = 0  # 被动腿的成交数量
        self.act_traded_avg_price = 0  # 主动腿的成交均价
        self.pas_traded_avg_price = 0  # 被动腿的成交均价
        self.act_order_ids = []  # 主动委托号，一个或多个
        self.pas_order_ids = []  # 被动委托号
        self.act_order_dt = None # 主动委托单得委托时间
        self.pas_order_dt = None # 被动委托单得委托时间
        self.act_order_avg_price = 0  # 主动腿的报单均价
        self.pas_order_avg_price = 0  # 被动腿的报单均价
        self.act_order_volume = 0  # 主动腿的报单数量
        self.pas_order_volume = 0  # 被动腿的报单数量

        self.count = 0  # 运行计数
        self.entrust = False  # 是否满足价差/价比交易条件
        self.stopable = True  # 可停止交易

        self.spd_tick = None  # 最新的价差/价比价格
        self.act_tick = None  # 最新的主动腿价格
        self.pas_tick = None  # 最新的被动腿价格

        self.act_vt_symbol = self.get_vt_symbol(setting.get('leg1_symbol'))  # 主动腿的真实合约
        self.act_ratio = int(setting.get('leg1_ratio', 1))  # 主动腿/leg1的下单比例
        self.pas_vt_symbol = self.get_vt_symbol(setting.get('leg2_symbol'))  # 被动腿真实合约
        self.pas_ratio = int(setting.get('leg2_ratio', 1))  # 被动腿/leg2的下单比例
        self.is_spread = setting.get('is_spread', False)  # 价差交易
        self.is_ratio = setting.get('is_ratio', False)  # 价比交易

        # 需要交易的数量
        self.act_target_volume = self.spd_req_volume * self.act_ratio
        self.pas_target_volume = self.spd_req_volume * self.pas_ratio

        # 订阅合约
        self.subscribe(self.act_vt_symbol)
        self.subscribe(self.pas_vt_symbol)
        self.subscribe(self.spd_vt_symbol)

        # 获取合约基本信息
        self.act_contract = self.get_contract(self.act_vt_symbol)
        self.pas_contract = self.get_contract(self.pas_vt_symbol)

        self.act_exchange = self.act_contract.exchange
        self.pas_exchange = self.pas_contract.exchange

        # 合约的持仓信息
        self.act_pos = None
        self.pas_pos = None

        self.put_parameters_event()
        self.put_variables_event()
        self.write_log(f'{self.algo_name}[初始化]')

        # 委托下单得费用满足
        self.invest_money_enough = False

    def get_vt_symbol(self, symbol):
        """获取vt_symbol"""
        if '.' in symbol:
            return symbol

        contract = self.get_contract(symbol)

        if contract:
            return contract.vt_symbol
        else:
            self.write_error(f'获取不到{symbol}合约信息')
            return symbol

    def on_tick(self, tick: TickData):
        """tick行情事件"""

        if not self.active:
            return

        # 更新三个合约的tick
        if tick.vt_symbol == self.spd_vt_symbol:
            self.spd_tick = tick
        elif tick.vt_symbol == self.act_vt_symbol:
            self.act_tick = tick
        elif tick.vt_symbol == self.pas_vt_symbol:
            self.pas_tick = tick

        # Tick 未更新完毕
        if self.spd_tick is None or self.pas_tick is None or self.act_tick is None:
            return

        # 检查1：资金是否满足开仓
        if not self.invest_money_enough:
            if self.spd_offset == Offset.OPEN:
                if self.check_invest_money():
                    self.invest_money_enough = True
                else:
                    self.write_log(f'保证金不满足开仓')
                    self.reject_order()
                    self.stop()
                    return
            else:
                self.invest_money_enough = True

        # 检查2：主动腿合约/被动腿合约，是否接近涨停/跌停价
        if self.check_price_limit(direction = self.spd_direction):
            return

        # 委托单状态 Submitting => Not traded
        if self.spd_status == Status.SUBMITTING:
            algo_order = copy.copy(self.algo_engine.get_spd_order(self.algo_name))
            if algo_order:
                algo_order.status = Status.NOTTRADED
                # 通用事件
                event1 = Event(type=EVENT_ORDER, data=algo_order)
                self.algo_engine.event_engine.put(event1)
                self.spd_status = Status.NOTTRADED

        # 如果未满足价差/价格交易条件, 进行比对
        if not self.entrust:
            if self.spd_offset in [Offset.CLOSE, Offset.CLOSETODAY, Offset.CLOSEYESTERDAY]:
                if not self.check_pos():
                    self.reject_order()
                    self.stop()
                    return

            if self.spd_direction in [Direction.LONG] and self.spd_tick.ask_price_1 <= self.spd_req_price and self.spd_tick.ask_volume_1 >= 2 * max(self.act_target_volume, self.pas_target_volume):
                self.entrust = True
                self.write_log(f'{self.spd_vt_symbol} 卖出价{self.spd_tick.ask_price_1}低于委托价{self.spd_req_price}，'
                               f'可买数量{self.spd_tick.ask_volume_1} 大于max({self.act_target_volume} ,{self.pas_target_volume}), 满足交易条件')
            elif self.spd_direction in [Direction.SHORT] and self.spd_tick.bid_price_1 >= self.spd_req_price and self.spd_tick.bid_volume_1 >= 2 * max(self.act_target_volume, self.pas_target_volume):
                self.entrust = True
                self.write_log(f'{self.spd_vt_symbol} 买入价{self.spd_tick.bid_price_1}高于委托价{self.spd_req_price}，'
                               f'可卖数量{self.spd_tick.bid_price_1} 大于max({self.act_target_volume} ,{self.pas_target_volume}), 满足交易条件')
            else:
                # 价位未满足
                return

        self.stopable = False

        #检查超时
        if self.order_type == OrderType.LIMIT:
            # 超时5秒，撤单
            if len(self.act_order_ids) > 0\
                    and self.act_order_dt \
                    and (self.act_tick.datetime - self.act_order_dt).total_seconds() > self.cancel_seconds:

                for order_id in self.act_order_ids:
                    # 撤单
                    self.write_log(f'主动腿{self.act_vt_symbol}超时撤单{order_id}')
                    self.cancel_order(order_id)

            # 超时5秒，撤单
            if len(self.pas_order_ids) > 0 \
                    and self.pas_order_dt \
                    and (self.pas_tick.datetime - self.pas_order_dt).total_seconds() > self.cancel_seconds:

                for order_id in self.pas_order_ids:
                    # 撤单
                    self.write_log(f'被动腿{self.pas_vt_symbol}超时撤单{order_id}')
                    self.cancel_order(order_id)

        # 发出委托单
        if self.act_target_volume > self.act_traded_volume and len(self.act_order_ids) == 0:
            # 主动腿发出委托单
            volume = self.act_target_volume - self.act_traded_volume
            # spd [long + open] => act : [long + open], spd [ long + close] => act : [ long + close]
            if self.spd_direction == Direction.LONG:
                self.write_log(f'[主动腿委托] {self.act_vt_symbol}, 方向:{Direction.LONG.value}, '
                               f'{self.spd_offset.value}, 价格:{self.act_tick.ask_price_1}, '
                               f'委托数量:{volume},'
                               f'目标数量:{self.act_target_volume}')
                # 以对价方式buy或cover
                order_ids = self.buy(
                    vt_symbol=self.act_vt_symbol,
                    price=self.act_tick.ask_price_1,
                    volume=volume,
                    offset=self.spd_offset,
                    order_type=self.order_type,
                    lock=self.act_exchange==Exchange.CFFEX)
                if len(order_ids) > 0:
                    self.act_order_dt = datetime.now()
                    self.act_order_ids.extend(order_ids)
                    self.write_log(f'[主动腿委托:{self.act_order_ids}')
                    self.act_order_avg_price = (self.act_traded_volume * self.act_order_avg_price + volume * self.act_tick.ask_price_1) / self.act_target_volume
                    self.act_order_volume = self.act_target_volume

            # spd [short + open] => act : [short + open], spd [ short + close] => act : [ short + close]
            elif self.spd_direction == Direction.SHORT:
                self.write_log(f'[主动腿委托] {self.act_vt_symbol}, 方向:{Direction.SHORT.value}, '
                               f'{self.spd_offset.value}, 价格:{self.act_tick.bid_price_1}, '
                               f'委托数量:{volume},'
                               f'目标数量:{self.act_target_volume}')
                # 以对价方式sell或short， FAK委托
                order_ids = self.sell(
                    vt_symbol=self.act_vt_symbol,
                    price=self.act_tick.bid_price_1,
                    volume=volume,
                    offset=self.spd_offset,
                    order_type=self.order_type,
                    lock=self.act_exchange==Exchange.CFFEX)
                if len(order_ids) > 0:
                    self.act_order_dt = datetime.now()
                    self.act_order_ids.extend(order_ids)
                    self.write_log(f'[主动腿委托]=>{self.act_order_ids}')
                    self.act_order_avg_price = (
                                                           self.act_traded_volume * self.act_order_avg_price + volume * self.act_tick.ask_price_1) / self.act_target_volume
                    self.act_order_volume = self.act_target_volume

        if self.pas_target_volume > self.pas_traded_volume and len(self.pas_order_ids) == 0:
            # 被动腿发出委托单
            volume = self.pas_target_volume - self.pas_traded_volume
            # spd [long + open] => pas : [short + open], spd [ long + close] => pass : [ short+ close]
            if self.spd_direction == Direction.LONG:
                # 以对价方式sell或short， FAK委托
                order_ids = self.sell(
                    vt_symbol=self.pas_vt_symbol,
                    price=self.pas_tick.bid_price_1,
                    volume=volume,
                    offset=self.spd_offset,
                    order_type=self.order_type,
                    lock=self.pas_exchange==Exchange.CFFEX
                )
                if len(order_ids) > 0:
                    self.pas_order_dt = datetime.now()
                    self.pas_order_ids.extend(order_ids)
                    self.write_log(f'[被动腿委托]=>{self.pas_order_ids}')
                    self.pas_order_avg_price = (
                                                           self.pas_traded_volume * self.pas_order_avg_price + volume * self.pas_tick.bid_price_1) / self.pas_target_volume
                    self.pas_order_volume = self.pas_target_volume

            elif self.spd_direction == Direction.SHORT:
                # 以对价方式，buy/cover， FAK委托
                order_ids = self.buy(
                    vt_symbol=self.pas_vt_symbol,
                    price=self.pas_tick.ask_price_1,
                    volume=volume,
                    offset=self.spd_offset,
                    order_type=self.order_type,
                    lock=self.pas_exchange==Exchange.CFFEX)
                if len(order_ids) > 0:
                    self.pas_order_dt = datetime.now()
                    self.pas_order_ids.extend(order_ids)
                    self.write_log(f'[被动腿委托]=>{self.pas_order_ids}')
                    self.pas_order_avg_price = (self.pas_traded_volume * self.pas_order_avg_price + volume * self.pas_tick.bid_price_1) / self.pas_target_volume
                    self.pas_order_volume = self.pas_target_volume

    def check_invest_money(self):
        """
        检查投资金额是否满足
        :return:
        """
        # 当前净值,可用资金,资金占用比例,资金上限
        balance, avaliable, occupy_percent, percent_limit = self.algo_engine.get_account()

        if occupy_percent >= percent_limit:
            self.write_log(u'当前资金占用:{},超过限定:{}'.format(occupy_percent, percent_limit))
            return False

        # 主动腿/被动腿得短合约符号
        act_short_symbol = get_underlying_symbol(self.act_vt_symbol)
        passive_short_symbol = get_underlying_symbol(self.pas_vt_symbol)

        # 主动腿的合约size/保证金费率
        act_size = self.algo_engine.get_size(self.act_vt_symbol)
        act_margin_rate = self.algo_engine.get_margin_rate(self.act_vt_symbol)

        # 被动腿的合约size/保证金费率
        passive_size = self.algo_engine.get_size(self.pas_vt_symbol)
        passive_margin_rate = self.algo_engine.get_margin_rate(self.pas_vt_symbol)

        # 主动腿保证金/被动腿保证金
        act_margin = self.act_target_volume * self.act_tick.last_price * act_size * act_margin_rate
        passive_margin = self.pas_target_volume * self.pas_tick.last_price * passive_size * passive_margin_rate

        if act_short_symbol == passive_short_symbol:
            # 同一品种套利
            invest_margin = max(act_margin, passive_margin)
        else:
            # 跨品种套利
            invest_margin = act_margin + passive_margin

        # 计划使用保证金
        target_margin = balance * (occupy_percent / 100) + invest_margin

        if 100 * (target_margin / balance) > percent_limit:
            self.write_log(u'委托后,预计当前资金占用:{},超过限定:{}比例,不能开仓'
                           .format(100 * (target_margin / balance), percent_limit))
            return False

        return True

    def check_price_limit(self, direction):
        """检查2：主动腿合约/被动腿合约，是否接近涨停/跌停价"""
        if self.act_traded_volume == 0 and self.pas_traded_volume == 0:
            if self.act_contract is not None and self.pas_contract is not None:
                if 0 < self.act_tick.limit_up < self.act_tick.last_price + self.act_contract.pricetick * 10 and direction==Direction.LONG:
                    self.write_log(f'{self.act_vt_symbol}合约价格{self.act_tick.last_price} '
                                   f'接近涨停价{self.act_tick.limit_up} 10个跳,不处理')
                    self.stop()
                    return True

                if 0 < self.pas_tick.limit_up < self.pas_tick.last_price + self.pas_contract.pricetick * 10 and direction==Direction.SHORT:
                    self.write_log(f'{self.pas_vt_symbol}合约价格{self.pas_tick.last_price} '
                                   f'接近涨停价{self.pas_tick.limit_up} 10个跳,不处理')
                    self.stop()
                    return True

                if 0 < self.act_tick.last_price - self.act_contract.pricetick * 10 < self.act_tick.limit_down and direction==Direction.SHORT:
                    self.write_log(f'{self.act_vt_symbol}合约价格{self.act_tick.last_price} '
                                   f'接近跌停价{self.act_tick.limit_down} 10个跳,不处理')
                    self.stop()
                    return True

                if 0 < self.pas_tick.last_price + self.pas_contract.pricetick * 10 < self.pas_tick.limit_down and direction==Direction.LONG:
                    self.write_log(f'{self.pas_vt_symbol}合约价格{self.pas_tick.last_price} '
                                   f'接近跌停价{self.pas_tick.limit_down} 10个跳,不开仓')
                    self.stop()
                    return True
        return False

    def check_pos(self):
        """
        检查仓位是否满足平仓要求
        :return:
        """
        # spd cover
        if self.spd_direction == Direction.LONG:
            # 主动腿 cover
            self.act_pos = self.algo_engine.get_position(vt_symbol=self.act_vt_symbol, direction=Direction.SHORT)
            if not self.act_pos:
                self.write_error(f'[仓位检查] 找不到{self.act_vt_symbol}的{Direction.SHORT}单持仓')
                return False

            if self.act_pos.volume < self.act_target_volume:
                self.write_error(f'[仓位检查] {self.act_vt_symbol}的{Direction.SHORT}单持仓'
                                 f'{self.act_pos.volume} 不满足平仓要求{self.act_target_volume}')
                return False
            # 被动腿 sell
            self.pas_pos = self.algo_engine.get_position(vt_symbol=self.pas_vt_symbol, direction=Direction.LONG)
            if not self.pas_pos:
                self.write_error(f'[仓位检查] 找不到{self.pas_vt_symbol}的{Direction.LONG}单持仓')
                return False

            if self.pas_pos.volume < self.pas_target_volume:
                self.write_error(f'[仓位检查] {self.pas_vt_symbol}的{Direction.LONG}单持仓'
                                 f'{self.pas_pos.volume} 不满足平仓要求{self.pas_target_volume}')
                return False
        # spd sell
        elif self.spd_direction == Direction.SHORT:
            # 主动腿 sell
            self.act_pos = self.algo_engine.get_position(vt_symbol=self.act_vt_symbol, direction=Direction.LONG)
            if not self.act_pos:
                self.write_error(f'[仓位检查] 找不到{self.act_vt_symbol}的{Direction.LONG}单持仓')
                return False

            if self.act_pos.volume < self.act_target_volume:
                self.write_error(f'[仓位检查] {self.act_vt_symbol}的{Direction.LONG}单持仓'
                                 f'{self.act_pos.volume} 不满足平仓要求{self.act_target_volume}')
                return False
            # 被动腿 cover
            self.pas_pos = self.algo_engine.get_position(vt_symbol=self.pas_vt_symbol, direction=Direction.SHORT)
            if not self.pas_pos:
                self.write_error(f'[仓位检查] 找不到{self.pas_vt_symbol}的{Direction.SHORT}单持仓')
                return False

            if self.pas_pos.volume < self.pas_target_volume:
                self.write_error(f'[仓位检查] {self.pas_vt_symbol}的{Direction.SHORT}单持仓'
                                 f'{self.pas_pos.volume} 不满足平仓要求{self.pas_target_volume}')
                return False

        return True

    def cancel_algo(self):
        """
        撤销当前算法实例订单
        :return:
        """
        self.write_log(u'{}发出算法撤单，合约:{}'.format(self.algo_name, self.spd_vt_symbol))

        algo_order = copy.copy(self.algo_engine.get_spd_order(self.algo_name))
        algo_order.status = Status.CANCELLED
        # 通用事件
        event1 = Event(type=EVENT_ORDER, data=algo_order)
        self.algo_engine.event_engine.put(event1)

    def stop(self):
        """重载停止，如果存在单腿，则不能停止"""
        if self.stopable:
            return super().stop()
        else:
            if len(self.active_orders) > 0 or len(self.act_order_ids)>0 or len(self.pas_order_ids) > 0:
                self.write_log(f'收到停止请求，但存在未完成委托单')
                for order in self.active_orders.values():
                    self.write_log(f'{order.__dict__}')

            self.write_error(f'{self.algo_name}当前仍存在委托,不能撤单')
            return False


    def reject_order(self):
        """发出拒单"""
        algo_order = copy.copy(self.algo_engine.get_spd_order(self.algo_name))
        algo_order.status = Status.REJECTED
        # 通用事件
        event1 = Event(type=EVENT_ORDER, data=algo_order)
        self.algo_engine.event_engine.put(event1)

    def append_trade_record(self, trade):
        """
        添加交易记录到文件
        :param trade:
        :return:
        """
        trade_fields = ['datetime', 'symbol', 'exchange', 'vt_symbol', 'tradeid', 'vt_tradeid', 'orderid', 'vt_orderid',
                        'direction', 'offset', 'price', 'volume', 'idx_price']
        trade_dict = OrderedDict()
        try:
            for k in trade_fields:
                if k == 'datetime':
                    dt = getattr(trade, 'datetime')
                    if isinstance(dt, datetime):
                        trade_dict[k] = dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        trade_dict[k] = datetime.now().strftime('%Y-%m-%d') + ' ' + getattr(trade, 'time', '')
                if k in ['exchange', 'direction', 'offset']:
                    trade_dict[k] = getattr(trade, k).value
                else:
                    trade_dict[k] = getattr(trade, k, '')

            # 添加指数价格
            symbol = trade_dict.get('symbol')
            idx_symbol = get_underlying_symbol(symbol).upper() + '99.' + trade_dict.get('exchange')
            idx_price = self.algo_engine.get_price(self, idx_symbol)
            if idx_price:
                trade_dict.update({'idx_price': idx_price})
            else:
                trade_dict.update({'idx_price': trade_dict.get('price')})

            if self.strategy_name is not None and len(self.strategy_name) > 0:
                trade_file = str(get_folder_path('data').joinpath('{}_trade.csv'.format(self.strategy_name)))
                append_data(file_name=trade_file, dict_data=trade_dict)
        except Exception as ex:
            self.write_error(u'写入交易记录csv出错：{},{}'.format(str(ex), traceback.format_exc()))

    def on_trade(self, trade):
        """处理成交结果"""
        self.write_log('spreadAlgo.on_trade(), {}'.format(trade.__dict__))
        if trade.vt_symbol not in [self.act_vt_symbol, self.pas_vt_symbol]:
            self.write_log(u'不认识的交易：{},{}'.format(self.strategy_name, trade.vt_symbol))
            return

        # 主动腿成交
        if trade.vt_symbol == self.act_vt_symbol:
            self.act_traded_avg_price = trade.price * trade.volume + self.act_traded_avg_price * self.act_traded_volume
            self.act_traded_volume += trade.volume
            self.act_traded_avg_price /= self.act_traded_volume

        else:
            self.pas_traded_avg_price = trade.price * trade.volume + self.pas_traded_avg_price * self.pas_traded_volume
            self.pas_traded_volume += trade.volume
            self.pas_traded_avg_price /= self.pas_traded_volume

        self.append_trade_record(trade)

        # 主动腿&被动腿都成交, 合成套利合约的成交更新事件, 并将它推送给EventEngine
        if self.pas_traded_volume == self.pas_target_volume and self.act_traded_volume == self.act_target_volume:
            traded_price = 0
            if self.is_spread:
                traded_price = self.act_traded_avg_price \
                               - self.pas_traded_avg_price  # noqa
            elif self.is_ratio:
                traded_price = 100 * self.act_traded_avg_price * self.act_traded_volume \
                               / (self.pas_traded_avg_price * self.pas_traded_volume)  # noqa
            self.write_log(f'所有交易已完成：{self.strategy_name},{traded_price}')

            """套利合约的成交信息推送"""
            algo_trade = TradeData(
                gateway_name=self.gateway_name,
                symbol=self.spd_symbol,
                exchange=self.exchange,
                orderid=self.algo_name,
                price=traded_price,
                volume=self.spd_req_volume,
                tradeid=self.algo_name,
                sys_orderid=self.algo_name,
                direction=self.spd_direction,
                offset=self.spd_offset,
                time=trade.time,
                datetime=trade.datetime,
                strategy_name=self.strategy_name
            )

            self.write_log(f'发出spd成交更新event:{algo_trade.__dict__}')
            # 通用事件
            event1 = Event(type=EVENT_TRADE, data=algo_trade)
            self.algo_engine.event_engine.put(event1)

            # 套利合约的订单变化推送
            order_price = 0
            if self.is_spread:
                order_price = self.act_order_avg_price - self.pas_order_avg_price
            elif self.is_ratio:
                order_price = 100 * self.act_order_avg_price * self.act_order_volume \
                              / (self.pas_order_avg_price * self.pas_order_volume)  # noqa

            # 发送套利合约得onOrder事件
            algo_order = copy.copy(self.algo_engine.get_spd_order(self.algo_name))
            algo_order.price = order_price
            algo_order.traded = algo_order.volume
            algo_order.status = Status.ALLTRADED

            # 通用事件
            self.write_log(f'发出spd委托更新event:{algo_order.__dict__}')
            event2 = Event(type=EVENT_ORDER, data=algo_order)
            self.algo_engine.event_engine.put(event2)

            self.stopable = True
            self.stop()

        self.put_variables_event()

    # ----------------------------------------------------------------------
    def on_order(self, order):
        """处理报单结果"""
        self.write_log('{}.on_order(), {}'.format(self.algo_name, order.__dict__))
        if order.vt_symbol not in [self.act_vt_symbol, self.pas_vt_symbol]:
            self.write_log(u'[on_order]不认识的交易：{},{}'.format(self.strategy_name, order.vt_symbol))
            return

        if order.status in [Status.CANCELLED, Status.REJECTED, Status.ALLTRADED]:
            if order.vt_symbol == self.act_vt_symbol:
                if order.vt_orderid in self.act_order_ids:
                    self.write_log(f'主动腿委托单列表{self.act_order_ids} 移除{order.vt_orderid}')
                    self.act_order_ids.remove(order.vt_orderid)

            elif order.vt_symbol == self.pas_vt_symbol:
                if order.vt_orderid in self.pas_order_ids:
                    self.write_log(f'被动腿委托单列表{self.pas_order_ids} 移除{order.vt_orderid}')
                    self.pas_order_ids.remove(order.vt_orderid)

        self.put_variables_event()

    # ----------------------------------------------------------------------
    def on_timer(self):
        """定时检查, 未完成开仓，就撤单"""
        self.count += 1
        if self.count < self.timer_interval:
            return
        self.stop()

    # ----------------------------------------------------------------------
    def on_stop(self):
        """"""
        self.active = False
        self.write_log(u'算法停止')
        self.put_variables_event()

    # ----------------------------------------------------------------------
    def put_variables_event(self):
        """更新变量"""
        variables = {}
        for name in self.variables:
            variables[name] = getattr(self, name)
        variables[u'主动腿持仓'] = self.act_traded_volume
        variables[u'被动腿持仓'] = self.pas_traded_volume

        self.algo_engine.put_variables_event(self, variables)

    # ----------------------------------------------------------------------
    def put_parameters_event(self):
        """更新参数"""
        d = OrderedDict()
        d[u'价差合约'] = self.spd_vt_symbol
        d[u'交易命令'] = f'{self.spd_direction.value}.{self.spd_offset.value}'
        d[u'价差'] = self.spd_req_price
        d[u'数量'] = self.spd_req_volume
        d[u'间隔'] = self.timer_interval
        d[u'策略名称'] = self.strategy_name
        self.algo_engine.put_parameters_event(self, d)
