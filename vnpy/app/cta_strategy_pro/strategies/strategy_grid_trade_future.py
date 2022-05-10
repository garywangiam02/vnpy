# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
from datetime import datetime, timedelta, time, date
import copy
import traceback
from collections import OrderedDict
# 然后是自己编写的模块
from vnpy.trader.utility import round_to
from vnpy.app.cta_strategy_pro.template import CtaProFutureTemplate, Exchange, \
    Direction, get_underlying_symbol, Interval, \
    TradeData, Offset, Status, OrderType
from vnpy.component.cta_policy import (
    CtaPolicy, TNS_STATUS_OBSERVATE, TNS_STATUS_READY, TNS_STATUS_ORDERING, TNS_STATUS_OPENED, TNS_STATUS_CLOSED
)
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar


########################################################################
class StrategyGridTradeFuture(CtaProFutureTemplate):
    """期货网格交易策略
    # v1：移植股票网格=》期货，按照当前价，往下n%开始布网格
    # 当创新高，没有网格时，重新布
    # v2， 支持自定义套利对（包括负数价格）,支持双向网格
    #  单向网格做多配置方式:
    "Future_grid_LONG_pg": {
            "class_name": "StrategyGridTradeFuture",
            "vt_symbol": "pg2011.DCE",
            "auto_init": true,
            "auto_start": true,
            "setting": {
                "backtesting": false,
                "class_name": "StrategyGridTradeFuture",
                "idx_symbol": "pg2011.DCE",
                "max_invest_rate": 0.05,
                "grid_height_percent": 0.4,
                "max_invest_pos": 15,
                "grid_lots": 15，
                "active_long_grid":true,
                "active_short_grid":false
            }
        }
    # 交易所标准套利合约配置方式：
    "Future_grid_LONG_I0901": {
        "class_name": "StrategyGridTradeFuture",
        "vt_symbol": "SP i2009&i2101.DCE",
        "auto_init": true,
        "auto_start": true,
        "setting": {
            "backtesting": false,
            "idx_symbol": "SP i2009&i2101.DCE",
            "class_name": "StrategyGridTradeFuture",
            "max_invest_rate": 0.05,
            "grid_height_percent": 10,
            "grid_height_pips": 10,   每个格是10个跳，
            "max_invest_pos": 15,     最大投入15手，平均到每一个网格就是1手
            "grid_lots": 15,          一共做15格
            "active_long_grid":true,  这里只做正套
            "fixed_highest_price": 100 这里限制了价差最高只能100，高于100就不做正套
        }
    },
    """
    author = u'大佳'

    # 网格数量
    grid_lots: int = 15
    # 网格高度百分比（ = 价格 * 百分比）
    grid_height_percent: float = 2
    # 网格限定跳动数量( = 跳 * 最小跳动, 不指定时，使用百分比）
    grid_height_pips: int = 0

    # 限定价格(设置后，不会从合约最高价获取), 例如指定为2000，当价格高于2000时，仍然按照2000为基准
    fixed_highest_price: float = None
    fixed_lowest_price: float = None

    long_stop_price: float = None   # 做多/正套，止损价格
    short_stop_price: float = None  # 做空/反套，止损价格

    # 启动做多/正套网格(缺省只做多）
    active_long_grid: bool = True
    # 启动做空/反套网格
    active_short_grid: bool = False

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "grid_height_percent", "grid_height_pips",
        "grid_lots", "fixed_highest_price", "fixed_lowest_price",
        "long_stop_price", "short_stop_price",
        "active_long_grid", "active_short_grid",
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

        self.policy = Future_Grid_Trade_Policy(self)

        # 仓位状态
        self.position = CtaPosition(strategy=self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

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
        self.policy.load()
        self.display_tns()
        if not self.backtesting:
            self.init_position()  # 初始持仓数据

        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

    def init_position(self):
        """
        重载恢复持仓
        上层模板并没有处理close_status。
        :return:
        """
        super().init_position()

        for grid in self.gt.dn_grids:
            if grid.close_status and grid.open_status:
                grid.close_status = False
                self.write_log(f'{grid.vt_symbol} 多单 open:{grid.open_price}->close:{grid.close_price} 重置平仓状态为False.')

        for grid in self.gt.up_grids:
            if grid.close_status and grid.open_status:
                grid.close_status = False
                self.write_log(f'{grid.vt_symbol} 空单 open:{grid.open_price}->close:{grid.close_price} 重置平仓状态为False.')

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
        if '&' in tick.symbol:
            tick.last_price = (tick.ask_price_1 + tick.bid_price_1) / 2

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

        if not self.inited or not self.trading:
            return

        # 自定义品种对，在临收盘前可能会产生断腿
        if self.exchange == Exchange.SPD:
            if self.cur_datetime.hour in [14, 22, 23, 1] and self.cur_datetime.minute >= 58:
                return
            if self.cur_datetime.hour in [11, 2] and self.cur_datetime.minute >= 28:
                return

        # 规划网格逻辑，当价格发生变化时，重新规划或者添加网格
        self.update_grids(self.vt_symbol)

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime, reopen=False)

        # 网格逐一开仓/止盈检查
        self.tns_check_grids()

        # 实盘这里是每分钟执行,定时更新和显示而已。
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute

            self.display_grids()
            self.display_tns()
            self.put_event()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """
        这个不用了。
        :param bar:
        :return:
        """
        pass

    def on_trade(self, trade: TradeData):
        """交易更新(重构），支持普通合约，自定义合约，套利指令合约"""
        self.write_log(u'{},交易更新事件:{},当前持仓：{} '
                       .format(self.cur_datetime,
                               trade.__dict__,
                               self.position.pos))

        dist_record = dict()
        if self.backtesting:
            dist_record['datetime'] = trade.time
        else:
            dist_record['datetime'] = ' '.join([self.cur_datetime.strftime('%Y-%m-%d'), trade.time])
        dist_record['volume'] = trade.volume
        dist_record['price'] = trade.price
        dist_record['symbol'] = trade.vt_symbol

        if "&" in self.vt_symbol and ' ' in self.vt_symbol and trade.vt_symbol != self.vt_symbol:
            symbol = self.vt_symbol.split('.')[0]
            act_symbol, pas_symbol = symbol.split(' ')[-1].split('&')

            if trade.symbol == pas_symbol:
                return

            dist_record['symbol'] = self.vt_symbol

        if trade.direction == Direction.LONG and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'buy'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset == Offset.OPEN:
            dist_record['operation'] = 'short'
            self.position.open_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.LONG and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'cover'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        if trade.direction == Direction.SHORT and trade.offset != Offset.OPEN:
            dist_record['operation'] = 'sell'
            self.position.close_pos(trade.direction, volume=trade.volume)
            dist_record['long_pos'] = self.position.long_pos
            dist_record['short_pos'] = self.position.short_pos

        self.save_dist(dist_record)
        self.pos = self.position.pos

    def update_grids(self, vt_symbol):
        """
        更新网格
        1. 不存在policy时，初始化该vt_symbol的policy配置，包括当前最高价（取当前价格和pre_close价格)
        2. 做多网格为例：如果价格高于最高价，从最高价往下，构造10个网格的买入价格。
        3. 记录最后一次开仓价格。
        :return:
        """

        grid_info = self.policy.grids.get(vt_symbol, {})
        cur_price = self.cta_engine.get_price(vt_symbol)
        if cur_price is None or cur_price == 0:
            self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=vt_symbol)
            return

        if self.active_long_grid:
            # v2版本，支持指定最高价格:
            if self.fixed_highest_price is not None and cur_price > self.fixed_highest_price:
                cur_price = self.fixed_highest_price

        elif self.active_short_grid:
            if self.fixed_lowest_price is not None and cur_price < self.fixed_lowest_price:
                cur_price = self.fixed_lowest_price

        price_tick = self.cta_engine.get_price_tick(vt_symbol)
        if self.grid_height_pips > 0:
            grid_height = self.grid_height_pips * price_tick
        else:
            # 每个格，按照4%计算
            grid_height = max(round_to(abs(cur_price) * self.grid_height_percent / 100, price_tick), 2 * price_tick)

        policy_changed = False
        # 允许做多，价格高于最高价
        if self.active_long_grid and cur_price > grid_info.get('high_price', -sys.maxsize):
            grid_info.update({
                'update_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'high_price': cur_price,
                'grid_height': grid_height,
                'long_prices': [round_to(cur_price - (i + 1) * grid_height, price_tick) for i in range(self.grid_lots)]
            })
            self.policy.grids.update({vt_symbol: grid_info})
            policy_changed = True

        # 允许做空，价格低于最低价
        if self.active_short_grid and cur_price < grid_info.get('low_price', abs(cur_price) * 2):
            grid_info.update({
                'update_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'low_price': cur_price,
                'grid_height': grid_height,
                'short_prices': [round_to(cur_price + (i + 1) * grid_height, price_tick) for i in
                                 range(self.grid_lots)]
            })
            self.policy.grids.update({vt_symbol: grid_info})
            policy_changed = True

        if policy_changed:
            self.policy.save()

        if self.active_long_grid:
            if self.long_stop_price is None or self.long_stop_price < cur_price:
                # 找出能满足当前价的多单开仓价
                long_prices = [p for p in grid_info.get('long_prices', []) if p >= cur_price]
                if len(long_prices) > 0:
                    open_price = long_prices[-1]
                    grid_height = grid_info.get('grid_height', grid_height)
                    # 检查是否存在这个价格的网格
                    grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.open_price == open_price]
                    if len(grids) == 0:
                        self.write_log(f'计划添加做多网格:{vt_symbol}')
                        close_price = round_to(open_price + grid_height, price_tick)
                        self.tns_add_long_grid(
                            vt_symbol=vt_symbol,
                            open_price=open_price,
                            close_price=close_price)

        if self.active_short_grid:
            if self.short_stop_price is None or self.short_stop_price > cur_price:
                # 找出能满足当前价的空开仓价
                short_prices = [p for p in grid_info.get('short_prices', []) if p <= cur_price]
                if len(short_prices) > 0:
                    open_price = short_prices[-1]
                    grid_height = grid_info.get('grid_height', grid_height)
                    # 检查是否存在这个价格的网格
                    grids = [g for g in self.gt.up_grids if g.vt_symbol == vt_symbol and g.open_price == open_price]
                    if len(grids) == 0:
                        self.write_log(f'计划添加做空网格:{vt_symbol}')
                        close_price = round_to(open_price - grid_height, price_tick)
                        self.tns_add_short_grid(
                            vt_symbol=vt_symbol,
                            open_price=open_price,
                            close_price=close_price)

    def tns_add_long_grid(self, vt_symbol, open_price, close_price):
        """
        事务添加做多网格
        :param :
        :return:
        """
        if not self.trading:
            return False

        balance, avaliable, _, _ = self.cta_engine.get_account()

        invest_margin = balance * self.max_invest_rate
        if invest_margin > self.max_invest_margin > 0:
            invest_margin = self.max_invest_margin

        cur_price = self.cta_engine.get_price(vt_symbol)
        if '&' in vt_symbol:
            symbol, exchange = vt_symbol.split('.')
            symbol = symbol.split(' ')[-1]
            act_symbol = symbol.split('&')[0]
            margin_rate = self.cta_engine.get_margin_rate(f'{act_symbol}.{exchange}')
            symbol_size = self.cta_engine.get_size(f'{act_symbol}.{exchange}')
        else:
            margin_rate = self.cta_engine.get_margin_rate(vt_symbol)
            symbol_size = self.cta_engine.get_size(vt_symbol)

        if cur_price == 0:
            return False

        max_volume = invest_margin / (abs(cur_price) * margin_rate * symbol_size)
        if self.max_invest_pos > 0:
            max_volume = min(self.max_invest_pos, max_volume)
        target_volume = max(int(max_volume / self.grid_lots), 1)

        self.write_log(f'{vt_symbol} 策略最大投入:{invest_margin},总仓位:{max_volume},每格投入:{target_volume}')

        if self.position.long_pos + target_volume > max_volume:
            target_volume = 0

        if target_volume <= 0:
            self.write_log(f'{vt_symbol} 目标增仓为{target_volume}。不做买入')
            return True

        grid = CtaGrid(direction=Direction.LONG,
                       vt_symbol=vt_symbol,
                       open_price=open_price,
                       stop_price=0,
                       close_price=close_price,
                       volume=target_volume
                       )

        self.gt.dn_grids.append(grid)
        self.write_log(u'添加做多计划{},买入数量:{},买入价格:{}'.format(grid.type, grid.volume, grid.open_price))
        self.gt.save()

        dist_record = OrderedDict()
        dist_record['datetime'] = self.cur_datetime
        dist_record['symbol'] = vt_symbol
        dist_record['volume'] = grid.volume
        dist_record['price'] = self.cur_price
        dist_record['operation'] = 'entry long'

        self.save_dist(dist_record)

        return True

    def tns_add_short_grid(self, vt_symbol, open_price, close_price):
        """
        事务添加做空网格
        :param :
        :return:
        """
        if not self.trading:
            return False

        balance, avaliable, _, _ = self.cta_engine.get_account()

        invest_margin = balance * self.max_invest_rate
        if invest_margin > self.max_invest_margin > 0:
            invest_margin = self.max_invest_margin

        if '&' in vt_symbol:
            symbol, exchange = vt_symbol.split('.')
            symbol = symbol.split(' ')[-1]
            act_symbol = symbol.split('&')[0]
            margin_rate = self.cta_engine.get_margin_rate(f'{act_symbol}.{exchange}')
            symbol_size = self.cta_engine.get_size(f'{act_symbol}.{exchange}')
        else:
            margin_rate = self.cta_engine.get_margin_rate(vt_symbol)
            symbol_size = self.cta_engine.get_size(vt_symbol)

        cur_price = self.cta_engine.get_price(vt_symbol)
        if cur_price == 0:
            return False

        max_volume = invest_margin / (abs(cur_price) * margin_rate * symbol_size)

        if self.max_invest_pos > 0:
            max_volume = min(self.max_invest_pos, max_volume)
        target_volume = max(int(max_volume / self.grid_lots), 1)

        self.write_log(f'{vt_symbol} 策略最大投入:{invest_margin},总仓位:{max_volume},每格投入:{target_volume}')

        if abs(self.position.short_pos) + target_volume > max_volume:
            target_volume = 0

        if target_volume <= 0:
            self.write_log(f'{vt_symbol} 目标增仓为{target_volume}。不做卖出')
            return True

        grid = CtaGrid(direction=Direction.SHORT,
                       vt_symbol=vt_symbol,
                       open_price=open_price,
                       stop_price=0,
                       close_price=close_price,
                       volume=target_volume
                       )

        self.gt.up_grids.append(grid)
        self.write_log(u'添加做空计划{},做空数量:{},做空价格:{}'.format(grid.type, grid.volume, grid.open_price))
        self.gt.save()

        dist_record = OrderedDict()
        dist_record['datetime'] = self.cur_datetime
        dist_record['symbol'] = vt_symbol
        dist_record['volume'] = grid.volume
        dist_record['price'] = self.cur_price
        dist_record['operation'] = 'entry short'

        self.save_dist(dist_record)

        return True

    def tns_check_grids(self):
        """事务检查持仓网格，进行开仓、止盈/止损"""
        if self.entrust != 0:
            return

        if not self.trading and not self.inited:
            self.write_error(u'当前不允许交易')
            return

        remove_gids = []
        # 逐一扫描，检查做多、正套网格
        for grid in self.gt.dn_grids:

            # 清除不一致的委托id
            for vt_orderid in list(grid.order_ids):
                if vt_orderid not in self.active_orders:
                    self.write_log(f'{vt_orderid}不在活动订单中，将移除')
                    grid.order_ids.remove(vt_orderid)

            # 清除已经平仓的网格，无持仓/委托的网格
            if grid.close_status and not grid.open_status and not grid.order_status and len(grid.order_ids) == 0:
                self.write_log(f'非开仓，无委托，将移除:{grid.__dict__}')
                remove_gids.append(grid.id)
                continue

            #  正在委托的
            if grid.order_status:
                continue

            # 检查持仓网格,是否满足止盈/止损条件
            if grid.open_status:
                # 判断是否满足止盈条
                cur_price = self.cta_engine.get_price(grid.vt_symbol)
                if cur_price is None:
                    self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                    continue

                if cur_price >= grid.close_price:
                    self.write_log(f'多单满足止盈条件')
                    if self.grid_sell(grid):
                        grid.close_status = True
                        grid.order_status = True
                        continue

                if self.long_stop_price is not None and self.long_stop_price > cur_price:
                    self.write_log(f'多单满足强制止损条件')
                    if self.grid_sell(grid):
                        grid.close_status = True
                        grid.order_status = True
                        continue

            # 检查未开仓网格，检查是否满足开仓条件
            if not grid.close_status and not grid.open_status:

                # 预判是否止损
                if self.long_stop_price is not None and self.long_stop_price > grid.open_price:
                    continue

                if grid.open_price >= self.cur_price > grid.open_price - abs(grid.close_price - grid.open_price):
                    self.write_log(f'当前价:{self.cur_price}满足开仓价格:{grid.open_price}，进行开多，止盈价:{grid.close_price}')
                    if self.grid_buy(grid):
                        grid.order_status = True
                        grid.stop_price = 0
                        continue

        if len(remove_gids) > 0:
            self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_gids)

        remove_gids = []
        for grid in self.gt.up_grids:

            # 清除不一致的委托id
            for vt_orderid in list(grid.order_ids):
                if vt_orderid not in self.active_orders:
                    self.write_log(f'{vt_orderid}不在活动订单中，将移除')
                    grid.order_ids.remove(vt_orderid)

            # 清除已经平仓的网格，无持仓/委托的网格
            if grid.close_status and not grid.open_status and not grid.order_status and len(grid.order_ids) == 0:
                self.write_log(f'非开仓，无委托，将移除:{grid.__dict__}')
                remove_gids.append(grid.id)
                continue

            #  正在委托的
            if grid.order_status:
                continue

            # 检查持仓网格,是否满足止盈/止损条件
            if grid.open_status:
                # 判断是否满足止盈条
                cur_price = self.cta_engine.get_price(grid.vt_symbol)
                if cur_price is None:
                    self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                    continue

                if cur_price <= grid.close_price:
                    self.write_log(f'空单满足止盈条件')
                    if self.grid_cover(grid):
                        grid.close_status = True
                        grid.order_status = True
                        continue

                if self.short_stop_price is not None and self.short_stop_price < cur_price:
                    self.write_log(f'空单满足强制止损条件')
                    if self.grid_cover(grid):
                        grid.close_status = True
                        grid.order_status = True
                        continue

            # 检查未开仓网格，检查是否满足开仓条件
            if not grid.close_status and not grid.open_status:

                # 预判是否止损
                if self.short_stop_price is not None and self.short_stop_price < grid.open_price:
                    continue

                if grid.open_price <= self.cur_price < grid.open_price + abs(grid.close_price - grid.open_price):
                    self.write_log(f'当前价:{self.cur_price}满足开仓价格:{grid.open_price}，进行开空，止盈价:{grid.close_price}')
                    if self.grid_short(grid):
                        grid.order_status = True
                        grid.stop_price = 0
                        continue

        if len(remove_gids) > 0:
            self.gt.remove_grids_by_ids(direction=Direction.SHORT, ids=remove_gids)

    def grid_buy(self, grid):
        """
        事务开多仓
        :return:
        """
        if self.backtesting:
            buy_price = self.cur_price + self.price_tick
        else:
            buy_price = self.cur_tick.last_price

        vt_orderids = self.buy(vt_symbol=self.vt_symbol,
                               price=buy_price,
                               volume=grid.volume,
                               order_type=self.order_type,
                               order_time=self.cur_datetime,
                               lock=self.exchange == Exchange.CFFEX,
                               grid=grid)

        if len(vt_orderids) > 0:
            self.write_log(u'创建{}事务多单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                           .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))
            self.gt.save()
            return True
        else:
            self.write_error(u'创建{}事务多单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                             .format(grid.type, grid.open_price, grid.volume, grid.close_price))
            return False

    def grid_sell(self, grid):
        """
        事务平多单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        self.write_log(u'执行事务平多仓位:{}'.format(grid.to_json()))
        """
        self.account_pos = self.cta_engine.get_position(
            vt_symbol=self.vt_symbol,
            direction=Direction.NET)

        if self.account_pos is None:
            self.write_error(u'无法获取{}得持仓信息'.format(self.vt_symbol))
            return False
        """
        # 发出委托卖出单
        if self.backtesting:
            sell_price = self.cur_price - self.price_tick
        else:
            sell_price = self.cta_engine.get_price(grid.vt_symbol)
            if sell_price is None:
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                return False

        # 发出平多委托
        if grid.traded_volume > 0:
            grid.volume -= grid.traded_volume
            grid.volume = round(grid.volume, 7)
            grid.traded_volume = 0

        vt_orderids = self.sell(
            vt_symbol=grid.vt_symbol,
            price=sell_price,
            volume=grid.volume,
            order_type=self.order_type,
            order_time=self.cur_datetime,
            lock=self.exchange == Exchange.CFFEX,
            grid=grid)
        if len(vt_orderids) == 0:
            if self.backtesting:
                self.write_error(u'多单平仓委托失败')
            else:
                self.write_error(u'多单平仓委托失败')
            return False
        else:
            self.write_log(u'多单平仓委托成功，编号:{}'.format(vt_orderids))

            return True

    def grid_short(self, grid):
        """
        事务开空仓
        :return:
        """
        if self.backtesting:
            short_price = self.cur_price - self.price_tick
        else:
            short_price = self.cur_tick.last_price

        vt_orderids = self.short(vt_symbol=self.vt_symbol,
                                 price=short_price,
                                 volume=grid.volume,
                                 order_type=self.order_type,
                                 order_time=self.cur_datetime,
                                 lock=self.exchange == Exchange.CFFEX,
                                 grid=grid)

        if len(vt_orderids) > 0:
            self.write_log(u'创建{}事务空单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                           .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))
            self.gt.save()
            return True
        else:
            self.write_error(u'创建{}事务空单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                             .format(grid.type, grid.open_price, grid.volume, grid.close_price))
            return False

    def grid_cover(self, grid):
        """
        事务平空单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        self.write_log(u'执行事务平空仓位:{}'.format(grid.to_json()))

        # 发出委托卖出单
        if self.backtesting:
            cover_price = self.cur_price + self.price_tick
        else:
            cover_price = self.cta_engine.get_price(grid.vt_symbol)
            if cover_price is None:
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                return False

        # 发出平空委托
        if grid.traded_volume > 0:
            grid.volume -= grid.traded_volume
            grid.volume = round(grid.volume, 7)
            grid.traded_volume = 0

        vt_orderids = self.cover(
            vt_symbol=grid.vt_symbol,
            price=cover_price,
            volume=grid.volume,
            order_type=self.order_type,
            order_time=self.cur_datetime,
            lock=self.exchange == Exchange.CFFEX,
            grid=grid)
        if len(vt_orderids) == 0:
            if self.backtesting:
                self.write_error(u'空单平仓委托失败')
            else:
                self.write_error(u'空单平仓委托失败')
            return False
        else:
            self.write_log(u'空单平仓委托成功，编号:{}'.format(vt_orderids))

            return True

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return
        self.write_log(u'{} 当前{}价格:{}'
                       .format(self.cur_datetime,
                               self.vt_symbol, self.cta_engine.get_price(self.vt_symbol)))
        if hasattr(self, 'policy'):
            policy = getattr(self, 'policy')
            op = getattr(policy, 'to_json', None)
            if callable(op):
                self.write_log(u'当前Policy:{}'.format(policy.to_json()))

    def tns_cancel_logic(self, dt, force=False, reopen=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders[vt_orderid]
            order_vt_symbol = order_info.get('vt_symbol', self.vt_symbol)
            order_time = order_info['order_time']
            order_volume = order_info['volume'] - order_info['traded']
            # order_price = order_info['price']
            # order_direction = order_info['direction']
            # order_offset = order_info['offset']
            order_grid = order_info['grid']
            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.NOTTRADED, Status.SUBMITTING] and (
                    order_type == OrderType.LIMIT or '.SPD' in order_vt_symbol):
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_log(f'{vt_orderid}撤单失败')

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

                if reopen:
                    # 撤销的委托单，属于开仓类，需要重新委托
                    if order_info['offset'] == Offset.OPEN:
                        self.write_log(u'超时撤单后，重新开仓')
                        # 开空委托单
                        if order_info['direction'] == Direction.SHORT:
                            short_price = self.cur_mi_price - self.price_tick
                            if order_grid.volume != order_volume and order_volume > 0:
                                self.write_log(
                                    u'网格volume:{},order_volume:{}不一致，修正'.format(order_grid.volume, order_volume))
                                order_grid.volume = order_volume

                            self.write_log(u'重新提交{}开空委托,开空价{}，v:{}'.format(order_vt_symbol, short_price, order_volume))
                            vt_orderids = self.short(price=short_price,
                                                     volume=order_volume,
                                                     vt_symbol=order_vt_symbol,
                                                     order_type=order_type,
                                                     order_time=self.cur_datetime,
                                                     grid=order_grid)

                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderid:{}'.format(vt_orderids))
                                order_grid.snapshot.update({'open_price': short_price})
                            else:
                                self.write_error(u'撤单后，重新委托开空仓失败')
                        else:
                            buy_price = self.cur_mi_price + self.price_tick
                            if order_grid.volume != order_volume and order_volume > 0:
                                self.write_log(
                                    u'网格volume:{},order_volume:{}不一致，修正'.format(order_grid.volume, order_volume))
                                order_grid.volume = order_volume

                            self.write_log(u'重新提交{}开多委托,开多价{}，v:{}'.format(order_vt_symbol, buy_price, order_volume))
                            vt_orderids = self.buy(price=buy_price,
                                                   volume=order_volume,
                                                   vt_symbol=order_vt_symbol,
                                                   order_type=order_type,
                                                   order_time=self.cur_datetime,
                                                   grid=order_grid)

                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                                order_grid.snapshot.update({'open_price': buy_price})
                            else:
                                self.write_error(u'撤单后，重新委托开多仓失败')
                    else:
                        # 属于平多委托单
                        if order_info['direction'] == Direction.SHORT:
                            sell_price = self.cur_mi_price - self.price_tick
                            self.write_log(u'重新提交{}平多委托,{}，v:{}'.format(order_vt_symbol, sell_price, order_volume))
                            vt_orderids = self.sell(price=sell_price,
                                                    volume=order_volume,
                                                    vt_symbol=order_vt_symbol,
                                                    order_type=order_type,
                                                    order_time=self.cur_datetime,
                                                    grid=order_grid)
                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                            else:
                                self.write_error(u'撤单后，重新委托平多仓失败')
                        # 属于平空委托单
                        else:
                            cover_price = self.cur_mi_price + self.price_tick
                            self.write_log(u'重新提交{}平空委托,委托价{}，v:{}'.format(order_vt_symbol, cover_price, order_volume))
                            vt_orderids = self.cover(price=cover_price,
                                                     volume=order_volume,
                                                     vt_symbol=order_vt_symbol,
                                                     order_type=order_type,
                                                     order_time=self.cur_datetime,
                                                     grid=order_grid)
                            if len(vt_orderids) > 0:
                                self.write_log(u'委托成功，orderids:{}'.format(vt_orderids))
                            else:
                                self.write_error(u'撤单后，重新委托平空仓失败')
                else:
                    if order_info['offset'] == Offset.OPEN \
                            and order_grid \
                            and len(order_grid.order_ids) == 0 \
                            and order_grid.traded_volume == 0:
                        self.write_log(u'移除委托网格{}'.format(order_grid.__dict__))
                        order_info['grid'] = None
                        self.gt.remove_grids_by_ids(direction=order_grid.direction, ids=[order_grid.id])

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(u'删除orderID:{0}'.format(vt_orderid))
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0


class Future_Grid_Trade_Policy(CtaPolicy):

    def __init__(self, strategy):
        super().__init__(strategy)
        # vt_symbol: {
        # name: "合约",
        # high_price: xxx,
        # low_price：xxx,
        # grid_height: x,
        # long_prices: [x,x,x,x]
        # short_prices: [x,x,x,x]
        self.grids = {}

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)
        self.grids = json_data.get('grids', {})
        # 兼容
        for k, v in self.grids.items():
            if 'open_prices' in v:
                v.update({'long_prices': v.pop('open_prices', [])})

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['grids'] = self.grids

        return j
