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
class StrategyGridTradeFutureV3(CtaProFutureTemplate):
    """期货网格交易策略
    # v1：移植股票网格=》期货，按照当前价，往下n%开始布网格
    # 当创新高，没有网格时，重新布
    # v2， 支持自定义套利对（包括负数价格）,支持双向网格
    # v3,
    #   增加缠论线段，避免急速下跌是被套
    #   增加每个格子的执行计数器，如果执行计数结束，就自动清除；当更低一级网格被执行，也自动被清除

    #  单向网格做多配置方式:
    "Future_grid_LONG_pg": {
            "class_name": "StrategyGridTradeFutureV3",
            "vt_symbol": "pg2011.DCE",
            "auto_init": true,
            "auto_start": true,
            "setting": {
                "backtesting": false,
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
        "class_name": "StrategyGridTradeFutureV3",
        "vt_symbol": "SP i2009&i2101.DCE",
        "auto_init": true,
        "auto_start": true,
        "setting": {
            "backtesting": false,
            "idx_symbol": "SP i2009&i2101.DCE",
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
    # 网格重复次数
    grid_repeats: int = 2

    # 限定价格(设置后，不会从合约最高价获取), 例如指定为2000，当价格高于2000时，仍然按照2000为基准
    fixed_highest_price: float = None
    fixed_lowest_price: float = None

    long_stop_price: float = None  # 做多/正套，止损价格
    short_stop_price: float = None  # 做空/反套，止损价格

    # 启动做多/正套网格(缺省只做多）
    active_long_grid: bool = True
    # 启动做空/反套网格
    active_short_grid: bool = False

    x_minute = 3  # 缠论辅助的3分钟K线

    # 策略在外部设置的参数
    parameters = [
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "grid_height_percent", "grid_height_pips",
        "grid_lots", "fixed_highest_price", "fixed_lowest_price",
        "long_stop_price", "short_stop_price",
        "active_long_grid", "active_short_grid",
        "grid_repeats", "x_minute",
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

        self.policy = Future_Grid_Trade_PolicyV3(self)

        # 仓位状态
        self.position = CtaPosition(strategy=self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        # 最低的多单平仓价格
        self.last_long_close_price = None
        # 最高的空单平仓价格
        self.last_short_close_price = None

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)
            # 根据配置文件更新参数
            self.update_setting(setting)
            kline_setting = {}
            kline_setting['name'] = f'{self.vt_symbol}_M{self.x_minute}'
            kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
            kline_setting['price_tick'] = self.cta_engine.get_price_tick(self.vt_symbol)
            kline_setting['underly_symbol'] = self.vt_symbol.split('.')[0]
            kline_setting['para_active_chanlun'] = True
            kline_setting['is_7x24'] = True

            self.kline_x = CtaMinuteBar(self, self.on_bar_k, kline_setting)
            self.klines.update({self.kline_x.name: self.kline_x})
            self.write_log(f'添加{self.vt_symbol} k线:{kline_setting}')

        if self.backtesting:
            # 回测时，设置自动输出K线csv文件
            self.export_klines()
            # 回测时,自动初始化
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
                # {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                # {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                # {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},
                # {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                # {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                # {'name': f'upper', 'source': 'line_bar', 'attr': 'line_macd_chn_upper', 'type_': 'list'},
                # {'name': f'lower', 'source': 'line_bar', 'attr': 'line_macd_chn_lower', 'type_': 'list'},
                # {'name': f'cci', 'source': 'line_bar', 'attr': 'line_cci', 'type_': 'list'},
            ]

            # 输出分笔csv文件
            kline.export_bi_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_bi.csv'.format(self.strategy_name, kline_name)))

            # 输出笔中枢csv文件
            kline.export_zs_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_zs.csv'.format(self.strategy_name, kline_name)))

            # 输出线段csv文件
            kline.export_duan_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_duan.csv'.format(self.strategy_name, kline_name)))

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
        self.write_log(u'行情更新')
        self.write_log(tick.symbol)
        self.write_log(tick.ask_price_1)
        self.write_log(tick.bid_price_1)
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

        self.kline_x.on_tick(tick)

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

        # 实盘这里是每5分钟执行,定时更新和显示而已。
        if self.last_minute != tick.datetime.minute and tick.datetime.minute % 5 == 0:
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

    def on_bar_k(self, *args, **kwargs):
        # if self.inited:
        #     if len(self.kline_x.duan_list) > 0:
        #         d = self.kline_x.duan_list[-1]
        #         self.write_log(f'当前段方向:{d.direction},{d.start}=>{d.end},low:{d.low},high:{d.high}')
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
        # vt_symbol => 找出网格计划
        grid_plan = self.policy.grids.get(vt_symbol, {})

        # vt_symbol => 当前价格， 价格跳动
        cur_price = self.cta_engine.get_price(vt_symbol)
        price_tick = self.cta_engine.get_price_tick(vt_symbol)
        if cur_price is None or cur_price == 0:
            # 没有价格，重新订阅行情
            self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=vt_symbol)
            return

        # 修正最高价、最低价限制
        if self.active_long_grid:
            # v2版本，支持指定最高价格:
            if self.fixed_highest_price is not None and cur_price > self.fixed_highest_price:
                cur_price = self.fixed_highest_price
        if self.active_short_grid:
            if self.fixed_lowest_price is not None and cur_price < self.fixed_lowest_price:
                cur_price = self.fixed_lowest_price

        # 网格高度计算: n跳 * 跳价，或者百分比
        if self.grid_height_pips > 0:
            grid_height = self.grid_height_pips * price_tick
        else:
            # 每个格，按照4%计算
            grid_height = max(round_to(abs(cur_price) * self.grid_height_percent / 100, price_tick), 2 * price_tick)

        # 允许做多，价格高于最高价，或者当前没有计划价格清单
        if self.active_long_grid:
            # 更新做多计划
            if cur_price > grid_plan.get('high_price', -sys.maxsize) \
                    or len(grid_plan.get('long_prices', {})) == 0:
                # long_prices: {'价格1':重复次数,'价格2':重复次数，，，，}
                grid_plan.update({
                    'update_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    'high_price': cur_price,
                    'grid_height': grid_height,
                    'long_prices': {round_to(cur_price - (i + 1) * grid_height, price_tick): self.grid_repeats for i in
                                    range(self.grid_lots)}
                })
                self.policy.grids.update({vt_symbol: grid_plan})
                self.policy.save()

        # 允许做空，价格低于最低价，或者当前没有计划价格清单
        if self.active_short_grid:
            # 更新做空计划
            if cur_price < grid_plan.get('low_price', abs(cur_price) * 2) \
                    or len(grid_plan.get('short_prices', {})) == 0:
                # short_prices: {'价格1':重复次数,'价格2':重复次数，，，，}
                grid_plan.update({
                    'update_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    'low_price': cur_price,
                    'grid_height': grid_height,
                    'short_prices': {round_to(cur_price + (i + 1) * grid_height, price_tick): self.grid_repeats for i in
                                     range(self.grid_lots)}
                })
                self.policy.grids.update({vt_symbol: grid_plan})
                self.policy.save()

        # 执行做多网格计划
        if self.active_long_grid and (self.long_stop_price is None or self.long_stop_price < cur_price):

            # 满足缠论过滤条件才执行计划
            if self.is_chanlun_fit(direction=Direction.LONG, vt_symbol=vt_symbol):
                long_prices = grid_plan.get('long_prices', {})
                # 找出能满足当前价的多单开仓价
                plan_long_prices = sorted([float(p) for p in long_prices.keys() if float(p) >= cur_price])

                # 找出当前持有仓位的最小开仓价格
                last_opened_price = self.get_last_opened_price(direction=Direction.LONG, vt_symbol=vt_symbol)

                # 过滤掉已经开仓的价格
                if last_opened_price:
                    remove_long_prices = [p for p in plan_long_prices if p > last_opened_price]
                    plan_long_prices = [p for p in plan_long_prices if p < last_opened_price]

                    # 计划移除的计划价格
                    if remove_long_prices:
                        self.write_log(f'{vt_symbol}以下做多计划价格{remove_long_prices}将被移除')
                        [long_prices.pop(p, None) for p in remove_long_prices]
                        grid_plan.update({'long_prices': long_prices})
                        self.policy.grids.update({vt_symbol: grid_plan})
                        self.policy.save()

                # 如果仍然有做多计划价格
                if len(plan_long_prices) > 0:
                    # 最低的开仓价
                    open_price = plan_long_prices[0]
                    # 如果有多个计划开仓价（缠论阻挡了这些价格的开仓）
                    volume_rate = len(plan_long_prices)
                    # 获取高度
                    grid_height = grid_plan.get('grid_height', grid_height)
                    # 检查是否存在这个价格的网格
                    grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.open_price == open_price]

                    if len(grids) == 0:
                        self.write_log(f'计划添加做多网格:{vt_symbol}')
                        close_price = round_to(open_price + grid_height, price_tick)
                        if self.tns_add_long_grid(
                                vt_symbol=vt_symbol,
                                open_price=open_price,
                                close_price=close_price,
                                volume_rate=volume_rate):
                            grid_repeats = long_prices.get(open_price)
                            if grid_repeats > 1:
                                self.write_log(f'{vt_symbol}做多计划{open_price}的执行次数 {grid_repeats} => {grid_repeats - 1}')
                                grid_repeats -= 1
                                long_prices.update({open_price: grid_repeats})
                            else:
                                self.write_log(f'{vt_symbol}的做多计划{open_price}将被移除')
                                long_prices.pop(open_price, None)
                            self.policy.grids.update({vt_symbol: grid_plan})
                            self.policy.save()

        # 执行做空计划
        if self.active_short_grid and (self.short_stop_price is None or self.short_stop_price > cur_price):

            # 满足缠论执行条件才执行
            if self.is_chanlun_fit(direction=Direction.SHORT, vt_symbol=vt_symbol):
                # 找出能满足当前价的空开仓价
                short_prices = grid_plan.get('short_prices', {})
                # 找出能满足当前价的空单开仓价
                plan_short_prices = sorted([p for p in short_prices.keys() if p <= cur_price])
                # 找出当前持有仓位的最大开空仓价格
                last_opened_price = self.get_last_opened_price(direction=Direction.SHORT, vt_symbol=vt_symbol)

                # 过滤掉已经开仓的价格
                if last_opened_price:
                    remove_short_prices = [p for p in plan_short_prices if p < last_opened_price]
                    plan_short_prices = [p for p in plan_short_prices if p > last_opened_price]

                    # 计划移除的计划价格
                    if remove_short_prices:
                        self.write_log(f'{vt_symbol}以下做空计划价格{remove_short_prices}将被移除')
                        [short_prices.pop(p, None) for p in remove_short_prices]
                        grid_plan.update({'short_prices': short_prices})
                        self.policy.grids.update({vt_symbol: grid_plan})
                        self.policy.save()

                if len(plan_short_prices) > 0:
                    # 找出最大的开空价格
                    open_price = plan_short_prices[-1]
                    # 开仓格子数量（如果缠论阻挡了部分开空价，那么plan_short_prices就可能存在几个）
                    volume_rate = len(plan_short_prices)
                    # 止盈数
                    grid_height = grid_plan.get('grid_height', grid_height)
                    # 检查是否存在这个价格的网格
                    grids = [g for g in self.gt.up_grids if g.vt_symbol == vt_symbol and g.open_price == open_price]
                    if len(grids) == 0:
                        self.write_log(f'计划添加做空网格:{vt_symbol}')
                        close_price = round_to(open_price - grid_height, price_tick)
                        if self.tns_add_short_grid(
                                vt_symbol=vt_symbol,
                                open_price=open_price,
                                close_price=close_price,
                                volume_rate=volume_rate):
                            grid_repeats = short_prices.get(open_price)
                            if grid_repeats > 1:
                                self.write_log(f'{vt_symbol}做空计划{open_price}的执行次数 {grid_repeats} => {grid_repeats - 1}')
                                grid_repeats -= 1
                                short_prices.update({open_price: grid_repeats})
                            else:
                                self.write_log(f'{vt_symbol}的做空计划{open_price}将被移除')
                                short_prices.pop(open_price, None)
                            self.policy.grids.update({vt_symbol: grid_plan})
                            self.policy.save()

    def is_chanlun_fit(self, direction, vt_symbol):
        """
        缠论过滤，是否满足方向
        :param direction: 满足做单方向, Direction.LONG，做多， Direction.SHORT,适合做空
        :param vt_symbol: 该合约
        :return: True，适合， False，不适合
        """

        # 如果K线得线段为空白，则不开仓
        if self.kline_x.cur_duan is None and self.kline_x.cur_bi is None:
            # self.write_log(f'{vt_symbol}的K线线段、分笔都未生成')
            return False

        if self.kline_x.cur_duan:
            # 如果线段为上涨线段，不做买入
            if self.kline_x.cur_duan.direction == 1 and direction == Direction.LONG:
                #self.write_log(f'{vt_symbol}的K线线段为向上, 不做买入')
                return False

            # 如果线段为下跌线段，不做卖出
            if self.kline_x.cur_duan.direction == -1 and direction == Direction.SHORT:
                #self.write_log(f'{vt_symbol}的K线线段为向下, 不做卖出')
                return False

        else:
            # 如果线段为上涨分笔，不做买入
            if self.kline_x.cur_bi.direction == 1 and direction == Direction.LONG:
                #self.write_log(f'{vt_symbol}的K线分笔为向上, 不做买入')
                return False

            # 如果线段为下跌分笔，不做卖出
            if self.kline_x.cur_bi.direction == -1 and direction == Direction.SHORT:
                #self.write_log(f'{vt_symbol}的K线分笔为向下, 不做卖出')
                return False

        # 必须有确认的底分型、顶分型
        cur_fx = self.kline_x.fenxing_list[-1]

        # 执行做多计划时，必须是底分型
        if direction == Direction.LONG:
            if cur_fx.direction == -1 and cur_fx.is_rt:
                # self.write_log(f'{vt_symbol}的K线底部分型未形成')
                return False
            return True

        # 执行做空计划时，必须时顶分型
        if direction == Direction.SHORT:
            if cur_fx.direction == 1 and cur_fx.is_rt:
                # self.write_log(f'{vt_symbol}的K线顶部分型未形成')
                return False
            return True

        return False

    def get_last_opened_price(self, direction, vt_symbol):
        """
        获取最后得已开仓价格
        :param direction: Direction.LONG: 多单， Direction.SHORT: 空单
        :param vt_symbol:
        :return: 多单最小开仓价,空单得最高开仓价,没单则返回None
        """
        # 找出多单最小开仓价格
        if direction == Direction.LONG:
            opened_prices = [g.open_price for g in self.gt.dn_grids if g.open_status and g.vt_symbol == vt_symbol]
            return min(opened_prices) if opened_prices else None

        # 找出空单最高开仓价
        opened_prices = [g.open_price for g in self.gt.up_grids if g.open_status and g.vt_symbol == vt_symbol]
        return max(opened_prices) if opened_prices else None

    def tns_add_long_grid(self, vt_symbol, open_price, close_price, volume_rate=1):
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
        target_volume = max(int(max_volume / self.grid_lots) * volume_rate, 1)

        self.write_log(f'{vt_symbol} 策略最大投入:{invest_margin},总仓位:{max_volume},当前格投入:{target_volume}')

        if self.position.long_pos + target_volume > max_volume:
            target_volume = 0

        if target_volume <= 0:
            self.write_log(f'{vt_symbol} 目标增仓为{target_volume}。不做买入')
            return False

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

    def tns_add_short_grid(self, vt_symbol, open_price, close_price, volume_rate=1):
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
        target_volume = max(int(max_volume / self.grid_lots) * volume_rate, 1)

        self.write_log(f'{vt_symbol} 策略最大投入:{invest_margin},总仓位:{max_volume},当前格投入:{target_volume}')

        if abs(self.position.short_pos) + target_volume > max_volume:
            target_volume = 0

        if target_volume <= 0:
            self.write_log(f'{vt_symbol} 目标增仓为{target_volume}。不做卖出')
            return False

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

        if len(self.gt.dn_grids) == 0:
            self.last_long_close_price = None
        if len(self.gt.up_grids) == 0:
            self.last_short_close_price = None

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

                # 扫描至最低平仓价格
                if self.last_long_close_price is None or self.last_long_close_price > grid.close_price:
                    self.last_long_close_price = grid.close_price

                if self.last_long_close_price and self.last_long_close_price < grid.close_price:
                    self.write_log(f'调整浮亏多单网格的平仓价: {grid.close_price} => {self.last_long_close_price}')
                    grid.close_price = self.last_long_close_price

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

                # 预判当前价是否会触发强制止损
                if self.long_stop_price is not None and self.long_stop_price > grid.open_price and self.long_stop_price > self.cur_price:
                    continue

                # 判断是否满足缠论过滤条件
                if not self.is_chanlun_fit(direction=Direction.LONG, vt_symbol=grid.vt_symbol):
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

                # 扫描至最高平仓价格
                if self.last_short_close_price is None or self.last_short_close_price < grid.close_price:
                    self.last_short_close_price = grid.close_price

                if self.last_short_close_price and self.last_short_close_price < grid.close_price:
                    self.write_log(f'调整浮亏空单网格的平仓价: {grid.close_price} => {self.last_long_close_price}')
                    grid.close_price = self.last_short_close_price

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

                # 预判当前价是否会触发强制止损
                if self.short_stop_price is not None and self.short_stop_price < grid.open_price and self.short_stop_price <= self.cur_price:
                    continue

                # 判断是否满足缠论过滤条件
                if not self.is_chanlun_fit(direction=Direction.SHORT, vt_symbol=grid.vt_symbol):
                    continue

                # 满足价格范围
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
        if self.kline_x.cur_duan:
            self.write_log(f'当前段方向:{self.kline_x.cur_duan.direction},{self.kline_x.cur_duan.start}'
                           f'=>{self.kline_x.cur_duan.end},'
                           f'low:{self.kline_x.cur_duan.low},high:{self.kline_x.cur_duan.high}')
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


class Future_Grid_Trade_PolicyV3(CtaPolicy):

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
                v.update({'long_prices': v.pop('open_prices', {})})

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['grids'] = self.grids

        return j
