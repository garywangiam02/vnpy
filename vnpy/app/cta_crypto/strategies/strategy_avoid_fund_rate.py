# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
from datetime import datetime, timedelta, time, date
import copy
import json
import traceback
from collections import OrderedDict
# 然后是自己编写的模块
from vnpy.trader.utility import round_to
from vnpy.app.cta_crypto.template import CtaFutureTemplate, Direction, get_underlying_symbol, Interval,TradeData,Status,OrderType
from vnpy.component.cta_policy import (
    CtaPolicy, TNS_STATUS_OBSERVATE, TNS_STATUS_READY, TNS_STATUS_ORDERING, TNS_STATUS_OPENED, TNS_STATUS_CLOSED
)
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar
from vnpy.data.binance.binance_future_data import BinanceFutureData

# {
#     "AOVID_FUND_FEE": {
#         "class_name": "StrategyAvoidFundFee",
#         "vt_symbol": "",
#         "setting": {
#             "activate_market": false,
#             "abcktesting": false,
#             "x_minute": 1,
#             "avoid_rate": 0.0005
#         }
#     }
# }

########################################################################
class StrategyAvoidFundFee(CtaFutureTemplate):
    """币安永续合约减少费率策略
    # 检查出当前帐号的持仓合约
    # 根据持仓合约，查询其费率，计算出需要被扣费的合约费用。
    资金费用=持仓仓位价值*当期资金费率
    # 如果费率>5美金，则对持仓合约进行锁定开仓，对冲其头寸。
    # 过了检查费率检查点时间，释放其锁定仓位

    """
    author = u'大佳'
    avoid_rate = 0.001  # 费率值阈值
    x_minute = 1  # 缠论辅助的5分钟K线
    # 策略在外部设置的参数
    parameters = [
        "activate_market",
        "avoid_rate",
        "x_minute",
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

        self.policy = Fund_Fee_Policy(self)

        # 仓位状态
        self.position = CtaPosition(strategy=self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)
        self.klines = {}
        self.check_point = None

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

    def create_kline(self, vt_symbol):
        """根据vt_symbol创建K线"""
        symbol = vt_symbol.split('.')[0]
        kline_setting = {}
        kline_setting['name'] = f'{vt_symbol}_M{self.x_minute}'
        kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
        kline_setting['price_tick'] = self.cta_engine.get_price_tick(vt_symbol)
        kline_setting['underly_symbol'] = symbol
        kline_setting['para_active_chanlun'] = True
        kline_setting['is_7x24'] = True

        kline_x = CtaMinuteBar(self, self.on_bar_k, kline_setting)
        self.klines.update({kline_x.name: kline_x})
        self.write_log(f'添加{vt_symbol} k线:{kline_setting}')

        # 马上补充K线数据
        self.update_kline_data(vt_symbol)
        # 订阅该vt_symbol。
        self.cta_engine.subscribe_symbol(self.strategy_name, vt_symbol)

    def on_bar_k(self, bar: BarData):
        """K线on bar事件"""
        if self.inited:
            symbol = bar.symbol
            kline_name = f'{symbol}_M{self.x_minute}'
            kline = self.klines.get(kline_name)
            if kline:
                if len(kline.duan_list) > 0:
                    d = kline.duan_list[-1]
                    self.write_log(f'{kline_name}当前段方向:{d.direction},{d.start}=>{d.end},low:{d.low},high:{d.high}')

    # ----------------------------------------------------------------------
    def on_init(self, force=False):
        """初始化"""
        self.write_log(u'策略初始化')

        if self.inited:
            if force:
                self.write_log(u'策略强制初始化')
                self.inited = False
                self.trading = False  # 控制是否启动交易
                # self.position.pos = 0  # 仓差
                # self.position.long_pos = 0  # 多头持仓
                # self.position.short_pos = 0  # 空头持仓
                self.gt.up_grids = []
                self.gt.dn_grids = []
            else:
                self.write_log(u'策略初始化')
                self.write_log(u'已经初始化过，不再执行')
                return

        # 得到持久化的Policy中的子事务数据
        self.init_policy()
        self.display_tns()

        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

        # 从本地缓存文件中加载K线，并取得最后的bar时间
        last_bar_dt = self.load_klines_from_cache()

        self.update_kline_data()
        self.inited = True
        return True

    def on_timer(self):
        """
        定时任务
        :return:
        """
        dt = datetime.now()
        # 实盘这里是每分钟执行
        if self.last_minute != dt.minute:
            self.last_minute = dt.minute

            self.tns_check_fund()

            if self.last_minute % 3 == 0:
                self.display_grids()
                self.display_tns()

                self.put_event()

    def update_kline_data(self, vt_symbols=[]):
        """
        更新所有【或指定】合约的K线数据
        :param vt_symbols:
        :return:
        """

        def on_bar_cb(bar, **kwargs):
            """历史数据的回调处理 =>推送到k线"""
            symbol = bar.symbol
            kline_name = f'{symbol}_M{self.x_minute}'
            kline = self.klines.get(kline_name)
            if kline:
                if kline.cur_datetime and bar.datetime < kline.cur_datetime:
                    return
                kline.add_bar(bar)

        for kline_name in list(self.klines.keys()):
            vt_symbol = kline_name.split('_')[0]

            if len(vt_symbols) > 0 and vt_symbol not in vt_symbols:
                continue
            kline = self.klines.get(kline_name)

            dt_now = datetime.now()
            if isinstance(kline.cur_datetime, datetime):
                self.write_log(u'{}缓存数据bar最后时间:{}'.format(kline_name, kline.cur_datetime))
                load_days = max((dt_now - kline.cur_datetime).days, 1)

            else:
                # 取 1分钟bar
                load_days = int(2000 / (24 * 60 / self.x_minute)) + 1
                self.write_log(f'{kline_name}无本地缓存文件，取{load_days}天 bar')

            self.cta_engine.load_bar(vt_symbol=vt_symbol,
                                     days=load_days,
                                     interval=Interval.MINUTE,
                                     callback=on_bar_cb)

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

        # 更新所有tick dict
        self.tick_dict.update({tick.vt_symbol: tick})

        # 更新策略执行的时间
        if self.cur_datetime is None or self.cur_datetime < tick.datetime:
            self.cur_datetime = tick.datetime

        if not self.inited or not self.trading:
            return

        kline = self.klines.get(f'{tick.vt_symbol}_M{self.x_minute}')
        if kline:
            kline.on_tick(tick)

        if self.check_point != tick.datetime.second // 6:
            self.check_point = tick.datetime.second // 6

            # 执行撤单逻辑
            self.tns_cancel_logic(tick.datetime, reopen=False)

            # 网格逐一开仓/止盈检查
            self.tns_check_grids()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """
        分钟K线数据（仅用于回测时，从策略外部调用)
        :param bar:
        :return:
        """
        pass

    def get_positions(self):
        """
        获取策略当前持仓(重构，使用主力合约）
        :return: [{'vt_symbol':symbol,'direction':direction,'volume':volume]
        """
        if not self.position:
            return []
        pos_list = []

        if self.position.long_pos > 0:
            for g in self.gt.get_opened_grids(direction=Direction.LONG):
                pos_list.append({'vt_symbol': g.vt_symbol,
                                 'direction': 'long',
                                 'volume': g.volume - g.traded_volume,
                                 'price': g.open_price})

        if abs(self.position.short_pos) > 0:
            for g in self.gt.get_opened_grids(direction=Direction.SHORT):
                pos_list.append({'vt_symbol': g.vt_symbol,
                                 'direction': 'short',
                                 'volume': abs(g.volume - g.traded_volume),
                                 'price': g.open_price})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'{}当前持仓:{}'.format(self.strategy_name, pos_list))
        return pos_list

    def on_trade(self, trade: TradeData):
        """交易更新"""
        self.write_log(u'{},交易更新:{}'
                       .format(self.cur_datetime,
                               trade.__dict__))


    def tns_check_fund(self):
        """
        事务检查资费

        :return:
        """
        # 获取所有帐号持仓
        all_pos = self.cta_engine.get_all_positions()
        all_pos = {pos.vt_symbol: pos for pos in all_pos}
        exchange = ""
        vt_symbols = []
        symbols = []
        for vt_symbol in list(all_pos.keys()):
            v = all_pos[vt_symbol]
            if not exchange:
                exchange = vt_symbol.split('.')[-1]
            if vt_symbol not in vt_symbols:
                vt_symbols.append(vt_symbol)
            if v.symbol not in symbols:
                symbols.append(v.symbol)
            if f'{vt_symbol}_M{self.x_minute}' not in self.klines:
                self.create_kline(vt_symbol)

        # 获取所有当前持仓合约得最新资金费率
        fund_rates = {"{}.{}".format(data.get('symbol'), exchange): data for data in BinanceFutureData().get_fund_rate()
                      if data.get('symbol') in symbols}

        # 移除不在pos中得vt_symbol
        for vt_symbol in list(self.policy.vt_symbols.keys()):
            if vt_symbol not in vt_symbols:
                self.write_log(f'移除{vt_symbol}在policy中得记录')
                self.policy.vt_symbols.pop(vt_symbol, None)

        begin_time = (datetime.now() + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
        # 更新 所有
        for vt_symbol in vt_symbols:
            info = self.policy.vt_symbols.get(vt_symbol, {})
            pos = all_pos.get(vt_symbol)
            info.update(fund_rates.get(vt_symbol, {}))
            info.update({"volume": pos.volume, 'price': pos.price, 'pnl': pos.pnl})
            # 移除计算得计费
            info.pop("fund", None)
            # 计算资费,结果正数，须被交易所扣取，结果为负数，交易所给你
            last_funding_rate = float(info.get('lastFundingRate', 0.0001))
            markPrice = float(info.get('markPrice', pos.price))
            fund_fee = round(pos.volume * markPrice * last_funding_rate,7)
            info.update({'fund': fund_fee})

            self.policy.vt_symbols.update({vt_symbol: info})

            # 判断资费& 持仓，是否需要平仓
            if fund_fee > 0 and abs(last_funding_rate) >= self.avoid_rate:
                if begin_time > info.get('nextFundingTime'):
                    grid_type = info.get('nextFundingTime')

                    # 仓位是多单，需要被收取资费
                    if pos.volume > 0:

                        # 检查是否存在网格中
                        grids = [g for g in self.gt.up_grids if g.vt_symbol == vt_symbol and g.type == grid_type]
                        if len(grids) > 0:
                            continue

                        grid = CtaGrid(
                            open_price=self.cta_engine.get_price(vt_symbol),
                            vt_symbol=vt_symbol,
                            direction=Direction.SHORT,
                            close_price=-sys.maxsize,
                            volume=pos.volume,
                            type=grid_type
                        )
                        self.gt.up_grids.append(grid)
                        self.write_log(f'添加{grid.vt_symbol}, 计划在{grid.type}前开出空单,数量:{grid.volume}')

                    # 仓位是空单，需要被收取资费
                    elif pos.volume < 0:
                        grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.type == grid_type]
                        if len(grids) > 0:
                            continue
                        grid = CtaGrid(
                            open_price=self.cta_engine.get_price(vt_symbol),
                            vt_symbol=vt_symbol,
                            direction=Direction.LONG,
                            close_price=sys.maxsize,
                            volume=abs(pos.volume),
                            type=grid_type
                        )
                        self.gt.dn_grids.append(grid)
                        self.write_log(f'添加{grid.vt_symbol}, 计划在{grid.type}前开出多单,数量:{grid.volume}')

        self.policy.save()

    def tns_check_grids(self):
        """事务检查持仓网格，进行开仓/止盈/止损"""
        if self.entrust != 0:
            return

        if not self.trading and not self.inited:
            self.write_error(u'当前不允许交易')
            return

        remove_gids = []
        # 逐一多头网格买入或离场
        for grid in self.gt.dn_grids:
            # 当前最新价
            cur_price = self.cta_engine.get_price(grid.vt_symbol)
            info = self.policy.vt_symbols.get(grid.vt_symbol, {})
            fund_time = info.get('nextFundingTime')
            # 强制买入时间
            force_time = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            # 当前时间推后15分钟
            check_time = (datetime.now() + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')

            if not cur_price and grid.vt_symbol:
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                continue

            # 清除不一致的委托id
            for vt_orderid in list(grid.order_ids):
                if vt_orderid not in self.active_orders:
                    self.write_log(f'{vt_orderid}不在活动订单中，将移除')
                    grid.order_ids.remove(vt_orderid)

            # 清除已经平仓的网格，无持仓/委托的网格
            if not grid.open_status and not grid.order_status and len(grid.order_ids) == 0:
                if grid.close_status or fund_time > check_time:
                    self.write_log(f'非开仓，无委托，将移除:{grid.__dict__}')
                    remove_gids.append(grid.id)
                    continue

            #  正在委托的
            if grid.order_status:
                continue

            # 检查持仓网格,是否满足止盈/止损条件
            if grid.open_status:
                # 判断是否满足止盈条件
                # 时间超过
                if fund_time is None or fund_time > check_time:
                    self.write_log(f'多单满足离场条件')
                    if self.grid_sell(grid):
                        grid.close_status = True
                        grid.order_status = True
                        continue

            # 检查未开仓网格，检查是否满足开仓条件
            if not grid.close_status and not grid.open_status:

                # 检查时间是否满足开仓要求（ 到达前1分钟内，强制买入，或者出现底分型时，买入）
                if fund_time is not None and fund_time <= force_time:
                    kline = self.klines.get(f'{grid.vt_symbol}_M{self.x_minute}')
                    # 如果K线得线段为空白，则不开仓
                    if len(kline.bi_list) == 0:
                        self.write_log(f'{kline.name}的K线分笔都未生成')
                        continue

                    # 如果改分笔为上涨分笔，不做买入
                    cur_bi = kline.bi_list[-1]
                    if cur_bi.direction == 1:
                        self.write_log(f'{kline.name}的K线分笔为向上')
                        continue

                    # 必须有确认的底分型
                    cur_fx = kline.fenxing_list[-1]
                    if cur_fx.direction == -1 and cur_fx.is_rt:
                        self.write_log(f'{kline.name}的K线底部分型未形成')
                        continue

                    self.write_log(f'{kline.name}当前价:{cur_price}满足开仓价格:{grid.open_price}，进行开多')
                    taker = False
                else:
                    self.write_log(f'{grid.vt_symbol}到达强制资费检查前一分钟内')
                    taker=True

                if self.grid_buy(grid, taker=taker):
                    grid.order_status = True
                    grid.stop_price = 0
                    continue

        if len(remove_gids) > 0:
            self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_gids)

        remove_gids = []
        # 逐一处理做空或空单离场
        for grid in self.gt.up_grids:
            # 当前最新价
            cur_price = self.cta_engine.get_price(grid.vt_symbol)
            info = self.policy.vt_symbols.get(grid.vt_symbol, {})
            fund_time = info.get('nextFundingTime')
            # 强制做空时间
            force_time = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            # 当前时间推后15分钟
            check_time = (datetime.now() + timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
            # 没有获取最新价，重新发出订阅
            if not cur_price and grid.vt_symbol:
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                continue

            # 清除不一致的委托id
            for vt_orderid in list(grid.order_ids):
                if vt_orderid not in self.active_orders:
                    self.write_log(f'{vt_orderid}不在活动订单中，将移除')
                    grid.order_ids.remove(vt_orderid)

            # 清除已经平仓的网格，无持仓/委托的网格
            if not grid.open_status and not grid.order_status and len(grid.order_ids) == 0:
                if grid.close_status or fund_time > check_time:
                    self.write_log(f'非开仓，无委托，将移除:{grid.__dict__}')
                    remove_gids.append(grid.id)
                    continue

            #  正在委托的
            if grid.order_status:
                continue

            # 检查持仓网格,是否满足止盈/止损条件
            if grid.open_status:
                # 判断是否满足止盈条件
                # 时间超过
                if fund_time is None or fund_time > check_time:
                    self.write_log(f'空单满足离场条件')
                    if self.grid_cover(grid, taker=False):
                        grid.close_status = True
                        grid.order_status = True
                        continue

            # 检查未开仓网格，检查是否满足开仓条件
            if not grid.close_status and not grid.open_status:

                # 检查时间是否满足开仓要求（ 到达前1分钟内，强制做空，或者出现顶分型时，做空）
                if fund_time is not None and fund_time <= force_time:
                    kline = self.klines.get(f'{grid.vt_symbol}_M{self.x_minute}')
                    # 如果K线得线段为空白，则不开仓
                    if len(kline.bi_list) == 0:
                        self.write_log(f'{kline.name}的K线分笔都未生成')
                        continue

                    # 如果改分笔为下跌分笔，不做卖出
                    cur_bi = kline.bi_list[-1]
                    if cur_bi.direction == -1:
                        self.write_log(f'{kline.name}的K线分笔为向下')
                        continue

                    # 必须有确认的底分型
                    cur_fx = kline.fenxing_list[-1]
                    if cur_fx.direction == 1 and cur_fx.is_rt:
                        self.write_log(f'{kline.name}的K线顶分型未形成')
                        continue

                    self.write_log(f'{kline.name}当前价:{cur_price}满足开仓价格:{grid.open_price}，进行开空')
                    taker = False
                else:
                    self.write_log(f'{grid.vt_symbol}到达强制资费检查前一分钟内')
                    taker = True

                if self.grid_short(grid, taker=taker):
                    grid.order_status = True
                    grid.stop_price = 0
                    continue

        if len(remove_gids) > 0:
            self.gt.remove_grids_by_ids(direction=Direction.SHORT, ids=remove_gids)


    def grid_buy(self, grid, **kwargs):
        """
        事务开多仓
        :return:
        """
        taker = kwargs.get('taker', True)
        tick = self.tick_dict.get(grid.vt_symbol)
        if taker:
            buy_price = tick.ask_price_1
        else:
            buy_price = tick.bid_price_1

        buy_volume = round(grid.volume - grid.traded_volume, 7)
        min_volume = self.cta_engine.get_volume_tick(grid.vt_symbol)
        if buy_volume >= min_volume:
            vt_orderids = self.buy(vt_symbol=grid.vt_symbol,
                                   price=buy_price,
                                   volume=buy_volume,
                                   order_type=self.order_type,
                                   order_time=self.cur_datetime,
                                   grid=grid)
            if len(vt_orderids) > 0:
                self.write_log(u'执行[{}, {}]事务多单,开仓价[{}=>{}，数量:[{}=>{}]'
                               .format(grid.vt_symbol, grid.type, grid.open_price, buy_price, grid.volume, buy_volume))
                self.gt.save()
                return True

        self.write_error(u'执行失败，[{}, {}]事务多单,开仓价[{}=>{}，数量:[{}=>{}]'
                       .format(grid.vt_symbol, grid.type, grid.open_price, buy_price, grid.volume, buy_volume))
        return False

    def grid_short(self, grid, **kwargs):
        """
        事务开空仓
        :return:
        """
        taker = kwargs.get('taker', True)
        tick = self.tick_dict.get(grid.vt_symbol)
        if taker:
            short_price = tick.bid_price_1
        else:
            short_price = tick.ask_price_1
        short_volume = round(grid.volume - grid.traded_volume, 7)
        min_volume = self.cta_engine.get_volume_tick(grid.vt_symbol)
        if short_volume >= min_volume:
            vt_orderids = self.short(vt_symbol=grid.vt_symbol,
                                     price=short_price,
                                     volume=short_volume,
                                     order_type=self.order_type,
                                     order_time=self.cur_datetime,
                                     grid=grid)
            if len(vt_orderids) > 0:
                self.write_log(u'执行[{}, {}]事务空单,开仓价[{}=>{}，数量:[{}=>{}]'
                               .format(grid.vt_symbol, grid.type, grid.open_price, short_price, grid.volume, short_volume))
                self.gt.save()
                return True

        self.write_error(u'执行失败:[{}, {}]事务空单,开仓价[{}=>{}，数量:[{}=>{}]'
                               .format(grid.vt_symbol, grid.type, grid.open_price, short_price, grid.volume, short_volume))
        return False

    def grid_sell(self, grid, **kwargs):
        """
        事务平多单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        # 发出委托卖出单
        self.write_log(u'执行事务平多仓位:{}'.format(grid.to_json()))
        taker = kwargs.get('taker', True)
        tick = self.tick_dict.get(grid.vt_symbol)
        if taker:
            sell_price = tick.bid_price_1
        else:
            sell_price = tick.ask_price_1

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

    def grid_cover(self, grid, **kwargs):
        """
        事务平空单仓位
        1.来源自止损止盈平仓
        :param 平仓网格
        :return:
        """
        self.write_log(u'执行事务平空仓位:{}'.format(grid.to_json()))
        taker = kwargs.get('taker', True)
        tick = self.tick_dict.get(grid.vt_symbol)
        if taker:
            cover_price = tick.ask_price_1
        else:
            cover_price = tick.bid_price_1

        # 发出cover委托
        if grid.traded_volume > 0:
            grid.volume -= grid.traded_volume
            grid.volume = round(grid.volume, 7)
            grid.traded_volume = 0

        vt_orderids = self.cover(
            price=cover_price,
            vt_symbol=grid.vt_symbol,
            volume=grid.volume,
            order_type=self.order_type,
            order_time=self.cur_datetime,
            grid=grid)

        if len(vt_orderids) == 0:
            self.write_error(f'{grid.vt_symbol}空单平仓{grid.volume}委托失败')
            return False
        else:
            self.write_log(u'{}空单平仓{}委托成功，编号:{}'.format(grid.vt_symbol,grid.volume, vt_orderids))
            return True

    def display_grids(self):
        """更新网格显示信息"""
        if not self.inited:
            return

        up_grids_info = ""
        for grid in list(self.gt.up_grids):
            if not grid.open_status and grid.order_status:
                up_grids_info += f'{grid.vt_symbol}平空中: [已平:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    up_grids_info += f'委托单号:{grid.order_ids}'
                continue

            if grid.open_status and not grid.order_status:
                up_grids_info += f'{grid.vt_symbol}持空中: [数量:{grid.volume}, 开仓时间:{grid.open_time}]\n'
                continue

            if not grid.open_status and grid.order_status:
                up_grids_info += f'{grid.vt_symbol}开空中: [已开:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    up_grids_info += f'委托单号:{grid.order_ids}'

        dn_grids_info = ""
        for grid in list(self.gt.dn_grids):
            if not grid.open_status and grid.order_status:
                dn_grids_info += f'{grid.vt_symbol}平多中: [已平:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    dn_grids_info += f'委托单号:{grid.order_ids}'
                continue

            if grid.open_status and not grid.order_status:
                dn_grids_info += f'{grid.vt_symbol}持多中: [数量:{grid.volume}, 开仓价:{grid.open_price},开仓时间:{grid.open_time}]\n'
                continue

            if not grid.open_status and grid.order_status:
                dn_grids_info += f'{grid.vt_symbol}开多中: [已开:{grid.traded_volume} => 目标:{grid.volume}, 委托时间:{grid.order_time}]\n'
                if len(grid.order_ids) > 0:
                    dn_grids_info += f'委托单号:{grid.order_ids}'

        if len(up_grids_info) > 0:
            self.write_log(up_grids_info)
        if len(dn_grids_info) > 0:
            self.write_log(dn_grids_info)

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return

        if len(self.active_orders) > 0:
            self.write_log('当前活动订单:{}'.format(self.active_orders))

        if hasattr(self, 'policy'):
            policy = getattr(self, 'policy')
            if policy:
                op = getattr(policy, 'to_json', None)
                if callable(op):
                    self.write_log(u'当前Policy:{}'.format(json.dumps(policy.to_json(), indent=2, ensure_ascii=False)))

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
            order_grid = order_info['grid']
            order_status = order_info.get('status', Status.NOTTRADED)
            order_type = order_info.get('order_type', OrderType.LIMIT)
            over_seconds = (dt - order_time).total_seconds()

            # 只处理未成交的限价委托单
            if order_status in [Status.SUBMITTING, Status.NOTTRADED] and order_type == OrderType.LIMIT:
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'{}超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(order_vt_symbol, over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_log(f'{order_vt_symbol}撤单失败,更新状态为撤单成功')
                        order_info.update({'status': Status.CANCELLED})
                        self.active_orders.update({vt_orderid: order_info})
                        if order_grid and vt_orderid in order_grid.order_ids:
                            order_grid.order_ids.remove(vt_orderid)

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)


        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(f'活动订单撤单成功，移除{vt_orderid}')
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0


class Fund_Fee_Policy(CtaPolicy):

    def __init__(self, strategy):
        super().__init__(strategy)
        # vt_symbols: {
        # symbol: BTCUSDT   # 交易对
        # markPrice: 44972.49215275   # 标的价格
        # indexPrice: 44965.44021200  # 指数价格
        # estimatedSettlePrice: 46948.50697801 # 预估结算价,仅在交割开始前最后一小时有意义
        # lastFundingRate: 0.00010000 # 最近更新的资金费率,只对永续合约有效，其他合约返回""
        # interestRate: 0.00010000    # 标的资产基础利率
        # nextFundingTime: 2021-02-24 00:00:00 # 计划执行资费时间
        # time: 2021-02-23 19:49:54.001000   # 上次查询时间
        self.vt_symbols = {}

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)
        self.vt_symbols = json_data.get('vt_symbols')

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['vt_symbols'] = self.vt_symbols

        return j
