# encoding: UTF-8

# 首先写系统内置模块
import sys
from datetime import datetime, timedelta
from copy import copy
import json
import traceback
from collections import OrderedDict, defaultdict
from typing import Dict

# 其次，导入vnpy的基础模块
from vnpy.trader.utility import extract_vt_symbol, round_to
from vnpy.app.cta_stock import (
    StockPolicy,
    CtaStockTemplate,
    Exchange,
    Direction,
    Status,
    OrderType,
    TickData,
    BarData,
    Interval
)

from vnpy.component.cta_grid_trade import (
    CtaGridTrade,
    CtaGrid
)
from vnpy.component.cta_line_bar import CtaMinuteBar
from vnpy.component.cta_utility import *
from vnpy.data.eastmoney.em_stock_data import EastMoneyData
from vnpy.trader.object import HistoryRequest


########################################################################
class StrategyStockGridTradeV3(CtaStockTemplate):
    """股票网格交易策略
        1、筛选股票
        2、资金管理：80%的资金平均分到每个票中作为底仓
        3、网格建仓：
            剩下20%资金，网格策略：竞争性仓位
            选择其中M个票进行网格交易
            每个股票都开始布10个网格，每格2*20%/(10*M)的仓位
                以现价往下布网格
                以每4%为网格间距，每格以4%为止盈，出现跳空低开的要补网格手数
                出现新高后更新网格
                布好的网格可以重用
        v2
        增加x分钟K线和缠论，只有在下跌线段，并满足背驰或两个中枢以上，才接多单
        v3:
        引入czsc得形态分析，所有类二买、类三买才接多单

    """
    author = u'大佳'

    max_single_margin = 10000  # 策略内，各只股票(单个网格)使用的资金上限
    grid_height_percent = 4
    grid_lots = 10
    x_minute = 3  # 3分钟K线
    parameters = ["max_invest_margin", "max_invest_rate", "max_single_margin",
                  "grid_height_percent", "grid_lots", "x_minute",
                  "backtesting"]

    def __init__(self, cta_engine, strategy_name, vt_symbols, setting):
        super().__init__(cta_engine, strategy_name, vt_symbols, setting)

        # 创建做多网格交易
        self.gt = CtaGridTrade(strategy=self)
        self.names = {}
        self.policy = Stock_Grid_Trade_Policy(self)  # 成交后的执行策略

        self.cancel_seconds = 3000

        self.last_minute = None

        if setting:
            self.update_setting(setting)

            # 更新 vt_symbol <=> 股票中文名
            for vt_symbol in self.vt_symbols:
                # 更新中文名称
                self.names.update({vt_symbol: self.cta_engine.get_name(vt_symbol)})
                # 创建K线
                self.create_kline(vt_symbol)

            if self.backtesting:
                self.export_klines()

    def create_kline(self, vt_symbol):
        """创建K线"""
        cn_name = self.cta_engine.get_name(vt_symbol)
        kline_setting = {}
        kline_setting['name'] = vt_symbol
        kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
        kline_setting['price_tick'] = self.cta_engine.get_price_tick(vt_symbol)
        kline_setting['underly_symbol'] = vt_symbol
        kline_setting['para_active_chanlun'] = True  # 激活缠论
        kline_setting['para_active_chan_xt'] = True  # 激活缠论的形态分析
        kline_setting['is_stock'] = True

        kline = CtaMinuteBar(self, self.on_bar_k, kline_setting)
        self.klines.update({kline.name: kline})
        self.write_log(f'添加{cn_name} k线:{kline_setting}')


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
                {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'}

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

    def on_init(self):
        """初始化"""
        self.write_log(f'{self.strategy_name}策略初始化')
        if self.inited:
            self.write_log(f'{self.strategy_name}已经初始化过，不再执行')
            return

        self.init_policy()  # 初始策略执行类

        self.init_position()  # 恢复持久化持仓信息

        if not self.backtesting:
            for vt_symbol in self.vt_symbols:
                self.init_kline_data(vt_symbol)

        self.inited = True

        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化完成')

    def init_policy(self):
        super().init_policy()

        if len(self.policy.grids) > 0:
            for vt_symbol in list(self.policy.grids.keys()):
                if vt_symbol not in self.vt_symbols:
                    grids = self.policy.grids.pop(vt_symbol, None)
                    c_name = self.cta_engine.get_name(vt_symbol)
                    self.write_log(f'{vt_symbol}[{c_name}]不在配置得合约中了，进行移除:{grids}')
                    self.policy.save()

    def init_position(self):
        """加载持仓"""
        super().init_position()

        changed = False
        remove_ids = []
        for grid in self.gt.dn_grids:
            # 移除委托状态，且没有成交得
            if grid.order_status and not grid.open_status and not grid.close_price \
                    and grid.traded_volume == 0 and len(grid.order_ids) == 0:
                self.write_log(f'网格处于开仓委托状态，移除: {grid.__dict__}')
                cn_name = self.cta_engine.get_name(grid.vt_symbol)
                self.write_log(f'{grid.vt_symbol}[{cn_name}]网格处于开仓委托状态，去除买入网格计划')
                remove_ids.append(grid.id)
                continue

            # 网格处于平仓委托状态，恢复持仓状态
            if grid.close_status \
                    and grid.order_status \
                    and grid.open_status \
                    and grid.volume - grid.traded_volume > 0 \
                    and len(grid.order_ids) == 0:
                self.write_log(f'网格处于平仓委托状态，恢复持仓状态: {grid.__dict__}')
                grid.close_status = False
                grid.order_status = False
                if grid.volume > grid.traded_volume > 0:
                    self.write_log(f'调整持仓数量:{grid.volume} => {grid.volume - grid.traded_volume}')
                    grid.volume -= grid.traded_volume
                    grid.traded_volume = 0

                changed = True

            # if not grid.open_status and grid.order_status and grid.vt_symbol not in self.vt_symbols and grid.traded_volume ==0:
            if grid.vt_symbol not in self.vt_symbols:
                cn_name = self.cta_engine.get_name(grid.vt_symbol)

                self.write_log(f'{grid.vt_symbol}[{cn_name}]不在vt_symbols清单中，先进行订阅，有行情后进行平仓')
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)

                if not grid.open_status and grid.order_status and grid.traded_volume == 0:
                    self.write_log(f'{grid.vt_symbol}[{cn_name}]不在vt_symbols清单中，去除买入网格计划')
                    remove_ids.append(grid.id)

        if len(remove_ids):
            self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_ids)
            changed = True

        if changed:
            self.gt.save()


    def init_kline_data(self, vt_symbol):
        """
        初始化K线数据
        :param vt_symbol:
        :return:
        """
        symbol, exchange = extract_vt_symbol(vt_symbol)
        start_date = None

        kline_names =[vt_symbol]
        if self.check_adjust(vt_symbol):
            # 从指定缓存文件中，获取vt_symbol的所有缓存数据
            last_cache_dt = self.load_klines_from_cache(kline_names=kline_names, vt_symbol=vt_symbol)

        for name in kline_names:
            kline = self.klines.get(name, None)
            if kline is None:
                continue

            if kline.interval == Interval.MINUTE:

                days = int((2000 / 4 / (60 / kline.bar_interval) / 5) * 7)

                bar_type = 'm'
                tdx_bar_type = 'min'
            elif kline.interval == Interval.HOUR:

                days = int((2000 / 4 / kline.bar_interval / 5) * 7)
                bar_type = 'h'
                tdx_bar_type = 'hour'
            elif kline.interval == Interval.DAILY:
                days = 600
                bar_type = 'd'
                tdx_bar_type = 'day'
            else:
                days = 300
                bar_type = 'm'
                tdx_bar_type = 'min'

            if kline.cur_datetime is None or len(kline.line_bar) == 0:
                self.write_log(f'[初始化]{kline.name}无本地缓存文件pkb2，取{days}天{kline.bar_interval}[{kline.interval.value}]数据')
                bars = self.cta_engine.get_bars(
                    vt_symbol=vt_symbol,
                    days=days,
                    interval=kline.interval,
                    interval_num=kline.bar_interval)
                bar_len = len(bars)
                if bar_len > 0:

                    self.write_log(f'[初始化]一共获取{bar_len}条{vt_symbol} {kline.bar_interval} [{kline.interval.value}]数据')
                    bar_count = 0

                    for bar in bars:

                        # 判断是否比最后bar时间早
                        # 推送bar => kline
                        last_bar_dt = kline.cur_datetime
                        if last_bar_dt is not None and bar.datetime < last_bar_dt:
                            continue
                        kline.add_bar(bar)
                else:
                    self.write_error(f'[初始化]获取{vt_symbol}'
                                     '{self.cta_engine.get_name(vt_symbol)}'
                                     '{kline.bar_interval}[{kline.interval.value}] {days}天数据失败')

            temp_dt = kline.cur_datetime
            if temp_dt is None:
                temp_dt = datetime.now() - timedelta(days=days)

            if self.em_stock_data is None:
                self.em_stock_data = EastMoneyData(self)
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                start=temp_dt,
                interval=Interval.MINUTE,
                interval_num=1
            )
            bars = self.em_stock_data.get_bars(req=req, return_dict=False)

            if len(bars) > 0:
                self.write_log(
                    u'已下载{}[{}] bars:,共{}'.format(vt_symbol, self.cta_engine.get_name(vt_symbol), len(bars)))
                for bar in bars:
                    if bar.datetime < temp_dt:
                        continue
                    if bar.datetime > datetime.now():
                        continue
                    kline.add_bar(bar)

            else:
                self.write_error(f'[初始化]em_stock_data获取{vt_symbol} 1分钟数据失败, 数据最后时间:{temp_dt}')

        self.save_klines_to_cache(kline_names=kline_names, vt_symbol=vt_symbol)


    def on_tick(self, tick_dict: Dict[str, TickData]):
        """行情更新
        :type tick: object
        """
        # 实盘检查是否初始化数据完毕。如果数据未初始化完毕，则不更新tick，避免影响cur_price
        if not self.backtesting:
            if not self.inited:
                self.write_log(u'数据还没初始化完毕，不更新tick')
                return

        run_once = False

        for vt_symbol, tick in tick_dict.items():

            # 更新策略执行的时间（用于回测时记录发生的时间）
            if not self.cur_datetime or tick.datetime > self.cur_datetime:
                self.cur_datetime = tick.datetime

            # 集合竞价时间
            if self.cur_datetime.hour == 9 and self.cur_datetime.minute < 30:
                continue

            # 执行撤单逻辑
            if not run_once:
                self.tns_cancel_logic(tick.datetime)
                run_once = True

                # 网格逐一止损/止盈检查
                self.grid_check_stop()

            # 检查网格
            self.tns_update_grids(vt_symbol)

            # 检查卖出网格
            self.tns_excute_sell_grids(vt_symbol)

            # 检查买入网格
            self.tns_execute_buy_grids(vt_symbol)

            if self.inited:
                kline = self.klines.get(vt_symbol, None)
                if kline:
                    kline.on_tick(tick)

        if self.inited:

            if self.last_minute != self.cur_datetime.minute:
                self.last_minute = self.cur_datetime.minute

                self.display_grids()
                # 每分钟显示事务信息
                self.display_tns()

    def bar_to_tick(self, bar):
        """ 回测时，通过bar计算tick数据 """

        tick = TickData(
            gateway_name='backtesting',
            symbol=bar.symbol,
            exchange=bar.exchange,
            datetime=bar.datetime
        )
        tick.date = bar.datetime.strftime('%Y-%m-%d')
        tick.time = bar.datetime.strftime('%H:%M:%S')
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

    def on_bar(self, bar_dict: Dict[str, BarData]):
        """
        分钟K线数据（仅用于回测时，从策略外部调用)
        :param bar:
        :return:
        """
        for k, v in bar_dict.items():
            tick = self.bar_to_tick(v)
            self.on_tick({k: tick})

    def on_bar_k(self, bar: BarData):
        """
        K线的onbar事件
        """
        pos = self.get_position(bar.vt_symbol)

        if pos.volume > 0:
            self.tns_xt_sell(bar.vt_symbol)


    def tns_xt_sell(self,vt_symbol):
        """
        事务根据形态信号，提前结束多单
        :param vt_symbol:
        :return:
        """

        cn_name = self.cta_engine.get_name(vt_symbol)

        kline = self.klines.get(vt_symbol,None)
        if kline is None:
            return

        if self.tns_check_xt(vt_symbol,direction=Direction.SHORT):
            for grid in self.gt.dn_grids:
                if grid.vt_symbol == vt_symbol:
                    if grid.open_status and not grid.close_status and not grid.order_status:
                        grid.close_status = True
                        grid.order_status = True
                        self.write_log(f'{vt_symbol}[{cn_name}]满足形态卖出，主动平仓')


    def tns_cancel_logic(self, dt, force=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        canceled_ids = []

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders[vt_orderid]
            order_vt_symbol = order_info.get('vt_symbol')
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
            if order_status in [Status.SUBMITTING, Status.NOTTRADED] and order_type == OrderType.LIMIT:
                if over_seconds > self.cancel_seconds or force:  # 超过设置的时间还未成交
                    self.write_log(u'超时{}秒未成交，取消委托单：vt_orderid:{},order:{}'
                                   .format(over_seconds, vt_orderid, order_info))
                    order_info.update({'status': Status.CANCELLING})
                    self.active_orders.update({vt_orderid: order_info})
                    ret = self.cancel_order(str(vt_orderid))
                    if not ret:
                        self.write_error(f'撤单失败,委托单号:{order_vt_symbol}')
                    #    order_info.update({'status': Status.CANCELLED})
                    #    self.active_orders.update({vt_orderid: order_info})
                    #    if order_grid:
                    #        if vt_orderid in order_grid.order_ids:
                    #            order_grid.order_ids.remove(vt_orderid)

                continue

            # 处理状态为‘撤销’的委托单
            elif order_status == Status.CANCELLED:
                self.write_log(u'委托单{}已成功撤单，删除{}'.format(vt_orderid, order_info))
                canceled_ids.append(vt_orderid)

        # 删除撤单的订单
        for vt_orderid in canceled_ids:
            self.write_log(u'删除orderID:{0}'.format(vt_orderid))
            self.active_orders.pop(vt_orderid, None)

        if len(self.active_orders) == 0:
            self.entrust = 0

    def tns_check_xt(self, vt_symbol, direction=Direction.LONG):
        """
        事务检查K线形态，是否满足类二买、类三买
        :param vt_symbol:
        :param direction: LONG: 是否满足类二买、类三买，是否满足类一卖，类二卖、类三卖
        :return:
        """
        cn_name = self.cta_engine.get_name(vt_symbol)
        # v1.2，获取K线
        kline = self.klines.get(vt_symbol, None)
        if kline is None:
            self.write_error(f'无法获取{vt_symbol}[{cn_name}]的K线')
            return False

        if not kline.cur_bi:
            return False

        # 做多判断
        if direction == Direction.LONG:
            # 分笔向下，具有底分型，才接多
            if check_bi_not_rt(kline, direction=Direction.SHORT):

                # 在9~11分笔形态中，寻找类二买、类三买信号
                for n in [9, 11]:
                    xt_signals = getattr(kline, f'xt_{n}_signals')
                    if xt_signals and len(xt_signals) > 0:
                        xt_signal = xt_signals[-1]
                        if xt_signal['signal'] in [ChanSignals.Q2L0.value, ChanSignals.Q3L0.value]:
                            return True

        # 卖出判断
        else:
            # 当前分笔是多，且形成顶分型
            if check_bi_not_rt(kline, direction=Direction.LONG):
                # 在9~13分笔形态中，寻找类一卖、类二卖、类三卖信号
                for n in [9, 11, 13]:
                    xt_signals = getattr(kline, f'xt_{n}_signals')
                    if xt_signals and len(xt_signals) > 0:
                        xt_signal = xt_signals[-1]
                        if xt_signal['signal'] in [ChanSignals.Q1S0.value, ChanSignals.Q2S0.value,
                                                   ChanSignals.Q3S0.value]:
                            return True

        return False

    def tns_update_grids(self, vt_symbol):
        """
        更新网格
        1. 不存在policy时，初始化该vt_symbol的policy配置，包括当前最高价（取当前价格和pre_close价格)
        2. 如果价格高于最高价，从最高价往下，构造10个网格的买入价格。
        3. 记录最后一次开仓价格。

        :return:
        """
        if vt_symbol not in self.vt_symbols:
            c_name = self.cta_engine.get_name(vt_symbol)
            if vt_symbol in self.policy.grids:
                self.write_log(f'{self.strategy_name}:{vt_symbol}[{c_name}]不在配置vt_symbols清单中，不进行更新')
                self.policy.grids.pop(vt_symbol, None)
                self.policy.save()

            return
        # if len(self.policy.grids) == 0:
        #    return
        grid_info = self.policy.grids.get(vt_symbol, {})
        cur_price = self.cta_engine.get_price(vt_symbol)
        cn_name = self.cta_engine.get_name(vt_symbol)

        if cur_price <= 0:
            return

        # 初始化/价格高于最高价,或没有价格
        if len(grid_info) == 0 or cur_price > grid_info.get('high_price', cur_price) or len(
                grid_info.get('open_prices', [])) == 0:
            # 每个格，按照4%计算，价格跳动统一最小0.01
            grid_height = round(cur_price * self.grid_height_percent / 100, 2)
            grid_info = {
                'name': self.names.get(vt_symbol),
                'high_price': cur_price,
                'grid_height': grid_height,
                'martin_rate': 1,
                'open_prices': [round(cur_price - (i + 2) * grid_height, 2) for i in range(self.grid_lots)]
            }
            self.policy.grids.update({vt_symbol: grid_info})
            self.policy.save()
            return

        # 找出能满足当前价的开仓价
        open_prices = [p for p in grid_info.get('open_prices', []) if p >= cur_price]
        if len(open_prices) == 0:
            return
        open_price = open_prices[-1]
        grid_height = grid_info.get('grid_height', round(cur_price * self.grid_height_percent / 100, 2))

        # 检查是否存在这个价格的网格
        grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.open_price <= open_price]
        if len(grids) == 0:

            # 检查K线缠论是否具有做多形态信号
            if not self.tns_check_xt(vt_symbol, direction=Direction.LONG):
                return

            # 已开仓得多单网格
            opened_grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.open_status]
            if self.cur_datetime.strftime("%Y-%m-%d") == "2015-07-24":
                a = 1
            # 如果没有持有多单网格，开仓数量 => 多少个满足价格得格子 =>合并
            if len(opened_grids) == 0:
                volume_rate = len(open_prices)
                self.write_log(f'{vt_symbol}[{cn_name}]没有持有多单，待开仓价格:{opened_grids}, 一共{volume_rate}个网格单元')

            # 如果有多单网格, 开仓数量 => 最低开仓价格 ~ 最低持仓价格 之间累计得格子数量
            else:
                # 最低持仓价格
                lowest_open_price = min([g.open_price for g in opened_grids])
                # 计划开仓得价格，必须最低的开仓
                open_prices = [p for p in open_prices if p < lowest_open_price]

                # 没有适合的价格
                if len(open_prices) == 0:
                    return

                # 格子倍数
                volume_rate = max(len(open_prices), 1)
                self.write_log(
                    f'{vt_symbol}[{cn_name}]持有多单，最低开仓价格:{lowest_open_price},可开仓价格:{open_prices}, 一共{volume_rate}个网格单元')

            # 当开仓格子倍数超过1个时，证明时超跌了，那么，该价格上方的所有价格都需要清除，不再开仓
            if volume_rate > 1:
                # 当前所有接多的价格
                all_prices = grid_info.get('open_prices', [])
                # 保留价格及下方的价格清单
                unopen_prices = [p for p in all_prices if p <= open_price]
                # 更新
                if len(all_prices) > len(unopen_prices):
                    if len(unopen_prices) > 0:
                        max_price = max(unopen_prices)
                        if max_price:
                            high_price = max_price + grid_info.get('grid_height') * 2
                            low_price = min(unopen_prices)
                            self.write_log(
                                f'{vt_symbol}[{cn_name}]调整所有待开仓价格:{all_prices}=> {unopen_prices}, 最高价:=>{high_price}')
                            extend_open_prices = [round(low_price - (i + 1) * grid_height, 2) for i in
                                                  range(self.grid_lots - len(unopen_prices))]
                            unopen_prices.extend(extend_open_prices)

                            grid_info.update({'open_prices': unopen_prices, 'high_price': high_price})
                        else:
                            self.write_log(f'{vt_symbol}[{cn_name}]未开仓价格:{unopen_prices}')
                    else:
                        grid_info.update({'open_prices': unopen_prices})
                    self.policy.grids.update({vt_symbol: grid_info})
                    self.policy.save()

            self.write_log(f'计划添加做多网格:{vt_symbol}[{self.names.get(vt_symbol)}]')
            self.tns_add_long_grid(
                vt_symbol=vt_symbol,
                open_price=open_price,
                close_price=open_price + grid_height * 2,
                volume_rate=grid_info.get('martin_rate', 1))

    def tns_add_long_grid(self, vt_symbol, open_price, close_price, volume_rate=1):
        """
        事务添加做多网格
        :param :
        :return:
        """
        if not self.trading:
            return False

        # # 限制最多只能2格
        # volume_rate = min(2, volume_rate)

        cur_price = self.cta_engine.get_price(vt_symbol)
        if cur_price <= 0:
            return
        if self.cur_datetime.strftime("%Y-%m-%d") == "2015-07-24":
            a = 1
        cn_name = self.names[vt_symbol]
        # 策略当前已经买入股票资金累计
        strategy_cur_margin = sum([p.price * p.volume for p in self.positions.values()])
        balance, available, _, _ = self.cta_engine.get_account()
        max_invest_margin = balance * self.max_invest_rate
        if self.max_invest_margin > 0:
            self.write_log(f'策略实例使用限定最高资金投入: {self.max_invest_margin}')
            max_invest_margin = max(max_invest_margin, self.max_invest_margin)

        kline = self.klines.get(vt_symbol)
        # 对于类二、类三卖点，线段的低点，就一定是前面若干笔的低点
        stop_price = float(kline.cur_duan.low)
        # 止损价，取下跌n%，和线段低点的最高值
        stop_price =  max(stop_price, open_price * (100 - self.grid_height_percent) / 100)
        # 该网格投入资金
        grid_invest_margin = min(max_invest_margin - strategy_cur_margin, self.max_single_margin * volume_rate)
        # 目标买入的数量
        buyable_volume = int(grid_invest_margin / cur_price)
        # 该合约得最小买入手数
        min_trade_volume = self.cta_engine.get_volume_tick(vt_symbol)

        self.write_log(
            f'{vt_symbol}[{cn_name}] 策略最大资金:{self.max_invest_margin}, 已占用:{strategy_cur_margin}, '
            f'网格投入:{grid_invest_margin}, 手数比率:{volume_rate} => 该网格投入:{grid_invest_margin}'
            f',当前价:{cur_price}，最小委托数量:{min_trade_volume},该网格买入数量:{buyable_volume}')

        # 检验调整买入数量

        if min_trade_volume < 10:
            self.write_error(f'股票获取{vt_symbol}最小交易手数{min_trade_volume}异常！！')
            min_trade_volume = min(100, min_trade_volume)

        target_volume = round_to(value=buyable_volume, target=min_trade_volume)

        # 如果当前策略占用保证金为0时，并且开仓为0时，按照最少可买单位买入
        if strategy_cur_margin == 0 and target_volume == 0:
            self.write_log(f'当前策略占用保证金:{strategy_cur_margin}, 更换买入手数:{target_volume}=>{min_trade_volume}')
            target_volume = min_trade_volume

        if target_volume < min_trade_volume:
            self.write_log(f'{vt_symbol}[{cn_name}] 目标增仓为{target_volume}。不做买入')
            return True

        grid = CtaGrid(direction=Direction.LONG,
                       vt_symbol=vt_symbol,
                       open_price=open_price,
                       stop_price= stop_price,
                       close_price=close_price,
                       volume=target_volume,
                       snapshot={'name': cn_name, 'lower_open_price': True}
                       )

        grid.order_status = True
        self.gt.dn_grids.append(grid)
        self.write_log(u'添加做多计划{},买入数量:{},买入价格:{}'.format(grid.type, grid.volume, grid.open_price))
        self.gt.save()

        pos = self.positions.get(vt_symbol)

        dist_record = OrderedDict()
        dist_record['datetime'] = self.cur_datetime
        dist_record['symbol'] = vt_symbol
        dist_record['volume'] = grid.volume
        dist_record['price'] = cur_price
        dist_record['operation'] = 'entry'
        dist_record['pos'] = pos.volume if pos is not None else 0
        dist_record['yd_pos'] = pos.yd_volume if pos is not None else 0
        dist_record['td_pos'] = pos.volume - pos.yd_volume if pos is not None else 0
        self.save_dist(dist_record)

        return True

    def tns_execute_buy_grids(self, vt_symbol=None):
        """
        事务执行买入网格
        :return:
        """
        if not self.trading:
            return
        if self.cur_datetime and 9 <= self.cur_datetime.hour <= 14:
            if self.cur_datetime.hour == 12:
                return
            if self.cur_datetime.hour == 9 and self.cur_datetime.minute < 30:
                return
            if self.cur_datetime.hour == 11 and self.cur_datetime.minute >= 30:
                return

        ordering_grid = None

        for grid in self.gt.dn_grids:
            # 只扫描vt_symbol 匹配的网格
            if vt_symbol and vt_symbol != grid.vt_symbol:
                continue
            if vt_symbol not in self.vt_symbols:
                continue

            # 获取最新价格
            cur_price = self.cta_engine.get_price(grid.vt_symbol)
            if cur_price is None or cur_price == 0:
                self.write_error(f'暂时不能获取{grid.vt_symbol}最新价格, 重新发出订阅请求')
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                continue

            # 排除已经执行完毕(处于开仓状态）的网格， 或者处于平仓状态的网格
            if grid.open_status or grid.close_status:
                continue

            # 排除非委托状态的网格
            if not grid.order_status:
                continue

            # 排除存在委托单号的网格
            if len(grid.order_ids) > 0:
                continue

            if grid.volume == grid.traded_volume:
                self.write_log(u'网格计划买入:{}，已成交:{}'.format(grid.volume, grid.traded_volume))
                self.tns_finish_buy_grid(grid)
                continue

            # 开仓价低于当前价，不做买入
            if grid.open_price < cur_price:
                continue

            # 定位到首个满足条件的网格，跳出循环
            ordering_grid = grid

            balance, availiable, _, _ = self.cta_engine.get_account()
            if availiable <= 0:
                self.write_error(u'当前可用资金不足'.format(availiable))
                continue

            vt_symbol = ordering_grid.vt_symbol
            cn_name = self.cta_engine.get_name(vt_symbol)
            cur_price = self.cta_engine.get_price(vt_symbol)

            # v1.2，获取K线
            kline = self.klines.get(vt_symbol, None)
            if kline is None:
                self.write_error(f'无法获取{vt_symbol}[{cn_name}]的K线')
                continue

            # 必须有确认的底分型
            cur_fx = kline.fenxing_list[-1]
            if cur_fx.direction == -1 and cur_fx.is_rt:
                self.write_log(f'{vt_symbol}[{cn_name}]的K线底部分型未形成')
                continue

            buy_volume = ordering_grid.volume - ordering_grid.traded_volume
            min_trade_volume = self.cta_engine.get_volume_tick(vt_symbol)
            if availiable < buy_volume * cur_price:
                self.write_error(f'可用资金{availiable},不满足买入{vt_symbol},数量:{buy_volume} X价格{cur_price}')
                max_buy_volume = int(availiable / cur_price)
                max_buy_volume = max_buy_volume - max_buy_volume % min_trade_volume
                if max_buy_volume <= min_trade_volume:
                    continue
                # 计划买入数量，与可用资金买入数量的差别
                diff_volume = buy_volume - max_buy_volume
                # 降低计划买入数量
                self.write_log(f'总计划{vt_symbol}买入数量:{ordering_grid.volume}=>{ordering_grid.volume - diff_volume}')
                ordering_grid.volume -= diff_volume
                self.gt.save()
                buy_volume = max_buy_volume

            if buy_volume == 0:
                continue

            buy_price = cur_price + self.cta_engine.get_price_tick(vt_symbol) * 10

            vt_orderids = self.buy(
                vt_symbol=vt_symbol,
                price=buy_price,
                volume=buy_volume,
                order_time=self.cur_datetime,
                grid=ordering_grid)
            if vt_orderids is None or len(vt_orderids) == 0:
                self.write_error(f'委托买入失败，{vt_symbol} 委托价:{buy_price} 数量:{buy_volume}')
                continue
            else:
                self.write_log(f'{vt_orderids},已委托买入，{vt_symbol} 委托价:{buy_price} 数量:{buy_volume}')

    def tns_excute_sell_grids(self, vt_symbol=None, force=False):
        """
        事务执行卖出网格
         1、找出所有order_status=True,open_status=Talse, close_status=True的网格。
        2、比对volume和traded volume, 如果两者得数量差，大于min_trade_volume，继续发单
        :return:
        """
        if not self.trading:
            return

        if self.cur_datetime and 9 <= self.cur_datetime.hour <= 14:
            if self.cur_datetime.hour == 12:
                return
            if self.cur_datetime.hour == 9 and self.cur_datetime.minute < 30:
                return
            if self.cur_datetime.hour == 11 and self.cur_datetime.minute >= 30:
                return

        # 成功止盈的网格
        win_grid_num = 0

        try:
            ordering_grid = None

            for grid in self.gt.dn_grids:
                # 只扫描vt_symbol 匹配的网格
                if vt_symbol and vt_symbol != grid.vt_symbol:
                    continue

                cur_price = self.cta_engine.get_price(grid.vt_symbol)
                if cur_price is None or cur_price == 0:
                    self.write_error(f'暂时不能获取{grid.vt_symbol}最新价格, 重新发出订阅请求')
                    self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=grid.vt_symbol)
                    continue

                if vt_symbol not in self.vt_symbols:
                    if grid.open_status and not grid.order_status:
                        if not grid.close_status:
                            self.write_log(f'{vt_symbol}不在配置清单中，自动卖出')
                            grid.close_status = True
                            grid.order_status = True

                        if grid.close_price > cur_price:
                            self.write_log(f'调整{vt_symbol} 目标平仓价 {grid.close_price} => {cur_price * 0.9}')
                            grid.close_price = cur_price * 0.9

                # 排除: 未开仓/非平仓/非委托的网格
                if not grid.open_status or not grid.close_status or not grid.order_status:
                    continue

                # 排除存在委托单号的网格
                if len(grid.order_ids) > 0 and len("".join(grid.order_ids)) > 0:
                    self.write_log(f'{vt_symbol}存在委托单:{grid.order_ids}，不执行再次卖出')
                    continue

                if grid.volume == grid.traded_volume:
                    self.write_log(u'网格计划卖出:{}，已成交:{}'.format(grid.volume, grid.traded_volume))
                    grid_info = self.policy.grids.get(grid.vt_symbol, {})
                    martin_rate = grid_info.get('martin_rate', 1)
                    self.tns_finish_sell_grid(grid)
                    if grid.open_price > cur_price:

                        self.write_log(f'亏损单，马丁仓位系数加倍')

                        grid_info.update({'martin_rate': martin_rate * 2})

                    else:
                        win_grid_num += 1
                        self.write_log(f'盈利单，马丁仓位系数恢复1')
                        grid_info.update({'martin_rate': 1})

                    self.policy.grids.update({grid.vt_symbol: grid_info})

                    continue

                # 如果价格低于止盈价，不做卖出
                if grid.stop_price == 0 and grid.close_price > cur_price > 0:
                    continue

                # 定位到首个满足条件的网格，跳出循环
                ordering_grid = grid

                acc_symbol_pos = self.cta_engine.get_position(
                    vt_symbol=ordering_grid.vt_symbol,
                    direction=Direction.NET)
                if acc_symbol_pos is None:
                    self.write_error(u'当前{}持仓查询不到'.format(ordering_grid.vt_symbol))
                    continue

                vt_symbol = ordering_grid.vt_symbol
                sell_volume = ordering_grid.volume - ordering_grid.traded_volume

                if sell_volume > acc_symbol_pos.volume:
                    self.write_error(u'账号{}持仓{},不满足减仓目标:{}'
                                     .format(vt_symbol, acc_symbol_pos.volume, sell_volume))

                    if grid.vt_symbol not in self.vt_symbols:
                        self.write_log(f'该合约{vt_symbol}不在vt_symbols中，强制减少')
                        if acc_symbol_pos.volume > 0:
                            new_volume = ordering_grid.traded_volume + acc_symbol_pos.volume
                            self.write_log(f'合约{vt_symbol}的网格持仓{ordering_grid.volume} =>{new_volume}, '
                                           f'已交易:{ordering_grid.traded_volume},计划卖出:{sell_volume}=>{acc_symbol_pos.volume}')
                            ordering_grid.volume = new_volume
                            sell_volume = acc_symbol_pos.volume
                        else:
                            self.write_log(f'合约{vt_symbol}的网格持仓 {ordering_grid.volume} => 清零，设置状态未空白')
                            ordering_grid.volume = 0
                            ordering_grid.traded_volume = 0
                            ordering_grid.open_status = False
                            ordering_grid.order_status = False
                            ordering_grid.close_status = False
                            continue

                if sell_volume > acc_symbol_pos.yd_volume:
                    self.write_error(u'账号{}昨持仓{},不满足减仓目标:{}'
                                     .format(vt_symbol, acc_symbol_pos.yd_volume, sell_volume))
                    continue

                # 获取当前价格
                sell_price = self.cta_engine.get_price(vt_symbol) - self.cta_engine.get_price_tick(vt_symbol)
                # 发出委托卖出
                vt_orderids = self.sell(
                    vt_symbol=vt_symbol,
                    price=sell_price,
                    volume=sell_volume,
                    order_time=self.cur_datetime,
                    grid=ordering_grid)
                if vt_orderids is None or len(vt_orderids) == 0:
                    self.write_error(f'委托卖出失败，{vt_symbol} 委托价:{sell_price} 数量:{sell_volume}')
                    continue
                else:
                    self.write_log(f'已委托卖出，{vt_symbol},委托价:{sell_price}, 数量:{sell_volume}')
        except Exception as ex:
            msg = f'execute_sell_grids异常:{str(ex)}'
            self.write_error(msg)
            self.write_error(traceback.format_exc())

        if win_grid_num > 0:
            self.tns_adjust_grids(win_grid_num)

    def tns_adjust_grids(self, grid_num):
        """
        事务调整网格
        :param grid_num:  已经盈利的网格
        :return:
        """
        opened_grids = self.gt.get_opened_grids(direction=Direction.LONG)
        if len(opened_grids) == 0:
            return
        # 找出开仓价格最高的网格
        sorted_opened_grids = sorted(opened_grids, key=lambda g: g.open_price)
        grid = sorted_opened_grids[-1]
        grid_height = abs(grid.close_price - grid.open_price)
        adj_rate = grid_num * len(opened_grids) / self.grid_lots
        new_open_price = round(grid.open_price - grid_height * adj_rate, 3)
        new_close_price = new_open_price + grid_height
        self.write_log(f'网格价格调整:open:{grid.open_price}=>{new_open_price},close:{grid.close_price}=>{new_close_price}')
        grid.open_price = new_open_price
        grid.close_price = new_close_price
        self.gt.save()


class Stock_Grid_Trade_Policy(StockPolicy):

    def __init__(self, strategy):
        super().__init__(strategy)
        # vt_symbol: {
        # name: "股票中文名",
        # high_price: xxx,
        # grid_height: x,
        # martin_rate: 1,
        # open_prices: [x,x,x,x]
        self.grids = {}

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)
        self.grids = json_data.get('grids', {})

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['grids'] = self.grids

        return j
