# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
import uuid
from datetime import datetime, timedelta
from copy import copy, deepcopy
import traceback
from collections import OrderedDict
from typing import Dict
import numpy as np
# 其次，导入vnpy的基础模块
from vnpy.app.cta_stock import (
    StockPolicy,
    CtaStockTemplate,
    Direction,
    Status,
    TickData,
    BarData
)
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_grid_trade import (
    CtaGridTrade,
    CtaGrid
)

from vnpy.component.cta_policy import (
    TNS_STATUS_OPENED,
    TNS_STATUS_ORDERING,
    TNS_STATUS_OBSERVATE,
    TNS_STATUS_READY,
    TNS_STATUS_CLOSED
)

from vnpy.component.cta_line_bar import (
    Interval,
    CtaLineBar,
    CtaMinuteBar,
    get_cta_bar_type)

from vnpy.data.huafu.data_source import DataSource
from vnpy.data.stock.adjust_factor import get_all_adjust_factor
from vnpy.trader.utility import get_underlying_symbol, extract_vt_symbol, get_trading_date, load_json
from vnpy.trader.constant import ChanSignals
from vnpy.component.cta_utility import *
from vnpy.trader.constant import Product
from vnpy.data.eastmoney.em_stock_data import EastMoneyData
from vnpy.trader.object import HistoryRequest

########################################################################
class StrategyStock3rdBuyGroupV1(CtaStockTemplate):
    """CTA 股票三买信号组合竞争仓位策略
    v1:
    1.日线产生三买信号
    2.次级别（30分钟）出现分笔底背离，中枢盘整，macd DIF底背离时进场
    3.基础级别（1分钟/5分钟）出现二买、三买信号时进场
    出场：
    1、日线回落中枢时离场
    2、次级别（30分钟）出现顶背驰、双重顶背驰时，启动跟随止盈离场
    3、基础级别（1分钟/5分钟)出现三卖信号时离场
    """
    author = u'大佳'

    share_symbol_count = 20  # 共享资金池得股票信号数量
    # 输入参数 基础级别_次级别_日线级别
    bar_names = 'M1_M30_D1'
    vt_symbols_json_file = ""  # 采用json文件格式得vt_symbols，
    # {
    #   "vt_symbols": [
    #     {
    #       "vt_symbol": "000568.XSHE",
    #       "cn_name": "泸州老窖",
    #       "entry_date": "2021-06-02"
    #       "active": true
    #     },
    #     {
    #       "vt_symbol": "000596.XSHE",
    #       "cn_name": "古井贡酒"
    #       如果没有active这个值或者false，策略将择时移除现有仓位
    #     }
    # }
    screener_strategies = []  # 选股结果=>csv格式, [选股实例名1，选股实例名2]
    stock_name_filters = ['st', '退']  # 选股结果的股票名称过滤，例如
    export_kline = False  # 为提高组合回测速度，输出K线功能可以紧闭

    # 策略在外部设置的参数
    parameters = ["max_invest_margin",  # 策略实例最大投入资金
                  "max_invest_rate",  # 策略实例资金最大投入比比率（占账号）
                  "max_single_margin",  # 单一股票最大投入资金
                  "vt_symbols_json_file",  # 指定观测股票的json文件
                  "screener_strategies",    # 当前账号下的选股实例名（见screener_setting.json)
                  "stock_name_filters",  # 过滤股票名字关键字
                  "share_symbol_count",  # 共享资金池的数量
                  "export_kline",
                  "backtesting"]

    # ----------------------------------------------------------------------
    def __init__(self, cta_engine,
                 strategy_name,
                 vt_symbols,
                 setting=None):
        """Constructor"""
        super().__init__(cta_engine=cta_engine,
                         strategy_name=strategy_name,
                         vt_symbols=vt_symbols,
                         setting=setting)

        self.vt_symbols_json_file = ""  # json格式得vt_symbols
        self.screener_strategies = []  # 选股实例

        # 创建一个策略规则
        self.policy = GroupPolicy(strategy=self)

        # 仓位状态
        self.positions = {}

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        self.display_bars = False
        self.pending_signals = []  # 未开仓的signal
        self.kline_inited_dict = {}
        self.vt_symbol_kline_map = {}  # 合约与K线得映射关系 vt_symbol: [kline1, kline2]

        # 所有除权因子
        self.stock_adjust_factors = get_all_adjust_factor()

        # 东财数据源
        self.em_stock_data = None

        # 是否输出K线文件数据
        self.export_kline = False

        # 正在运行选股添加
        self.syncing_screener = False
        self.syncing_json = False

        # 每只股票的tick分钟
        self.minute_dict = {}

        self.bar_01_name = 'M1'
        self.bar_02_name = 'M30'
        self.bar_03_name = 'D1'

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)
            self.bar_01_name, self.bar_02_name, self.bar_03_name = self.bar_names.split('_')

            # 实盘时，初始化会清空所有合约，具体合约从配置文件或者选股中获取
            if not self.backtesting:
                self.vt_symbols = []

            # 配置文件 => self.vt_symbols
            self.tns_sync_from_json()

            # 选股结果 => self.vt_symbols
            self.tns_sync_from_screener()

        if self.backtesting:
            # 回测时输出K线
            self.export_klines()

            # 回测时,自动初始化
            self.on_init()

    def create_klines(self, vt_symbol):
        """创建K线"""
        for bar_name in self.bar_names.split('_'):
            kline_name = f'{vt_symbol}_{bar_name}'

            if kline_name in self.klines:
                self.write_log(f'{kline_name}已创建，无需创建')
                continue

            # 创建K线
            kline_setting = {}
            kline_class, interval_num = get_cta_bar_type(bar_name)

            kline_setting['name'] = kline_name

            kline_setting['bar_interval'] = interval_num  # K线的Bar时长
            kline_setting['para_ma1_len'] = 55  # 缠论常用得第1条均线
            kline_setting['para_ma2_len'] = 89  # 缠论常用得第2条均线

            kline_setting['para_macd_fast_len'] = 12
            kline_setting['para_macd_slow_len'] = 26
            kline_setting['para_macd_signal_len'] = 9

            kline_setting['para_active_chanlun'] = True  # 激活缠论
            kline_setting['para_active_chan_xt'] = True  # 激活缠论的形态分析
            kline_setting['price_tick'] = self.cta_engine.get_price_tick(vt_symbol)
            kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol.split('.')[0]).upper()
            kline_setting['is_stock'] = True  # 股票区别于期货,没有早盘休盘时间
            self.write_log(f'创建K线:{kline_setting}')
            if bar_name == self.bar_03_name:
                on_bar_func = self.on_bar_d1
            elif bar_name == self.bar_02_name:
                on_bar_func = self.on_bar_m30
            else:
                on_bar_func = self.on_bar_k
            # 创建K线
            kline = kline_class(self, on_bar_func, kline_setting)
            # 添加到klines中
            self.klines.update({kline.name: kline})
            # 更新股票与k线得映射关系
            vt_symbol_klines = self.vt_symbol_kline_map.get(vt_symbol, [])
            vt_symbol_klines.append(kline.name)
            self.vt_symbol_kline_map[vt_symbol] = vt_symbol_klines
            # 设置当前K线状态为未初始化数据
            self.kline_inited_dict[kline.name] = self.backtesting
            # 订阅合约
            self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=vt_symbol)

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting or not self.export_kline:
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
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},
            ]
            # 输出分笔记录
            kline.export_bi_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_bi.csv'.format(self.strategy_name, kline_name)))
            # 输出笔中枢记录
            kline.export_zs_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_zs.csv'.format(self.strategy_name, kline_name)))
            # 输出线段记录
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

        if not self.backtesting:
            # 实盘扫码所有信号
            for vt_symbol in list(self.policy.signals.keys()):
                signal = self.policy.signals.get(vt_symbol)
                if signal.get('status') == TNS_STATUS_CLOSED:
                    self.policy.signals.pop(vt_symbol, None)
                    continue
                # 添加到策略配置中
                self.vt_symbols.append(vt_symbol)
                # 创建K线
                self.create_klines(vt_symbol)
                # 初始化数据
                self.init_kline_data(vt_symbol)

            # 检查不在policy中的持仓
            for grid in self.gt.dn_grids:
                if grid.open_status and not grid.order_status and not grid.close_status:
                    if grid.vt_symbol not in self.policy.signals:
                        self.write_log(f'{grid.vt_symbol}不在policy中，强制移除')
                        grid.close_status = True
                        grid.order_status = True


        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

    def on_timer(self):
        """定时器执行"""
        if self.backtesting:
            return

        self.cur_datetime = datetime.now()

        if not self.inited or not self.trading:
            return

        hour_minute = self.cur_datetime.strftime('%H%M')

        # 实盘这里是每分钟执行
        if self.last_minute is None or hour_minute > self.last_minute \
                and self.cur_datetime.second >= 5:
            self.last_minute = hour_minute

            if self.cur_datetime.minute % 3 == 0:
                self.write_log(f'定时执行tns_sync.{self.cur_datetime}')
                self.tns_sync_from_json()
                self.tns_sync_from_screener()

                self.display_tns()
                self.display_grids()

            # 交易时间
            if hour_minute < '0930' or '1130' < hour_minute < '1300' or hour_minute > '1500':
                return

            # 策略每分钟只执行一次
            self.tns_calcute_pos()

            # 每6分钟检查一次
            if self.cur_datetime.minute % 3 == 0:
                self.tns_calcute_profit()
                super().display_tns()

    def sync_data(self):
        """同步更新数据"""
        if not self.backtesting:
            self.write_log(u'开始保存k线缓存数据')
            for vt_symbol in self.vt_symbols:
                self.save_klines_to_cache(kline_names=[], vt_symbol=vt_symbol)

        if self.inited and self.trading:
            self.write_log(u'保存policy数据')
            self.policy.save()

    def tns_sync_from_json(self):
        """
        从指定vt_symbol json配置文件中获取所有纳入观测和交易得vt_symbols
        :return:
        """
        if self.backtesting:
            for vt_symbol in self.vt_symbols:
                # 创建K线
                self.create_klines(vt_symbol)
            return True

        try:
            if self.syncing_json:
                return True
            if not os.path.exists(self.vt_symbols_json_file):
                return True

            self.syncing_json = True

            # 读取源json文件 => {}
            vt_symbols = load_json(self.vt_symbols_json_file, auto_save=False)
            for d in vt_symbols:
                vt_symbol = d.get('vt_symbol')
                if not vt_symbol:
                    continue
                # 转换交易所
                vt_symbol = vt_symbol.replace('XSHE', 'SZSE').replace('XSHG', 'SSE')

                if vt_symbol in self.vt_symbols:
                    continue

                # 股票不被激活，将不纳入
                if not d.get('active', False):
                    continue
                contract = self.cta_engine.get_contract(vt_symbol)
                if contract is None or contract.product in [Product.INDEX]:
                    continue

                # 根据股票名称过滤
                if any([name in contract.name for name in self.stock_name_filters]):
                    continue

                # 添加到策略配置中
                self.vt_symbols.append(vt_symbol)

                # 创建K线
                self.create_klines(vt_symbol)

                # 初始化数据
                self.init_kline_data(vt_symbol)

        except Exception as ex:
            self.write_error(f'{self.strategy_name}读取{self.vt_symbols_json_file}发生异常:{str(ex)}')
            self.syncing_json = False
            return False

        self.syncing_json = False
        return True

    def tns_sync_from_screener(self):
        """从选股结果中获取股票清单"""
        if self.backtesting or len(self.screener_strategies) == 0:
            return

        if self.syncing_screener:
            return
        self.syncing_screener = True
        # 所有csv文件
        all_csv_files = [name for name in os.listdir(self.cta_engine.get_data_path()) if name.endswith('.csv')]

        for screener_name in self.screener_strategies:
            # 当前选股实例得所有输出结果
            results = sorted([name for name in all_csv_files if name.startswith(f'{screener_name}_')])
            if len(results) == 0:
                continue
            # 最新选股结果
            last_csv_name = results[-1]
            # 最新日期与当前不能超过一周
            if last_csv_name < "{}_{}.csv".format(screener_name,
                                                  (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")):
                continue

            import csv
            last_csv_name = os.path.join(self.cta_engine.get_data_path(), last_csv_name)

            with open(file=last_csv_name, mode='r', encoding='utf8', newline='\n') as f:
                reader = csv.DictReader(f)
                for item in reader:
                    vt_symbol = item.get('vt_symbol', "")
                    name = item.get('name', "")
                    signal = item.get('signal', "")
                    if '.' not in vt_symbol:
                        return
                    if vt_symbol in self.vt_symbols:
                        continue

                    contract = self.cta_engine.get_contract(vt_symbol)
                    # 过滤指数信号
                    if contract is None or contract.product in [Product.INDEX]:
                        continue

                    # 根据股票名称过滤
                    if any([n in contract.name for n in self.stock_name_filters]):
                        continue

                    self.write_log(f'添加{screener_name}/信号{signal}/{vt_symbol}[{name}]=> 策略vt_symbols')
                    self.vt_symbols.append(vt_symbol)
                    # 创建K线
                    self.create_klines(vt_symbol)

                    # 初始化数据
                    self.init_kline_data(vt_symbol)

        self.syncing_screener = False

    def init_kline_data(self, vt_symbol):
        """
        初始化K线数据
        :param vt_symbol:
        :return:
        """
        symbol, exchange = extract_vt_symbol(vt_symbol)
        start_date = None

        kline_names = self.vt_symbol_kline_map.get(vt_symbol, [])
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

            if kline.cur_datetime is not None:
                self.kline_inited_dict[kline.name] = True

            else:
                self.write_error(f'[初始化]em_stock_data获取{vt_symbol} 1分钟数据失败, 数据最后时间:{temp_dt}')

        self.save_klines_to_cache(kline_names=kline_names, vt_symbol=vt_symbol)


    def on_tick(self, tick_dict: Dict[str, TickData]):
        """行情更新（实盘运行，从tick导入）
        :type tick: object
        """
        # 首先检查是否是实盘运行还是数据预处理阶段
        if not (self.inited):
            return

        # 逐一推送至对应得kline
        for vt_symbol, tick in tick_dict.items():
            # 获取对应得多个K线
            kline_names = self.vt_symbol_kline_map.get(vt_symbol, [])
            # 逐一推送
            for kline_name in kline_names:
                # 获取K线
                kline = self.klines.get(kline_name, None)

                # 该K线未完成inited，直接返回
                if kline is None or not self.kline_inited_dict.get(kline_name, False):
                    continue

                kline.on_tick(copy(tick))

            # 撤单
            self.tns_cancel_logic(tick.datetime)

            # 每个股票的每分钟处理
            if tick.datetime.minute != self.minute_dict.get(vt_symbol, None):
                self.minute_dict[vt_symbol] = tick.datetime.minute

                self.tns_close_logic(vt_symbol)
                self.tns_excute_sell_grids(vt_symbol)

                self.tns_open_logic(vt_symbol)

                self.tns_execute_buy_grids(vt_symbol)

    def on_bar(self, bar_dict: Dict[str, BarData]):
        """
        分钟K线数据（仅用于回测时，从策略外部调用)
        :param bar:
        :return:
        """
        is_new_minute = False
        for vt_symbol, bar in bar_dict.items():
            if self.backtesting:

                new_dt = bar.datetime + timedelta(seconds=60)
                if self.cur_datetime and new_dt < self.cur_datetime:
                    return
                if self.cur_datetime and self.cur_datetime < new_dt:
                    is_new_minute = True

                self.cur_datetime = new_dt

                if self.inited and is_new_minute:
                    # 执行撤单逻辑
                    self.tns_cancel_logic(bar.datetime)

                    # 网格逐一止损/止盈检查
                    self.grid_check_stop()

            # 推送bar到所有K线
            try:

                # 获取对应得多个K线
                kline_names = self.vt_symbol_kline_map.get(bar.vt_symbol, [])
                # 逐一推送
                for kline_name in kline_names:
                    # 获取K线
                    kline = self.klines.get(kline_name, None)
                    # 找到K线，且该K线已经inited
                    if kline and self.kline_inited_dict.get(kline_name, False):
                        if kline_name.endswith('M1'):
                            bar_complete = True
                        else:
                            bar_complete = False
                        kline.add_bar(bar=copy(bar), bar_is_completed=bar_complete)

                self.tns_close_logic(vt_symbol)
                self.tns_excute_sell_grids(vt_symbol)

                self.tns_open_logic(vt_symbol)

                self.tns_execute_buy_grids(vt_symbol)

            except Exception as ex:
                self.write_error(u'[on_bar] 异常 {},{}'.format(str(ex), traceback.format_exc()))

            if is_new_minute:
                self.tns_calcute_pos()

            # 每6分钟检查一次
            if self.cur_datetime.minute % 6 == 0:
                self.tns_calcute_profit()

    def is_3rd_signal(self, kline: CtaLineBar, direction):
        """
        判断是否为三买、三卖信号
        从kline的形态分析结果中检查，最新的是否存在三买、三卖信号
        :param kline: k线
        :param direction: 判断的方向 Direction.LONG: 三买; Direction.SHORT: 三卖
        :return: Ture/False, n 如果True时，n就是第一笔形态三类买卖点
        """
        if kline is None or len(kline.xt_3_signals) == 0:
            return False, 0

        # 三买/三卖信号
        signal_value = ChanSignals.LI0.value if direction == Direction.LONG else ChanSignals.SI0.value

        # 从5笔分析 ~ 7笔分析结果 中寻找三买三卖
        for n in range(5, 9, 2):
            xt_signals = getattr(kline, f'xt_{n}_signals', [])
            for signal in xt_signals[-1:]:
                if signal.get('signal', None) == signal_value:
                    return True, n

        return False, 0

    def is_macd_signal(self, kline: CtaLineBar):
        """
            判断macd底背离信号
            :param kline: k线
            :param direction: 需要开仓的方向
            :return:
        """
        if kline is None:
            return False

        # 下跌线段才可以
        if kline.cur_duan.direction == 1:
            return False

        # 为了更稳重，一定要形成上涨分笔，才认为下跌的背驰可信
        if kline.cur_bi.direction == -1:
            return False

        is_fx_div = kline.is_fx_macd_divergence(direction=Direction.SHORT, cur_duan=kline.cur_duan)

        is_macd_div = False
        if len(kline.macd_segment_list) < 4:
            return is_fx_div

        # 重新计算, 最后一个segment是做多的，所以从倒数4取三个segment
        tre_seg, pre_seg, cur_seg = kline.macd_segment_list[-4:-1]
        if tre_seg['start'] > kline.cur_duan.start \
                and (tre_seg['min_dif'] > cur_seg['min_dif'] or tre_seg['min_macd'] > cur_seg['min_macd']) \
                and tre_seg['min_price'] > cur_seg['min_price'] \
                and cur_seg['macd_count'] < 0:
            is_macd_div = True

        # 屏蔽掉3卖信号得背驰
        ret, n = self.is_3rd_signal(kline, Direction.SHORT)
        if ret:
            return False

        if is_fx_div or is_macd_div:
            return True

        return False

    def tns_discover_d1_3rd(self, vt_symbol):
        """
        发现日线三买信号
        该方法为提高回测运算速度使用, 试盘时，由tns_open_logic继续每分钟检查
        :param vt_symbol:
        :return:
        """
        # 获取policy的事务中属于该股票的信号
        signal = self.policy.signals.get(vt_symbol, None)
        d_kline = self.klines.get(f'{vt_symbol}_D1', None)  # 日线
        if d_kline is None:
            # self.write_log(f'{vt_symbol} K线数据还未就绪')
            return

        # 没有信号 => 发现信号
        if signal is None:
            # 复查日线是否有三买
            ret, n = self.is_3rd_signal(d_kline, Direction.LONG)
            if ret and d_kline.cur_bi.direction == -1:
                cn_name = self.cta_engine.get_name(vt_symbol)
                signal = {'vt_symbol': vt_symbol,
                          'cn_name': cn_name,
                          'last_signal': 'long',
                          'd1_3rd_time': d_kline.cur_bi.end,  # 记录日线发现三买的时间
                          'd1_3rd_bi_start': d_kline.cur_bi.start,  # 日线下跌一笔开始的时间
                          'd1_3rd_bi_high': d_kline.cur_bi.high,  # 日线下跌笔的最高点
                          'd1_3rd_zs_high': max([bi.high for bi in d_kline.bi_list[-n:-n + 3]]),  # 三买点下方的高点
                          'd1_stop_price': d_kline.bi_list[-3].high,  # 日线级别止损点
                          'status': TNS_STATUS_OBSERVATE
                          }
                self.policy.signals[vt_symbol] = signal
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": d_kline.cur_price,
                    "operation": '日线三买观测',
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)

    def tns_open_logic(self, vt_symbol):
        """
        开仓逻辑
        :return:
        """

        if self.cur_datetime.strftime("%Y-%m-%d") in ['2018-10-15', '2017-10-14', '2017-12-05']:
            a = 1

        # 获取policy的事务中属于该股票的信号
        signal = self.policy.signals.get(vt_symbol, None)
        d_kline = self.klines.get(f'{vt_symbol}_D1', None)  # 日线
        m30_kline = self.klines.get(f'{vt_symbol}_M30', None)  # 30分钟线
        m1_kline = self.klines.get(f'{vt_symbol}_M1', None)  # 1分钟线
        if any([d_kline is None, m30_kline is None, m1_kline is None]):
            # self.write_log(f'{vt_symbol} K线数据还未就绪')
            return

        cn_name = self.cta_engine.get_name(vt_symbol)

        # 没有信号 => 发现信号
        if signal is None and not self.backtesting:
            # 实盘时，使用形态复查日线是否有三买
            # ret, n = self.is_3rd_signal(d_kline, Direction.LONG)
            # if ret and d_kline.cur_bi.direction == -1:
            #     signal = {'vt_symbol': vt_symbol,
            #               'cn_name': cn_name,
            #               'last_signal': 'long',
            #               'd1_3rd_time': d_kline.cur_bi.end,  # 记录日线发现三买的时间
            #               'd1_3rd_bi_start': d_kline.cur_bi.start,  # 日线下跌一笔开始的时间
            #               'd1_3rd_bi_high': d_kline.cur_bi.high,  # 日线下跌笔的最高点
            #               'd1_3rd_zs_high': max([bi.high for bi in d_kline.bi_list[-n:-n + 3]]),  # 三买点下方的高点
            #               'd1_stop_price': d_kline.bi_list[-2].low,  # 日线级别止损点
            #               'status': TNS_STATUS_OBSERVATE
            #               }

            # 使用完整的中枢+三买点复查日线是否有三买
            ret = check_zs_3rd(big_kline=d_kline, small_kline=None, signal_direction=Direction.LONG,
                               first_zs=False,
                               all_zs=False)
            if ret:
                signal = {'vt_symbol': vt_symbol,
                          'cn_name': cn_name,
                          'last_signal': 'long',
                          'd1_3rd_time': d_kline.cur_bi.end,  # 记录日线发现三买的时间
                          'd1_3rd_bi_start': d_kline.cur_bi.start,  # 日线下跌一笔开始的时间
                          'd1_3rd_bi_high': d_kline.cur_bi.high,  # 日线下跌笔的最高点
                          'd1_3rd_zs_high': d_kline.cur_bi_zs.high,  # 三买点下方的高点
                          'd1_stop_price': d_kline.cur_bi_zs.high,  # 日线级别止损点
                          'status': TNS_STATUS_OBSERVATE
                          }
                self.policy.signals[vt_symbol] = signal
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": m1_kline.cur_price,
                    "operation": '日线三买观测',
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)

        if signal is None:
            return

        signal_status = signal.get('status', '')

        # 如果状态为平仓，且不是当天交易日，则剔除
        if signal_status == TNS_STATUS_CLOSED:
            if self.cur_datetime.strftime('%Y-%m-%d') > signal.get('close_date', ""):
                self.write_log('平仓日期{}跟当前日{}不是同一天，移除信号'.format(
                    signal.get('close_date', ""), self.cur_datetime.strftime('%Y-%m-%d')))
                self.policy.signals.pop(vt_symbol, None)
                return

        # ovservate => ready or closed
        if signal_status == TNS_STATUS_OBSERVATE:
            # 观测期间，如果没有30分钟底背驰信号，三买信号消失等，都将剔除
            ret, n = self.is_3rd_signal(d_kline, Direction.LONG)
            if not ret:
                self.write_log(f'观察期间，{vt_symbol}[{cn_name}]日线3买信号消失')
                signal_status = TNS_STATUS_CLOSED
                signal.update({'status': signal_status, 'close_date': self.cur_datetime.strftime('%Y-%m-%d')})
                self.policy.signals.update({vt_symbol: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": m1_kline.cur_price,
                    "operation": '[{}]三买观测退出'.format(signal['cn_name']),
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)
                return

            if d_kline.cur_bi.direction == 1 and float(d_kline.cur_bi.high) > float(signal['d1_3rd_bi_high']):
                self.write_log(f'观察期间，{vt_symbol}[{cn_name}]日线3买信突破并消失')
                signal_status = TNS_STATUS_CLOSED
                signal.update({'status': signal_status, 'close_date': self.cur_datetime.strftime('%Y-%m-%d')})
                self.policy.signals.update({vt_symbol: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": m1_kline.cur_price,
                    "operation": '[{}]三买突破，观测退出'.format(signal['cn_name']),
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)
                return

            # 检查30分钟是否有底背驰信号
            if signal.get('m30_signal', "") == "":
                # 找到macd背驰信号, => 就绪信号
                if self.is_macd_signal(m30_kline):
                    signal_status = TNS_STATUS_READY
                    signal.update({'m30_div_time': m30_kline.cur_bi.end,
                                   'm30_bi_low': m30_kline.cur_bi.low,
                                   'status': signal_status
                                   })
                    self.policy.signals[vt_symbol] = signal
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": m1_kline.cur_price,
                        "operation": '[{}]30分钟macd背驰'.format(signal['cn_name']),
                        "signal": f'{vt_symbol}.long'
                    }
                    self.save_dist(d)

                # 找到盘整背驰1买信号, => 就绪信号
                if check_pzbc_1st(big_kline=m30_kline, small_kline=None, signal_direction=Direction.LONG):
                    signal_status = TNS_STATUS_READY
                    signal.update({'m30_div_time': m30_kline.cur_bi.end,
                                   'm30_bi_low': m30_kline.cur_bi.low,
                                   'status': signal_status
                                   })
                    self.policy.signals[vt_symbol] = signal
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": m1_kline.cur_price,
                        "operation": '[{}]30分钟盘整背驰'.format(signal['cn_name']),
                        "signal": f'{vt_symbol}.long'
                    }
                    self.save_dist(d)

                # 找到三卖后盘整背驰1买信号, => 就绪信号
                if check_pz3bc_1st(big_kline=m30_kline, small_kline=None, signal_direction=Direction.LONG):
                    signal_status = TNS_STATUS_READY
                    signal.update({'m30_div_time': m30_kline.cur_bi.end,
                                   'm30_bi_low': min(m30_kline.cur_bi.low, m30_kline.bi_list[-3].low),
                                   'status': signal_status
                                   })
                    self.policy.signals[vt_symbol] = signal
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": m1_kline.cur_price,
                        "operation": '[{}]30分钟三卖后一买'.format(signal['cn_name']),
                        "signal": f'{vt_symbol}.long'
                    }
                    self.save_dist(d)

                # 找到区间套1买信号, => 就绪信号
                if check_qjt_1st(big_kline=m30_kline, small_kline=None, signal_direction=Direction.LONG):
                    signal_status = TNS_STATUS_READY
                    signal.update({'m30_div_time': m30_kline.cur_bi.end,
                                   'm30_bi_low': min(m30_kline.cur_bi.low, m30_kline.bi_list[-3].low),
                                   'status': signal_status
                                   })
                    self.policy.signals[vt_symbol] = signal
                    self.policy.save()
                    d = {
                        "datetime": self.cur_datetime,
                        "price": m1_kline.cur_price,
                        "operation": '[{}]30分钟区间套一买'.format(signal['cn_name']),
                        "signal": f'{vt_symbol}.long'
                    }
                    self.save_dist(d)

        # 就绪状态的演变
        # 日线三买被破坏
        # 一分钟3买信号 => 进场
        if signal_status == TNS_STATUS_READY:
            # 观测期间，三买信号消失等，都将剔除
            if m1_kline.cur_price < signal['d1_stop_price']:
                self.write_log(f'就绪期间，{vt_symbol}[{cn_name}]日线3买信号到达止损价，消失')
                self.policy.signals.pop(vt_symbol, None)
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": m1_kline.cur_price,
                    "operation": '[{}]三买观测退出'.format(signal['cn_name']),
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)
                return

            if signal.get('m30_bi_low', 0) > m1_kline.cur_price \
                    and signal.get('m30_bi_low', 0) > m1_kline.close_array[-1]:
                self.write_log(f'就绪期间，{vt_symbol}[{cn_name}]30分钟低点被击穿，回归观测')
                signal.update({'status': TNS_STATUS_OBSERVATE})
                signal.pop('m30_bi_low', None)
                signal.pop('m30_div_time', None)
                self.policy.signals.update({vt_symbol: signal})
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": m1_kline.cur_price,
                    "operation": '[{}]三买就绪=>观测'.format(signal['cn_name']),
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)
                return

            # 检查1分钟是否具有3买信号,且日线满足底分型要求
            ret, n = self.is_3rd_signal(m1_kline, Direction.LONG)
            if ret and check_bi_not_rt(d_kline, Direction.SHORT):
                # 就绪状态 => 委托开仓状态
                signal_status = TNS_STATUS_ORDERING
                signal.update({
                    'm1_3rd_time': m1_kline.cur_bi.end,
                    'm1_stop_price': min(bi.low for bi in m1_kline.bi_list[-n:]),
                    'status': signal_status
                })
                self.policy.signals[vt_symbol] = signal
                self.policy.save()

        # 正在委托 => 虚拟开仓
        if signal_status == TNS_STATUS_ORDERING:
            if signal.get('m1_3rd_time', "") != "" and not signal.get('opened', False):
                signal.update({
                    'stop_price': min(m30_kline.cur_duan.low, signal['d1_stop_price'], signal['m1_stop_price']),
                    'open_price': m1_kline.cur_price,
                    'init_price': m1_kline.cur_price,
                    "status": TNS_STATUS_OPENED,  # 事务状态为虚拟开仓
                    'opened': False,  # 真实并未开仓,用于竞争仓位
                    'profit_rate': 0})

                self.policy.signals[vt_symbol] = signal
                self.policy.save()
                d = {
                    "datetime": self.cur_datetime,
                    "price": m1_kline.cur_price,
                    "operation": '[{}]1分钟三买进场'.format(signal['cn_name']),
                    "signal": f'{vt_symbol}.long'
                }
                self.save_dist(d)

        if signal.get('opened', ''):
            pass

    def tns_close_logic(self, vt_symbol):
        """
        主动离场逻辑
        1、刚开仓初期，判断是否突破高点，
        突破后，
        -启动日线双均线跟随止盈,
        -30分钟三卖后顶背驰离场
        2、开仓初期出现中枢，主动离场
        3、基础周期出现三卖信号，主动离场

        :return:
        """

        signal = self.policy.signals.get(vt_symbol, None)
        if signal is None:
            return

        d_kline = self.klines.get(f'{vt_symbol}_D1', None)  # 日线
        m30_kline = self.klines.get(f'{vt_symbol}_M30', None)  # 30分钟线
        m1_kline = self.klines.get(f'{vt_symbol}_M1', None)  # 1分钟线
        if any([d_kline is None, m30_kline is None, m1_kline is None]):
            # self.write_log(f'{vt_symbol} K线数据还未就绪')
            return
        cn_name = self.cta_engine.get_name(vt_symbol)
        long_exit = False
        # 突破高点 => break = True
        if not signal.get('break', False):
            # 还没突破高点时
            d1_3rd_bi_high = signal.get('d1_3rd_bi_high', None)
            if d1_3rd_bi_high and m1_kline.cur_price > d1_3rd_bi_high:
                signal.update({'break': True})

        else:
            # 离场条件1：日线均线死叉离场
            if d_kline.ma12_count < 0 and d_kline.cur_bi.direction == 1 and not d_kline.cur_fenxing.is_rt\
                    and d_kline.cur_bi.high < d_kline.line_ma1[-1]:
                self.write_log(f'{vt_symbol}[{cn_name}] {d_kline.name}均线死叉离场')
                long_exit = True

            # 离场条件2： 30分钟的三卖信号
            ret, n = self.is_3rd_signal(kline=m30_kline, direction=Direction.SHORT)
            if ret and check_bi_not_rt(m30_kline, Direction.LONG):
                self.write_log(f'{vt_symbol}[{cn_name}] 30分钟三卖信号离场')
                long_exit = True

            # 跟随线段提高止盈线
            if check_bi_not_rt(d_kline, Direction.LONG) and duan_bi_is_end(d_kline.cur_duan, Direction.LONG):
                if "stop_price" in signal and signal['stop_price'] < float(d_kline.cur_bi.low):
                    self.write_log(f'{vt_symbol}[{cn_name}]跟随提高止损线 =>{d_kline.cur_bi.low}')
                    signal['stop_price'] = float(d_kline.cur_bi.low)

            # 击中了止盈、止损线
            if "stop_price" in signal and m1_kline.close_array[-1] < signal['stop_price']:
                self.write_log(f'{vt_symbol}[{cn_name}] 跟随止损信号离场')
                long_exit = True

            # if m30_kline.is_fx_macd_divergence(direction=Direction.LONG, cur_duan=m30_kline.cur_duan):
            #     self.write_log(f'{vt_symbol}[{cn_name}] 30分钟顶背离信号离场')
            #     long_exit = True
            #
            # # 如何时候都判断是否1分钟三卖信号
            # if self.is_3rd_signal(kline=m1_kline, direction=Direction.SHORT):
            #     self.write_log(f'{vt_symbol}[{cn_name}] 1分钟三卖信号离场')
            #     long_exit = True

        if long_exit:
            d = {
                "datetime": self.cur_datetime,
                "price": m1_kline.cur_price,
                "operation": '[{}]多单离场'.format(signal['cn_name']),
                "signal": f'{vt_symbol}.long'
            }
            self.save_dist(d)

            # 移除信号
            self.write_log(f'{vt_symbol}[{cn_name}的多头信号离场')
            signal.update({'status': TNS_STATUS_CLOSED,
                           'opened': False,
                           'close_date': self.cur_datetime.strftime('%Y-%m-%d')})
            self.policy.signals.update({vt_symbol: signal})
            self.policy.save()

            changed = False
            for grid in self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[vt_symbol]):
                # 更新为卖出状态
                if grid.open_status and not grid.order_status and not grid.close_status:
                    grid.close_price = m1_kline.cur_price
                    grid.traded_volume = 0
                    grid.close_status = True
                    self.write_log(f'添加卖出{vt_symbol}[{cn_name}],数量:{grid.volume},'
                                   f'开仓价格:{grid.open_price},平仓价格:{grid.close_price}')
                    changed = True

            if changed:
                self.gt.save()

    def on_bar_k(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        pass

    def on_bar_m30(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        pass

    def on_bar_d1(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        bar = args[0] if len(args) > 0 else kwargs.get('bar', None)
        if bar is None:
            return

        if bar.vt_symbol not in self.policy.signals:
            self.tns_discover_d1_3rd(bar.vt_symbol)

    def tns_calcute_pos(self):
        """事务计算仓位，并开仓"""
        if not self.trading or self.entrust != 0:
            return

        # 获取所有未开仓的做多信号
        pending_signals = [signal for signal in self.policy.signals.values() if
                           signal['last_signal'] == 'long' \
                           and signal['status'] == TNS_STATUS_OPENED \
                           and not signal.get('opened')]
        # 根据收益率进行排序
        self.pending_signals = sorted(pending_signals, key=lambda s: s['profit_rate'], reverse=True)

        # long_klines => pos => diff
        # 确保优先从赚钱的信号执行
        for long_signal in self.pending_signals:

            vt_symbol = long_signal['vt_symbol']

            # 获取事务信号
            signal = self.policy.signals.get(vt_symbol, {})

            # 当前价格
            cur_price = self.cta_engine.get_price(vt_symbol)
            if not cur_price:
                continue
            cn_name = self.cta_engine.get_name(vt_symbol)

            # 新增仓位

            # 存在已经开仓
            opened_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[vt_symbol])
            if len(opened_grids) > 0:
                signal.update({"opened": True})
                self.policy.signals.update({vt_symbol: signal})
                self.policy.save()
                continue

            # 正在委托
            grids = self.gt.get_grids_with_types(direction=Direction.LONG, types=[vt_symbol])
            if len(grids) > 0:
                continue

            # 判断止损价，避免开仓即止损
            stop_price = signal.get('stop_price', None)
            if stop_price is None or stop_price > cur_price:
                continue

            # 账号资金
            balance, available, _, _ = self.cta_engine.get_account()
            # 策略当前已经买入股票资金累计
            strategy_cur_margin = sum([p.price * p.volume for p in self.positions.values()])
            # 策略总投入资金
            max_invest_margin = balance * self.max_invest_rate
            if self.max_invest_margin > 0:
                self.write_log(f'策略实例使用限定最高资金投入: {self.max_invest_margin}')
                max_invest_margin = max(max_invest_margin, self.max_invest_margin)

            # 单信号资金
            if self.max_single_margin == 0:
                single_margin = max_invest_margin / min(self.share_symbol_count, len(self.vt_symbols))
            else:
                single_margin = self.max_single_margin

            # 还有空余资金
            if max_invest_margin - strategy_cur_margin > single_margin / 2:
                # 最小成交单位
                volume_tick = self.cta_engine.get_volume_tick(vt_symbol)
                # 取整
                buy_volume = int((min(max_invest_margin - strategy_cur_margin,
                                      single_margin) / cur_price) / volume_tick) * volume_tick
                self.write_log(
                    f'策略资金：{max_invest_margin}，已使用{strategy_cur_margin}，{vt_symbol}[{cn_name}]投入资金:{single_margin}')
                if buy_volume < volume_tick:
                    self.write_error(f'{vt_symbol}可购买{cn_name}数量低于最小下单量{volume_tick}')
                    continue

                grid = CtaGrid(direction=Direction.LONG,
                               vt_symbol=vt_symbol,
                               open_price=round(cur_price * 1.1, 3),
                               close_price=sys.maxsize,
                               stop_price=stop_price,
                               volume=buy_volume,
                               snapshot={'name': cn_name},
                               type=vt_symbol)

                grid.order_status = True
                self.gt.dn_grids.append(grid)
                self.write_log(
                    f'添加三买信号买入{vt_symbol}[{cn_name}],数量:{grid.volume},当前价{cur_price}，委托价格:{grid.open_price}')
                self.gt.save()

    def tns_calcute_profit(self):
        """
        计算事务中所有信号的收益统计
        :return:
        """
        if not self.backtesting:
            self.write_log(f'扫描所有开仓的信号收益统计')

        # 只有持有仓位的股票数量，超过共享仓位，才进行收益调整。例如20只票里面，超过16只
        if len([p.volume for p in self.positions.values() if p.volume > 0]) < self.share_symbol_count * 0.8:
            return

        # 一定要有未开仓的票
        if len(self.pending_signals) > 0:
            pending_avg_profit = sum([s.get("profit_rate", 0) for s in self.pending_signals]) / len(
                self.pending_signals)
            pending_avg_profit = round(pending_avg_profit, 2)
            if not self.backtesting:
                self.write_log(f'未开仓信号收益率:{pending_avg_profit}')
        else:
            return

            # pending_avg_profit = -0.05
            # if not self.backtesting:
            #     self.write_log(f'无开仓信号，假设收益率:{pending_avg_profit}')

        for grid in self.gt.get_opened_grids(direction=Direction.LONG):

            if grid.close_status:
                self.write_log(f'{grid.vt_symbol}处于平仓状态，不检查收益率')
                continue

            if grid.order_status:
                self.write_log(f'{grid.vt_symbol}处于委托状态，不检查收益率')
                continue

            # 当前价格
            cur_price = self.cta_engine.get_price(grid.vt_symbol)
            m30_kline_name = f'{grid.vt_symbol}_{self.bar_02_name}'
            # 取30分钟K线
            kline = self.klines.get(m30_kline_name, None)

            if not kline:
                self.write_error(f'无法获取{m30_kline_name}的K线')
                continue

            if cur_price is None:
                cur_price = kline.cur_price

            # 当前收益率
            profit_rate = (cur_price - grid.open_price) / grid.open_price
            self.write_log(f'{grid.vt_symbol}[{self.cta_engine.get_name(grid.vt_symbol)}]'
                           f'开仓价:{grid.open_price},当前价格:{cur_price},收益率:{profit_rate}')
            if isinstance(grid.open_time, datetime):
                open_date = grid.open_time.strftime('%Y-%m-%d')
            elif isinstance(grid.open_time, str) and len(grid.open_time) > 10:
                open_date = grid.open_time[:10]
            else:
                open_date = ""
            if self.cur_datetime.strftime('%Y-%m-%d') > open_date:

                # 获取信号
                signal = self.policy.signals.get(grid.vt_symbol, None)
                if not signal:
                    self.write_error(f'无法获取policy中{grid.vt_symbol}的信号')

                # 低于平均收益率,且在均线1的下方
                if profit_rate < pending_avg_profit and len(kline.line_ma1) > 0 and cur_price < kline.line_ma1[-1]:
                    cn_name = self.cta_engine.get_name(grid.vt_symbol)
                    self.write_log(
                        f'{grid.vt_symbol}[{cn_name}] 收益率:{profit_rate}低于未开仓的平均收益率{pending_avg_profit}, 将主动离场')

                    # 更新为卖出状态。由execute_sell_grid执行卖出处理
                    grid.close_status = True
                    self.write_log(f'强制移除{grid.vt_symbol}')
                    self.policy.signals.pop(grid.vt_symbol, None)

            else:
                if not self.backtesting:
                    self.write_log(f'当天交易股票不检查')

    def tns_get_grid(self, vt_symbol, close_volume):
        """根据需要平仓的volume，选取/创建出一个grid"""

        opened_grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and
                        g.open_status and not g.close_status and not g.order_status]

        if len(opened_grids) == 0:
            self.write_error(f'当前没有{vt_symbol}得网格')
            return None

        select_grid = None
        remove_gids = []

        for g in opened_grids:
            if select_grid is None:
                select_grid = g
                # 恰好等于需要close的数量
                if round(select_grid.volume, 7) == close_volume:
                    self.write_log(u'选中首个网格，仓位:{}'.format(close_volume))
                    break
                # volume 大于需要close的数量
                if select_grid.volume > close_volume:
                    remain_volume = select_grid.volume - close_volume
                    remain_volume = round(remain_volume, 7)
                    select_grid.volume = close_volume
                    remain_grid = copy(select_grid)
                    remain_grid.id = str(uuid.uuid1())
                    remain_grid.volume = remain_volume
                    self.gt.dn_grids.append(remain_grid)
                    self.write_log(u'选择首个网格，仓位超出，创建新的剩余网格:{}'.format(remain_volume))
                    break
            else:
                # 如果
                if select_grid.volume + g.volume <= close_volume:
                    old_volume = select_grid.volume
                    select_grid.volume += g.volume
                    select_grid.volume = round(select_grid.volume, 7)

                    g.volume = 0
                    remove_gids.append(g.id)
                    self.write_log(u'close_volume: {} => {}，需要移除:{}'
                                   .format(old_volume, select_grid.volume, g.__dict__))
                    if select_grid.volume == close_volume:
                        break
                elif select_grid.volume + g.volume > close_volume:
                    g.volume -= (close_volume - select_grid.volume)
                    select_grid.volume = close_volume
                    self.write_log(u'close_volume已满足')
                    break

        if select_grid is None:
            self.write_error(f'没有可选择的{vt_symbol}网格')
            return None

        if round(select_grid.volume, 7) != close_volume:
            self.write_error(u'没有可满足数量{}的{}单网格'.format(close_volume, vt_symbol))
            return None

        self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_gids)

        return select_grid

    def display_tns(self):
        if not self.inited:
            return
        if self.backtesting:
            return

        total_signals = len(self.policy.signals.keys())
        total_symbols = len(self.vt_symbols)
        total_klines = len(self.klines)
        self.write_log(f'股票总数：{total_symbols},K线总数：{total_klines}, 信号总数:{total_signals}')

        observate_signals = ['{}[{}]'.format(s.get('vt_symbol'), s.get('cn_name')) for s in self.policy.signals.values()
                             if s.get('status') == TNS_STATUS_OBSERVATE]
        ready_signals = ['{}[{}]'.format(s.get('vt_symbol'), s.get('cn_name')) for s in self.policy.signals.values() if
                         s.get('status') == TNS_STATUS_READY]
        ordering_signals = []
        opened_signals = []
        for s in self.policy.signals.values():
            # 正在委托开仓
            if s.get('status') == TNS_STATUS_ORDERING:
                vt_symbol = s.get('vt_symbol')
                open_price = s.get('open_price')
                cur_price = self.cta_engine.get_price(vt_symbol)
                if open_price and cur_price:
                    profit = round(100 * (cur_price - open_price) / open_price, 3)
                else:
                    profit = 0
                ordering_signals.append('{}[{}]:{}%'.format(vt_symbol, s.get('cn_name'), profit))

            elif s.get('status') == TNS_STATUS_OPENED:
                vt_symbol = s.get('vt_symbol')
                open_price = s.get('open_price')
                cur_price = self.cta_engine.get_price(vt_symbol)
                if open_price and cur_price:
                    profit = round(100 * (cur_price - open_price) / open_price, 3)
                else:
                    profit = 0
                opened_signals.append('{}[{}]:{}%'.format(vt_symbol, s.get('cn_name'), profit))

        self.write_log('观测:{}'.format(';'.join(observate_signals)))
        self.write_log('就绪:{}'.format(';'.join(ready_signals)))
        self.write_log('准备:{}'.format(';'.join(ordering_signals)))
        self.write_log('持仓:{}'.format(';'.join(opened_signals)))


class GroupPolicy(CtaPolicy):
    """组合策略事务"""

    def __init__(self, strategy):
        super().__init__(strategy)

        self.signals = {}  # kline_name: { 'last_signal': '', 'last_signal_time': datetime }

        self.long_klines = []  # 做多信号得kline.name list
        self.short_klines = []  # 做空信号得kline.name list

        self.last_long = {}  # kline_name: signal_count

    def to_json(self):
        """
        将数据转换成dict
        :return:
        """
        j = dict()
        j['create_time'] = self.create_time.strftime(
            '%Y-%m-%d %H:%M:%S') if self.create_time is not None else ""
        j['save_time'] = self.save_time.strftime('%Y-%m-%d %H:%M:%S') if self.save_time is not None else ""

        d = {}
        for kline_name, signal in self.signals.items():
            save_signal = deepcopy(signal)

            last_signal_time = save_signal.get('last_signal_time', None)

            if isinstance(last_signal_time, datetime):
                save_signal.update({"last_signal_time": last_signal_time.strftime(
                    '%Y-%m-%d %H:%M:%S')})
            elif last_signal_time is None:
                save_signal.update({"last_signal_time": ""})

            d.update({kline_name: save_signal})
        j['signals'] = d

        j['long_klines'] = self.long_klines
        j['short_klines'] = self.short_klines

        j['last_long'] = self.last_long

        return j

    def from_json(self, json_data):
        """
        将dict转化为属性
        :param json_data:
        :return:
        """
        if not isinstance(json_data, dict):
            return

        if 'create_time' in json_data:
            try:
                if len(json_data['create_time']) > 0:
                    self.create_time = datetime.strptime(json_data['create_time'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.create_time = datetime.now()
            except Exception as ex:
                self.create_time = datetime.now()

        if 'save_time' in json_data:
            try:
                if len(json_data['save_time']) > 0:
                    self.save_time = datetime.strptime(json_data['save_time'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.save_time = datetime.now()
            except Exception as ex:
                self.save_time = datetime.now()

        signals = json_data.get('signals', {})
        for kline_name, signal in signals.items():
            last_signal = signal.get('last_signal', "")
            str_last_signal_time = signal.get('last_signal_time', "")
            last_signal_time = None
            try:
                if len(str_last_signal_time) > 0:
                    last_signal_time = datetime.strptime(str_last_signal_time, '%Y-%m-%d %H:%M:%S')
                else:
                    last_signal_time = None
            except Exception as ex:
                last_signal_time = None
            signal.update({'last_signal_time': last_signal_time})
            self.signals.update({kline_name: signal})

        self.long_klines = json_data.get('long_klines', [])
        self.short_klines = json_data.get('short_klines', [])
        self.last_long = json_data.get('last_long', {})

    def clean(self):
        """
        清空数据
        :return:
        """
        self.write_log(u'清空policy数据')
        self.signals = {}
        self.long_klines = []
        self.short_klines = []
        self.last_long = {}
