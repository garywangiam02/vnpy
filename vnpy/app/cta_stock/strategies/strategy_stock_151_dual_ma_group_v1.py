# encoding: UTF-8

# 首先写系统内置模块
import sys
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
from vnpy.trader.object import HistoryRequest
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_grid_trade import (
    CtaGridTrade,
    CtaGrid
)

from vnpy.component.cta_policy import (
    TNS_STATUS_OPENED,
    TNS_STATUS_ORDERING,
    TNS_STATUS_OBSERVATE,
    TNS_STATUS_READY
)

from vnpy.component.cta_line_bar import (
    Interval,
    CtaMinuteBar,
    get_cta_bar_type)
from vnpy.component.cta_utility import ChanSignals, check_bi_not_rt, check_chan_xt
from vnpy.data.eastmoney.em_stock_data import EastMoneyData
from vnpy.data.stock.adjust_factor import get_all_adjust_factor
from vnpy.data.tdx.tdx_stock_data import TdxStockData
from vnpy.trader.utility import get_underlying_symbol, extract_vt_symbol, get_trading_date, load_json


########################################################################
class StrategyStockDualMaGroupV1(CtaStockTemplate):
    """CTA 双均线 组合竞争仓位策略
    思路：
        - 服务于大周期的选股策略，如日线级别的类二买，也可以单独多标的股票运行
        - 演变：1）日线类二买点 => 下跌继续（止损离场）; 2）=> 日线上涨中枢; 3） => 日线上涨趋势；
        - 使用15/30分钟的双均线，捕获2、3类演变的利润
    v1版本：
         根据缠论形态，判断反转形态后的均线金叉信号；
         根据缠论形态判断趋势突破后的均线金叉信号

        持仓期间，进行若干止损价格提升保护，如开仓价格保本提损、新中枢提损；
        持仓期间，对反向信号出现并确认时，提升止损价
        出现做空信号时，主动离场

    """
    author = u'大佳'

    share_symbol_count = 20  # 共享资金池得股票信号数量
    # 输入参数 [ 快均线长度_慢均线长度_信号线长度_K线周期]
    bar_names = ['f55_s89_n9_M30']
    vt_symbols_file = "vt_symbols.json"  # 采用json文件格式得vt_symbols，
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

    # 策略在外部设置的参数
    parameters = ["max_invest_margin",
                  "max_invest_rate",
                  "max_single_margin",
                  "bar_names",
                  "vt_symbols_file",
                  "share_symbol_count",
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

        # 策略逻辑，处理各股票的进场、离场逻辑持久化
        self.policy = GroupPolicy(strategy=self)

        # 各股票的仓位状态
        self.positions = {}

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        self.display_bars = False
        self.pending_signals = []      # 未开仓的signal
        self.kline_inited_dict = {}    # 已经完成初始化数据的k线
        self.vt_symbol_kline_map = {}  # 合约与K线得映射关系 vt_symbol: [kline_name1, kline_name2]
        # 每只股票的tick分钟
        self.minute_dict = {}

        # 东财数据源
        self.em_stock_data = None

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 配置文件 => self.vt_symbols
            self.tns_sync_from_json()

            # 回测时输出K线
            self.export_klines()

        if self.backtesting:
            # 回测时,自动初始化
            self.on_init()

    def create_klines(self, vt_symbol):
        """创建K线"""
        for bar_name in self.bar_names:
            # 创建K线
            kline_setting = {}
            # 分解参数
            para_fast_len, para_slow_len, para_signal_len, name = bar_name.split('_')

            kline_class, interval_num = get_cta_bar_type(name)

            kline_setting['name'] = f'{vt_symbol}_{bar_name}'

            para_fast_len = int(para_fast_len.replace('f', ''))
            para_slow_len = int(para_slow_len.replace('s', ''))
            para_signal_len = int(para_signal_len.replace('n', ''))  # 暂时不使用

            kline_setting['bar_interval'] = interval_num  # K线的Bar时长
            kline_setting['para_ma1_len'] = para_fast_len  # 第1条均线
            kline_setting['para_ma2_len'] = para_slow_len  # 第2条均线

            kline_setting['para_macd_fast_len'] = 12  # 激活macd，供趋势背驰的一些判断
            kline_setting['para_macd_slow_len'] = 26
            kline_setting['para_macd_signal_len'] = 9

            kline_setting['para_active_chanlun'] = True  # 激活缠论
            kline_setting['para_active_chan_xt'] = True  # 激活缠论的形态分析

            kline_setting['price_tick'] = self.cta_engine.get_price_tick(vt_symbol)
            kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol.split('.')[0]).upper()
            kline_setting['is_stock'] = True
            self.write_log(f'创建K线:{kline_setting}')
            # 创建K线
            kline = kline_class(self, self.on_bar_k, kline_setting)
            # 添加到klines中
            self.klines.update({kline.name: kline})
            # 更新股票与k线得映射关系
            vt_symbol_klines = self.vt_symbol_kline_map.get(vt_symbol, [])
            vt_symbol_klines.append(kline.name)
            self.vt_symbol_kline_map[vt_symbol] = vt_symbol_klines
            # 设置当前K线状态为未初始化数据
            self.kline_inited_dict[kline.name] = self.backtesting

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
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                {'name': f'upper', 'source': 'line_bar', 'attr': 'line_macd_chn_upper', 'type_': 'list'},
                {'name': f'lower', 'source': 'line_bar', 'attr': 'line_macd_chn_lower', 'type_': 'list'},
                {'name': 'atr', 'source': 'line_bar', 'attr': 'line_atr1', 'type_': 'list'},
                {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},
                {'name': f'cci', 'source': 'line_bar', 'attr': 'line_cci', 'type_': 'list'},
            ]

            kline.export_bi_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_bi.csv'.format(self.strategy_name, kline_name)))

            kline.export_zs_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_zs.csv'.format(self.strategy_name, kline_name)))

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
            # 这里是使用gateway历史数据
            for vt_symbol in self.vt_symbols:
                self.init_kline_data(vt_symbol)

        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

    def tns_sync_from_json(self):
        """
        从指定vt_symbol配置文件中获取所有纳入观测和交易得vt_symbols
        :return:
        """
        if self.backtesting:
            for vt_symbol in self.vt_symbols:
                # 创建K线
                self.create_klines(vt_symbol)
            return
        try:
            # 读取源json文件 => {}
            vt_symbols = load_json(self.vt_symbols_file, auto_save=False)
            # gary issue
            self.vt_symbols=[]
            for d in vt_symbols["vt_symbols"]:
                vt_symbol = d.get('vt_symbol')
                if not vt_symbol:
                    continue
                # 转换交易所，针对ricequant过来的选股结果
                vt_symbol = vt_symbol.replace('XSHE', 'SZSE').replace('XSHG', 'SSE')

                if vt_symbol in self.vt_symbols:
                    continue
                # 股票不被激活，将不纳入
                if not d.get('active', False):
                    continue

                # 添加到策略配置中
                self.vt_symbols.append(vt_symbol)

                # 创建K线
                self.create_klines(vt_symbol)

                # 已经初始化过了，说明该合约是策略启动后被更新进来得
                if self.inited:
                    self.init_kline_data(vt_symbol)

        except Exception as ex:
            self.write_error(f'{self.strategy_name}读取{self.vt_symbols_file}发生异常:{str(ex)}')
            return False

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
                # 找到K线，且该K线已经inited
                if kline and self.kline_inited_dict.get(kline_name, False):
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

                self.tns_execute_buy_grids(vt_symbol)

                self.tns_excute_sell_grids(vt_symbol)

            except Exception as ex:
                self.write_error(u'[on_bar] 异常 {},{}'.format(str(ex), traceback.format_exc()))

            if is_new_minute:
                self.tns_open_logic(vt_symbol)
                self.tns_close_logic(vt_symbol)
                self.tns_calcute_pos()

            # 每6分钟检查一次
            if self.cur_datetime.minute % 6 == 0:
                self.tns_calcute_profit()

    def fit_zs_signal(self, kline, strict=False):
        """
        判断当前K线，是否处于两个连续的下跌中枢
        :param kline:
        :param stict: 严格：True 两个中枢不存在中枢扩展；False，两个中枢
        :return:
        """
        if len(kline.bi_zs_list) < 2:
            return False

        if not strict and kline.bi_zs_list[-2].high > kline.bi_zs_list[-1].high:
            return True

        # 倒2中枢底部 > 倒1中枢顶部
        if kline.bi_zs_list[-2].low > kline.bi_zs_list[-1].high:
            # 倒2中枢的低点
            min_low = min([bi.low for bi in kline.bi_zs_list[-2].bi_list])
            # 倒1中枢的高点
            max_high = max([bi.high for bi in kline.bi_zs_list[-1].bi_list])
            if min_low > max_high:
                return True

        return False

    def fit_xt_signal(self, kline, signal_values):
        """
        判断是否满足缠论心态信号要求
        在倒5笔内，出现信号.如类一买点
        :param kline:
        :param signal_values: [ChanSignals的value]
        :return:
        """
        # 检查9~13笔的信号
        for n in [9, 11, 13]:
            # 倒5~倒1的信号value清单
            all_signals = [s.get('signal') for s in getattr(kline,f'xt_{n}_signals',[])[-6:-1]]
            # 判断是否有类一买点信号存在
            for signal_value in signal_values:
                if signal_value in all_signals:
                    return True

        return False

    def tns_open_logic(self, vt_symbol):
        """
        开仓逻辑
        :param vt_symbol: 股票合约代码
        :return:
        """
        if self.entrust != 0:
            return

        # 调试专用
        if self.cur_datetime.strftime("%Y-%m-%d") in ['2014-11-06', '2017-10-14', '2017-12-05']:
            a = 1

        # 属于vt_symbol的所有K线
        for kline_name in self.vt_symbol_kline_map.get(vt_symbol, []):
            # 找到K线
            kline = self.klines.get(kline_name)

            # 要求至少有三个线段
            if kline.tre_duan is None:
                continue

            # kline_name => 信号事务
            signal = self.policy.signals.get(kline_name, {})

            # 做多事务
            if signal.get('last_signal', '') != 'long':

                # 反转信号1：两个以上下跌笔中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
                if self.fit_zs_signal(kline) \
                        and kline.cur_duan.direction == 1\
                        and len(kline.cur_duan.bi_list) == 1 \
                        and kline.cur_duan.low < kline.cur_bi_zs.low < kline.cur_duan.high\
                        and check_bi_not_rt(kline,direction=Direction.SHORT)\
                        and kline.cur_duan.end == kline.cur_bi.start\
                        and kline.ma12_count > 0:

                    min_low = kline.cur_duan.low

                    signal = {'last_signal': 'long',
                              'last_signal_time': self.cur_datetime,
                              'signal_name': '趋势反转',
                              'kline_name': kline_name,
                              'stop_price': float(min_low),
                              'open_price': kline.cur_price,
                              'init_price': kline.cur_price,
                              'bi_start': kline.bi_list[-1].start,
                              'opened': False,
                              'profit_rate': 0}

                    self.policy.signals[kline_name] = signal
                    self.policy.save()

                    if kline_name in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                        self.policy.short_klines.remove(kline_name)

                    self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                    self.policy.long_klines.insert(0, kline_name)
                    d = {
                        "datetime": self.cur_datetime,
                        "price": kline.cur_price,
                        "operation": '趋势反转后金叉信号',
                        "signal": f'{kline.name}.long',
                        "stop_price": min_low
                    }
                    self.save_dist(d)
                    continue


                # 反转信号2：倒6~倒1笔内出现类一买点，然后均线金叉+下跌分笔
                if self.fit_xt_signal(kline, [ ChanSignals.Q1L0.value]):

                    # 防止出现瞬间跌破一类买点
                    if kline.cur_duan.low == kline.cur_bi.low:
                        continue

                    # 均线金叉，下跌分笔
                    if kline.ma12_count > 0 and check_bi_not_rt(kline, direction=Direction.SHORT):

                        min_low = min([bi.low for bi in kline.bi_list[-5:]])

                        signal = {'last_signal': 'long',
                                  'last_signal_time': self.cur_datetime,
                                  'signal_name': '类一买点',
                                  'kline_name': kline_name,
                                  'stop_price': float(min_low),
                                  'open_price': kline.cur_price,
                                  'init_price': kline.cur_price,
                                  'bi_start': kline.bi_list[-1].start,
                                  'opened': False,
                                  'profit_rate': 0}

                        self.policy.signals[kline_name] = signal
                        self.policy.save()

                        if kline_name in self.policy.short_klines:
                            self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                            self.policy.short_klines.remove(kline_name)

                        self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                        self.policy.long_klines.insert(0, kline_name)
                        d = {
                            "datetime": self.cur_datetime,
                            "price": kline.cur_price,
                            "operation": '类一买点后金叉信号',
                            "signal": f'{kline.name}.long',
                            "stop_price": min_low
                        }
                        self.save_dist(d)
                        continue

                # 进攻信号1：
                xt_5_signal = kline.get_xt_signal(xt_name='xt_5_signals', x=1)
                # 倒1笔出现三类买点形态，然后才出现金叉
                if xt_5_signal.get('signal') == ChanSignals.LI0.value\
                    and kline.pre_duan.height > kline.cur_duan.height \
                    and kline.pre_duan.direction == -1\
                    and kline.cur_bi.direction == 1\
                    and kline.ma12_count > 0:

                    # 三类买点的时间，在金叉之前
                    if xt_5_signal.get('end', "") < kline.ma12_cross_list[-1].get('datetime', ""):
                        zs_high = kline.bi_list[-4].high

                        signal = {'last_signal': 'long',
                                  'last_signal_time': self.cur_datetime,
                                  'signal_name': '三类形态买点',
                                  'kline_name': kline_name,
                                  'stop_price': float(zs_high),
                                  'open_price': kline.cur_price,
                                  'init_price': kline.cur_price,
                                  'bi_start': kline.bi_list[-1].start,
                                  'opened': False,
                                  'profit_rate': 0}

                        self.policy.signals[kline_name] = signal
                        self.policy.save()

                        if kline_name in self.policy.short_klines:
                            self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                            self.policy.short_klines.remove(kline_name)

                        self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                        self.policy.long_klines.insert(0, kline_name)
                        d = {
                            "datetime": self.cur_datetime,
                            "price": kline.cur_price,
                            "operation": '三类买点后金叉信号',
                            "signal": f'{kline.name}.long',
                            "stop_price": zs_high
                        }
                        self.save_dist(d)

                        continue

                # 进攻信号2：线段级别，形成扩张中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
                if kline.tre_duan.height < kline.pre_duan.height < kline.cur_duan.height \
                        and kline.cur_duan.direction == 1 \
                        and len(kline.cur_duan.bi_list) == 1 \
                        and kline.cur_duan.low < kline.cur_bi_zs.low < kline.cur_duan.high \
                        and check_bi_not_rt(kline, direction=Direction.SHORT) \
                        and kline.cur_duan.end == kline.cur_bi.start \
                        and kline.ma12_count > 0:

                    min_low = kline.cur_duan.low

                    signal = {'last_signal': 'long',
                              'last_signal_time': self.cur_datetime,
                              'signal_name': '线段扩张中枢金叉',
                              'kline_name': kline_name,
                              'stop_price': float(min_low),
                              'open_price': kline.cur_price,
                              'init_price': kline.cur_price,
                              'bi_start': kline.bi_list[-1].start,
                              'opened': False,
                              'profit_rate': 0}

                    self.policy.signals[kline_name] = signal
                    self.policy.save()

                    if kline_name in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                        self.policy.short_klines.remove(kline_name)

                    self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                    self.policy.long_klines.insert(0, kline_name)
                    d = {
                        "datetime": self.cur_datetime,
                        "price": kline.cur_price,
                        "operation": '线段扩张中枢金叉信号',
                        "signal": f'{kline.name}.long',
                        "stop_price": min_low
                    }
                    self.save_dist(d)
                    continue

    def tns_close_logic(self, vt_symbol):
        """
        主动离场逻辑
        主要应对开仓后，就进入震荡中枢状态
        :return:
        """

        #  属于vt_symbol的所有K线
        for kline_name in self.vt_symbol_kline_map.get(vt_symbol, []):
            # kline_name => kline
            kline = self.klines.get(kline_name)
            # kline_name => 事务
            signal = self.policy.signals.get(kline_name, None)
            if signal is None:
                continue

            last_signal = signal.get('last_signal', None)
            last_signal_time = signal.get('last_signal_time', None)
            # datetime => str
            if last_signal_time and isinstance(last_signal_time, datetime):
                last_signal_time = last_signal_time.strftime('%Y-%m-%d %H:%M:%S')
            signal_name = signal.get('signal_name', None)
            open_price = signal.get('open_price', None)
            init_price = signal.get('init_price', None)
            bi_start = signal.get('bi_start', None)
            stop_price = signal.get('stop_price', None)

            # 持有做多事务,进行更新
            if kline_name in self.policy.long_klines:
                # 更新收益率
                if init_price:
                    profit = kline.cur_price - init_price
                    profit_rate = round(profit / init_price, 4)
                    signal.update({'profit_rate': profit})

            # 保本提损：开仓后，有新的中枢产生，且价格比开仓价格高
            if stop_price and stop_price < open_price\
                and kline.cur_bi_zs.start > last_signal_time\
                and kline.cur_bi_zs.low > open_price:
                # 保本+一点点利润
                stop_price = open_price + (kline.cur_bi_zs.low - open_price) * 0.2
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '保本提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 中枢提损：如果止损价低于当前中枢底部，且当前上涨笔出现在中枢上方
            if stop_price and stop_price < float(kline.cur_bi_zs.low) \
                    and kline.cur_bi.low > kline.cur_bi_zs.high\
                    and kline.cur_bi.direction == 1\
                    and last_signal_time \
                    and kline.cur_bi_zs.start > last_signal_time:
                # 止损价，移动至当前中枢底部
                stop_price = float(kline.cur_bi_zs.low)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '中枢提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 下跌线段站稳提损：上涨过程中，出现回调线段，此时，向上一笔+死叉，提升止损
            if kline.cur_duan.direction == -1 \
                and kline.cur_duan.start > last_signal_time \
                and kline.cur_duan.low == kline.cur_bi.low \
                and check_bi_not_rt(kline, Direction.LONG) \
                and kline.ma12_count < 0 \
                and stop_price < float(kline.cur_bi.low):
                # 止损价，移动至当前笔的底部
                stop_price = float(kline.cur_bi.low)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '下跌线段站稳提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 三卖信号提损，实时出现三卖信号，立刻止损价提高到笔的下方
            if check_bi_not_rt(kline, Direction.LONG) \
                    and kline.get_xt_signal(xt_name='xt_5_signals').get('signal') == ChanSignals.SI0.value:
                if stop_price and stop_price < kline.cur_bi.low:
                    stop_price = float(kline.cur_bi.low)
                    signal.update({'stop_price': stop_price})
                    self.policy.signals.update({kline_name: signal})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": stop_price,
                        "operation": '三卖提损',
                        "signal": f'{kline.name}.long',
                        "stop_price": stop_price
                    }
                    self.save_dist(d)

            # 获取五笔形态的倒1笔的信号
            xt_5_signal = kline.get_xt_signal(xt_name='xt_5_signals', x=1)
            signal_leave = False
            stop_leave = False
            leave_type = None

            # 次级别的卖点判断，需要遵循本级别的买点判断
            # 使用线段判断是否存在三买,如果存在，则不离场
            duan_xt_leave = True
            if len(kline.duan_list) > 5:
                duan_xt_signal = check_chan_xt(kline,kline.duan_list[-5:])
                if duan_xt_signal in [ChanSignals.LI0.value]:
                    duan_xt_leave = False

            # 倒1笔为三类卖点形态，且死叉; 或者下破止损点
            if duan_xt_leave \
                    and xt_5_signal.get('signal') == ChanSignals.SI0.value \
                    and kline.ma12_count < 0 \
                    and xt_5_signal.get('start','') > last_signal_time:
                leave_type = '三卖+死叉'
                self.write_log(u'{} => {} 做多信号离场'.format(leave_type, kline_name))
                signal_leave = True

            if stop_price and stop_price > kline.cur_price:
                leave_type = '止损'
                self.write_log(u'{} => {} 做多信号离场'.format(leave_type, kline_name))
                stop_leave = True

            # 满足离场信号
            if signal_leave or stop_leave:

                # 移除事务
                self.policy.signals.pop(kline_name, None)

                # 移除做多信号
                if kline_name in self.policy.long_klines:
                    self.write_log(f'{kline_name} => 做多信号移除')
                    self.policy.long_klines.remove(kline_name)
                if kline_name not in self.policy.short_klines:
                    self.write_log(f'{kline_name} => 添加离场信号')
                    self.policy.short_klines.append(kline_name)

                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": leave_type,
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

                continue

    def on_bar_k(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        pass

    def tns_calcute_pos(self):
        """事务计算仓位"""
        if not self.trading or self.entrust != 0:
            return

        # 获取所有未开仓的做多信号
        pending_signals = [signal for signal in self.policy.signals.values() if
                           signal['last_signal'] == 'long' and not signal.get('opened', False)]
        # 根据收益率进行排序
        self.pending_signals = sorted(pending_signals, key=lambda s: s['profit_rate'], reverse=True)

        # long_klines => pos => diff
        # 确保优先从赚钱的信号执行
        for long_signal in self.pending_signals:

            kline_name = long_signal['kline_name']

            # 获取事务信号
            signal = self.policy.signals.get(kline_name, {})

            # 比对policy中记录得累计数量是否一致
            if self.policy.last_long.get(kline_name, 0) > 0:
                continue

            vt_symbol = kline_name.split('_')[0]
            # 当前价格
            cur_price = self.cta_engine.get_price(vt_symbol)
            if not cur_price:
                continue
            cn_name = self.cta_engine.get_name(vt_symbol)

            # 新增仓位

            # 存在已经开仓
            opened_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[kline_name])
            if len(opened_grids) > 0:
                self.policy.last_long[kline_name] = 1
                signal.update({"opened": True})
                self.policy.signals.update({kline_name: signal})
                self.policy.save()
                continue

            # 正在委托
            grids = self.gt.get_grids_with_types(direction=Direction.LONG, types=[kline_name])
            if len(grids) > 0:
                continue

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
                single_margin = max_invest_margin / min(self.share_symbol_count,
                                                        len(self.vt_symbols) * len(self.bar_names))
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
                    f'策略资金：{max_invest_margin}，已使用{strategy_cur_margin}，{kline_name}[{cn_name}]投入资金:{single_margin}')
                if buy_volume < volume_tick:
                    self.write_error(f'{kline_name}可购买{cn_name}数量低于最小下单量{volume_tick}')
                    continue

                grid = CtaGrid(direction=Direction.LONG,
                               vt_symbol=vt_symbol,
                               open_price=round(cur_price * 1.1, 3),
                               close_price=sys.maxsize,
                               stop_price=stop_price,
                               volume=buy_volume,
                               snapshot={'name': cn_name},
                               type=kline_name)

                grid.order_status = True
                self.gt.dn_grids.append(grid)
                self.write_log(
                    f'添加{kline_name}信号买入[{cn_name}],数量:{grid.volume},当前价{cur_price}，委托价格:{grid.open_price}')
                self.gt.save()

        # 减少、移除仓位
        for kline_name in self.policy.short_klines:
            vt_symbol = kline_name.split('_')[0]
            # 当前价格
            cur_price = self.cta_engine.get_price(vt_symbol)
            if not cur_price:
                continue
            cn_name = self.cta_engine.get_name(vt_symbol)

            # # 策略内当前股票得持仓
            cur_pos = self.get_position(vt_symbol=vt_symbol)

            # 存在已经开仓
            opened_grids = self.gt.get_opened_grids_within_types(direction=Direction.LONG, types=[kline_name])
            if len(opened_grids) > 0:
                # 逐一更换为平仓状态
                for grid in opened_grids:
                    if not grid.close_status:
                        grid.close_price = cur_price
                        grid.traded_volume = 0
                        grid.close_status = True
                        self.write_log(f'添加卖出{vt_symbol}[{cn_name}],数量:{grid.volume},'
                                       f'开仓价格:{grid.open_price},当前价格{cur_price},止盈价格:{grid.close_price}')

            else:

                if kline_name in self.policy.last_long:
                    self.write_log(f'{kline_name}已不存在仓位，成功移除信号记录')
                    self.policy.last_long.pop(kline_name, None)
                    self.policy.save()
                continue

    def tns_calcute_profit(self):
        """
        计算事务中所有信号的收益统计
        :return:
        """
        # 只有持有仓位的股票数量，超过共享仓位，才进行收益调整
        if len([p.volume for p in self.positions.values() if p.volume >0]) < self.share_symbol_count *  0.8:
            return

        if len(self.pending_signals) > 0:
            pending_avg_profit = sum([s.get("profit_rate", 0) for s in self.pending_signals]) / len(
                self.pending_signals)
        else:
            pending_avg_profit = -0.01

        for grid in self.gt.get_opened_grids(direction=Direction.LONG):

            if grid.close_status:
                continue

            if grid.order_status:
                continue

            # 当前价格
            cur_price = self.cta_engine.get_price(grid.vt_symbol)
            kline = self.klines.get(grid.type, None)
            if not kline:
                continue
            if isinstance(grid.open_time, datetime) \
                    and self.cur_datetime.strftime('%Y-%m-%d') > grid.open_time.strftime('%Y-%m-%d'):

                # 获取信号
                signal = self.policy.signals.get(grid.type, None)
                if signal:
                    # 当前收益率
                    profit_rate = (cur_price - grid.open_price) / grid.open_price
                    # 低于平均收益率,且在均线1的下方
                    if profit_rate < pending_avg_profit and kline.cur_price < kline.line_ma1[-1]:
                        # 标记为强制卖出
                        signal.update({'force_sell': True})
                        self.policy.signals.update({grid.type: signal})
                        cn_name = self.cta_engine.get_name(grid.vt_symbol)
                        self.write_log(
                            f'{grid.vt_symbol}[{cn_name}] 收益率:{profit_rate}低于未开仓的平均收益率{pending_avg_profit}, 将主动离场')

                        # 更新为卖出状态。由execute_sell_grid执行卖出处理
                        grid.close_status = True
                        if grid.type in self.policy.last_long:
                            self.write_log(f'强制移除{grid.type}上一交易计数器')
                            self.policy.last_long.pop(grid.type, None)

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
