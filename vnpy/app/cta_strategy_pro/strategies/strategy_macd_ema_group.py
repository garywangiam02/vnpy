# encoding: UTF-8

# 首先写系统内置模块
import sys
import os
from datetime import datetime, timedelta, time, date
import copy
import traceback
from collections import  OrderedDict

# 然后是自己编写的模块
from vnpy.trader.utility import round_to
from vnpy.app.cta_strategy_pro.template import CtaProFutureTemplate, Direction, get_underlying_symbol, Interval, TickData, BarData
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar
# from vnpy.data.huafu.data_source import DataSource
from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date


class GroupPolicy(CtaPolicy):
    """组合策略事务"""

    def __init__(self, strategy):
        super().__init__(strategy)

        self.signals = {}  # kline_name: { 'last_signal': '', 'last_signal_time': datetime }

        self.long_klines = []  # 做多信号得kline.name list
        self.short_klines = []  # 做空信号得kline.name list

        self.last_net_count = 0

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
            last_signal_time = signal.get('last_signal_time', None)
            d.update({kline_name:
                          {'last_signal': signal.get('last_signal', ''),
                           'last_signal_time': last_signal_time.strftime(
                               '%Y-%m-%d %H:%M:%S') if last_signal_time is not None else ""
                           }
                      })
        j['singlals'] = d

        j['long_klines'] = self.long_klines
        j['short_klines'] = self.short_klines

        j['last_net_count'] = self.last_net_count
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
        for kline_name, signal in signals:
            last_signal = signal.get('last_signal', "")
            str_ast_signal_time = signal.get('last_signal_time', "")
            last_signal_time = None
            try:
                if len(str_ast_signal_time) > 0:
                    last_signal_time = datetime.strptime(str_ast_signal_time, '%Y-%m-%d %H:%M:%S')
                else:
                    last_signal_time = None
            except Exception as ex:
                last_signal_time = None
            self.signals.update({kline_name: {'last_signal': last_signal, 'last_signal_time': last_signal_time}})

        self.long_klines = json_data.get('long_klines', [])
        self.short_klines = json_data.get('short_klines', [])
        self.last_net_count = json_data.get('last_net_count', 0)

    def clean(self):
        """
        清空数据
        :return:
        """
        self.write_log(u'清空policy数据')
        self.signals = {}
        self.long_klines = []
        self.short_klines = []
        self.last_net_count = 0


########################################################################
class StrategyMacdEmaGroup(CtaProFutureTemplate):
    """CTA MACD+EMA 组合轧差策略

    """
    author = u'李来佳'
    # 输入参数 [ MACD快均线长度_慢均线长度_EMA长度_K线周期]
    bar_names = ['f18_s60_ema100_M10', 'f18_s60_ema100_M15', 'f18_s120_ema120_M30']

    # 策略在外部设置的参数
    parameters = ["max_invest_pos", "max_invest_margin", "max_invest_rate",
                  "bar_names",
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

        # 创建一个策略规则
        self.policy = GroupPolicy(strategy=self)

        # 仓位状态
        self.position = CtaPosition(strategy=self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        self.kline_count = len(self.bar_names)
        self.net_kline_count = 0   # 净仓位

        self.init_past_3_4 = False  # 初始化经过2/3时间
        self.display_bars = False

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 更新监控的k线总数
            self.kline_count = len(self.bar_names)

            for bar_name in self.bar_names:
                # 创建K线
                kline_setting = {}
                para_fast_len, para_slow_len, para_ema_len, name = bar_name.split('_')
                kline_class, interval_num = get_cta_bar_type(name)
                kline_setting['name'] = bar_name

                para_fast_len = int(para_fast_len.replace('f', ''))
                para_slow_len = int(para_slow_len.replace('s', ''))
                para_ema_len = int(para_ema_len.replace('ema', ''))
                kline_setting['bar_interval'] = interval_num  # K线的Bar时长
                kline_setting['para_ma1_len'] = para_fast_len  # 第1条均线
                kline_setting['para_ma2_len'] = para_slow_len  # 第2条均线
                kline_setting['para_ema1_len'] = para_ema_len
                kline_setting['para_macd_fast_len'] = para_fast_len
                kline_setting['para_macd_slow_len'] = para_slow_len
                kline_setting['para_macd_signal_len'] = 20
                kline_setting['price_tick'] = self.price_tick
                kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol.split('.')[0]).upper()
                self.write_log(f'创建K线:{kline_setting}')
                kline = kline_class(self, self.on_bar_k, kline_setting)
                self.klines.update({bar_name: kline})

            #self.export_klines()

        if self.backtesting:
            # 回测时,自动初始化
            self.on_init()

        if self.backtesting:
            self.export_klines()

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
                {'name': 'dif', 'source': 'line_bar', 'attr': 'line_dif', 'type_': 'list'},
                {'name': 'dea', 'source': 'line_bar', 'attr': 'line_dea', 'type_': 'list'},
                {'name': 'macd', 'source': 'line_bar', 'attr': 'line_macd', 'type_': 'list'},
                {'name': f'ma{kline.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'},
                {'name': 'ema', 'source': 'line_bar', 'attr': 'line_ema1', 'type_': 'list'},
            ]

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
        self.load_policy()
        self.display_tns()

        if not self.backtesting:
            self.init_position()  # 初始持仓数据

        if not self.backtesting:
            # 这里是使用gateway历史数据
            if not self.init_data():
                self.write_error(u'初始数据失败')

        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

    def init_data(self):
        """初始化数据"""

        # 从数据源加载最新的 bar, 不足，则获取ticks
        try:
            dt_now = datetime.now()
            # 华富的期货数据源
            # ds = DataSource(timeout=180000)

            # 从本地缓存文件中加载K线，并取得最后的bar时间
            last_bar_dt = self.load_klines_from_cache()

            if isinstance(last_bar_dt, datetime):
                self.write_log(f'[初始化]缓存数据bar最后时间:{last_bar_dt}')
                self.cur_datetime = last_bar_dt
                start_date = (last_bar_dt - timedelta(days=1)).strftime('%Y-%m-%d')

            else:
                # 取 1分钟bar
                start_date = (datetime.now() - timedelta(days=200)).strftime('%Y-%m-%d')
                self.write_log(f'[初始化]无本地缓存文件，取90天1分钟数据')
            end_date = datetime.now().strftime('%Y-%m-%d')
            fields = ['open', 'close', 'high', 'low', 'volume', 'open_interest', 'limit_up', 'limit_down',
                      'trading_date']
            frequency = 1

            symbol, exchange = extract_vt_symbol(self.idx_symbol)
            self.write_log(u'ds.get_price(order_book_id={}, start_date={},  frequency={}m, fields={}'
                           .format(get_full_symbol(symbol).upper(), start_date, frequency, fields))
            order_book_id = '{}.{}'.format(get_full_symbol(symbol).upper(), exchange.value)
            df = ds.get_price(order_book_id=order_book_id, start_date=start_date,
                              end_date=end_date, frequency='{}m'.format(frequency), fields=fields)
            bar_len = len(df)
            self.write_log(f'[初始化]一共获取{bar_len}条{self.vt_symbol} {frequency}分钟数据')
            bar_count = 0
            bar_close_dt = None
            for idx in df.index:
                row = df.loc[idx]
                bar_close_dt = datetime.strptime(str(idx), '%Y-%m-%d %H:%M:00')
                if last_bar_dt is not None and bar_close_dt < last_bar_dt:
                    continue

                bar_start_dt = bar_close_dt - timedelta(minutes=frequency)
                self.cur_datetime = bar_close_dt
                bar = BarData(
                    gateway_name='huafu',
                    symbol=symbol,
                    exchange=exchange,
                    datetime=bar_start_dt,
                    trading_day=get_trading_date(bar_start_dt),
                    open_price=float(row['open']),
                    high_price=float(row['high']),
                    low_price=float(row['low']),
                    close_price=float(row['close']),
                    volume=int(row['volume'])
                )

                # 推送Bar到所有K线中
                self.cur_99_price = bar.close_price

                bar_count += 1
                if bar_count > int(bar_len * 3 / 4) and not self.init_past_3_4:
                    self.init_past_3_4 = True
                if bar_count >= bar_len - 10:
                    self.write_log(f'{bar.__dict__}')

                for kline_name, kline in self.klines.items():
                    if kline_name.endswith('M1'):
                        bar_complete = True
                    else:
                        bar_complete = False
                    kline.add_bar(bar, bar_is_completed=bar_complete)


        except Exception as e:
            self.write_error(f'[初始化] {self.strategy_name} 加载历史数据失败：\n {str(e)}\n{traceback.format_exc()}')
            return False

        return True

    # ----------------------------------------------------------------------
    def on_tick(self, tick: TickData):
        """行情更新
        :type tick: object
        """
        # 实盘检查是否初始化数据完毕。如果数据未初始化完毕，则不更新tick，避免影响cur_price
        if not self.backtesting:
            if not self.inited:
                self.write_log(u'[on_tick]数据还没初始化完毕，不更新tick')
                return

        # 更新所有tick dict（包括 指数/主力/历史持仓合约)
        self.tick_dict.update({tick.vt_symbol: tick})

        if tick.vt_symbol == self.vt_symbol:
            self.cur_mi_tick = tick
            self.cur_mi_price = tick.last_price

        else:
            # 所有非vt_symbol得tick，全部返回
            return

        # 更新策略执行的时间（用于回测时记录发生的时间）
        self.cur_datetime = tick.datetime
        self.cur_99_price = tick.last_price
        self.cur_99_tick = tick

        for kline in self.klines.values():
            kline.on_tick(copy.copy(tick))

        if not self.inited or not self.trading:
            return

        diff_seconds = (self.cur_99_tick.datetime - self.cur_mi_tick.datetime).total_seconds()
        if diff_seconds > 10 and self.cur_datetime.strftime('%H%M') not in ['1030', '1330']:
            self.write_error(
                u'指数tick时间:{} 与主力tick时间:{}发生超过{}秒得偏差'.format(self.cur_99_tick.datetime, self.cur_mi_tick.datetime,
                                                             diff_seconds))
            if self.cur_99_tick.datetime > self.cur_mi_tick.datetime:
                self.write_log(f'重新订阅合约:{self.vt_symbol}')
                self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=self.vt_symbol)
            return

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime, reopen=False)

        # 网格逐一止损/止盈检查
        self.grid_check_stop()

        # 实盘这里是每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute

            # 平仓逻辑处理
            self.tns_close_logic()
            # 开仓逻辑处理
            self.tns_open_logic()

            self.display_grids()
            self.display_tns()

            # 事务平衡仓位
            self.tns_calcute_net_pos()

            self.put_event()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """
        分钟K线数据（仅用于回测时，从策略外部调用)
        :param bar:
        :return:
        """

        if self.backtesting:
            new_dt = bar.datetime + timedelta(seconds=60)
            if self.cur_datetime and new_dt < self.cur_datetime:
                return
            self.cur_datetime = new_dt
            self.cur_99_price = bar.close_price
            self.cur_mi_price = bar.close_price

            if self.inited:

                # 执行撤单逻辑
                self.tns_cancel_logic(bar.datetime)

                # 网格逐一止损/止盈检查
                self.grid_check_stop()

        # 推送bar到所有K线
        try:
            for kline_name, kline in self.klines.items():
                if kline_name.endswith('M1'):
                    bar_complete = True
                else:
                    bar_complete = False
                kline.add_bar(bar=copy.copy(bar), bar_is_completed=bar_complete)

        except Exception as ex:
            self.write_error(u'[on_bar] 异常 {},{}'.format(str(ex), traceback.format_exc()))

        if self.inited and self.trading:

            # 平仓逻辑处理
            self.tns_close_logic()

            # 开仓逻辑处理
            self.tns_open_logic()

            # 事务平衡仓位
            self.tns_calcute_net_pos()

        # 显示各指标信息
        self.display_tns()

    def tns_open_logic(self):
        """
        开仓逻辑
        :return:
        """
        if self.entrust != 0:
            return

        for kline_name in list(self.klines.keys()):
            kline = self.klines.get(kline_name)
            # 前置检查,分钟K线均线长度,EMA不够
            if len(kline.line_ma1) <= 2 \
                    or len(kline.line_ma2) <= 2 \
                    or len(kline.line_ema1) <= 2 \
                    or len(kline.line_macd) <= 2:
                return

            m_macd_rt_dead_cross = kline.cur_macd_count > 0 and \
                                   kline.rt_macd_count < 0 and \
                                   kline.rt_macd_cross_price > 0 and \
                                   self.cur_99_price <= kline.rt_macd_cross_price
            # 计算MACD是否实时金叉
            m_macd_rt_golden_cross = kline.cur_macd_count < 0 and \
                                     kline.rt_macd_count > 0 and \
                                     kline.rt_macd_cross_price > 0 and \
                                     self.cur_99_price >= kline.rt_macd_cross_price

            # 通过MACD参数，判断多/空
            is_m_macd_long = (kline.cur_macd_count > 0 and not m_macd_rt_dead_cross) or m_macd_rt_golden_cross
            is_m_macd_short = (kline.cur_macd_count < 0 and not m_macd_rt_golden_cross) or m_macd_rt_dead_cross

            # 做多事务
            if kline.rt_ma1 > kline.line_ma1[-1] \
                    and kline.rt_ma2 > kline.line_ma2[-1] \
                    and kline.rt_ema1 > kline.line_ema1[-1] \
                    and is_m_macd_long:
                signal = self.policy.signals.get(kline_name, {})
                if signal.get('last_signal', '') != 'long':
                    signal.update({'last_signal': 'long', 'last_signal_time': self.cur_datetime})
                    self.policy.signals.update({kline_name: signal})
                if kline_name in self.policy.short_klines:
                    self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                    self.policy.short_klines.remove(kline_name)
                if kline_name not in self.policy.long_klines:
                    self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                    self.policy.long_klines.append(kline_name)
                continue

            # 做空事务
            if kline.rt_ma1 < kline.line_ma1[-1] \
                    and kline.rt_ma2 < kline.line_ma2[-1] \
                    and kline.rt_ema1 < kline.line_ema1[-1] \
                    and is_m_macd_short:
                signal = self.policy.signals.get(kline_name, {})
                if signal.get('last_signal', '') != 'short':
                    signal.update({'last_signal': 'short', 'last_signal_time': self.cur_datetime})
                    self.policy.signals.update({kline_name: signal})
                if kline_name in self.policy.long_klines:
                    self.write_log(u'从做多信号队列中移除:{}'.format(kline_name))
                    self.policy.long_klines.remove(kline_name)
                if kline_name not in self.policy.short_klines:
                    self.write_log(u'从做空信号队列中增加:{}'.format(kline_name))
                    self.policy.short_klines.append(kline_name)

    def tns_close_logic(self):
        """
        平仓逻辑
        :return:
        """
        if not self.trading or self.entrust != 0:
            return

        for kline_name, kline in self.klines.items():

            if kline_name in self.policy.long_klines \
                    and kline.rt_ma1 < kline.line_ma1[-1] \
                    and kline.rt_ma2 < kline.line_ma2[-1] \
                    and kline.rt_ema1 < kline.line_ema1[-1]:
                self.write_log(u'{}多头,{} 周期离场'.format(self.vt_symbol, kline_name))
                self.policy.long_klines.remove(kline_name)
                signal = self.policy.signals.get(kline_name, None)
                if signal and signal.get('last_signal', '') == 'long':
                    self.policy.signals.pop(kline_name, None)

                continue

            if kline_name in self.policy.short_klines \
                    and kline.rt_ma1 > kline.line_ma1[-1] \
                    and kline.rt_ma2 > kline.line_ma2[-1] \
                    and kline.rt_ema1 > kline.line_ema1[-1]:
                self.write_log(u'{}空头, {} 周期离场'.format(self.vt_symbol, kline_name))
                self.policy.short_klines.remove(kline_name)
                signal = self.policy.signals.get(kline_name, None)
                if signal and signal.get('last_signal', '') == 'short':
                    self.policy.signals.pop(kline_name, None)

    def on_bar_k(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        pass

    def tns_calcute_net_pos(self):
        """事务计算仓位轧差"""
        if not self.trading or self.entrust != 0:
            return

        self.account_pos = self.cta_engine.get_position_holding(self.vt_symbol)
        if not self.account_pos:
            self.write_error(f'未能获取{self.vt_symbol}持仓')

        # 校验多一次多空信号
        long_klines = [s for s in self.policy.long_klines if s in self.bar_names]
        short_klines = [s for s in self.policy.short_klines if s in self.bar_names]

        if len(long_klines) != len(self.policy.long_klines):
            self.send_wechat(f'{self.strategy_name}多头信号校验不一致,修正{self.policy.long_klines} => {long_klines}')
            self.policy.long_klines = copy.copy(long_klines)

        if len(short_klines) != len(self.policy.short_klines):
            self.send_wechat(f'{self.strategy_name}空头信号校验不一致，修正:{self.policy.short_klines} => {short_klines}')
            self.policy.short_klines = copy.copy(short_klines)

        # 多/空/净仓分数
        long_kline_count = len(self.policy.long_klines)
        short_kline_count = len(self.policy.short_klines)
        net_kline_count = long_kline_count - short_kline_count

        if net_kline_count != self.policy.last_net_count:
            self.write_log(u'信号K线净仓变化 {} =>{}'.format(self.policy.last_net_count, net_kline_count))
            self.policy.last_net_count = net_kline_count

        # 计算目标头寸，(正数：多头， 负数：空头，0：持平）
        if self.max_invest_pos > 0:
            # 采用固定最大仓位时
            target_volume = int(self.max_invest_pos * net_kline_count / self.kline_count)
            single_volume = float(self.max_invest_pos / self.kline_count)
            max_volume = self.max_invest_pos
        else:
            # 采用资金投入百分比
            balance, avaliable, _, _ = self.cta_engine.get_account()
            invest_margin = balance * self.max_invest_rate
            if invest_margin > self.max_invest_margin > 0:
                invest_margin = self.max_invest_margin
            max_volume = invest_margin / (self.cur_99_price * self.margin_rate * self.symbol_size)
            single_volume = float(max_volume / self.kline_count)
            target_volume = int(max_volume * net_kline_count / self.kline_count)

        diff_volume = target_volume - self.position.pos
        diff_volume = round(diff_volume, 7)
        single_volume = round(single_volume, 7)

        # 排除一些噪音（根据净值百分比出来的偏差）
        if abs(diff_volume) < single_volume * 0.8:
            return

        self.write_log(f"{self.vt_symbol}, 账号多单:{self.account_pos.long_pos},账号空单:{self.account_pos.short_pos}"
                       f"策略净仓:{self.position.pos}，多单:{self.position.long_pos},空单：{self.position.short_pos}\n"
                       f"目标仓位:{target_volume}，偏差仓位:{diff_volume},"
                       f"最大限仓:{max_volume}, 单次变动:{single_volume}")

        if diff_volume > 0:
            cover_volume = 0
            buy_volume = diff_volume
            if self.position.short_pos < 0:
                cover_volume = abs(self.position.short_pos)
                if cover_volume > diff_volume:
                    cover_volume = diff_volume
                    buy_volume = 0
                else:
                    buy_volume = diff_volume - cover_volume

            self.write_log(f'需要增加{self.vt_symbol}仓位{diff_volume} = [平空:{cover_volume}] + 开多{buy_volume}]')

            if cover_volume > 0:
                self.write_log(f'执行 {self.vt_symbol} cover:{cover_volume}')
                ret = self.tns_process_cover(cover_volume=cover_volume)
                if ret:
                    self.write_log(f'委托平仓空单成功')
                    return
                else:
                    self.write_log(u'执行平仓失败，转移买入数量:{} => {}'.format(buy_volume, buy_volume + cover_volume))
                    buy_volume += cover_volume
                    buy_volume = buy_volume

            if buy_volume > 0:
                self.write_log(f'执行 {self.vt_symbol} buy:{buy_volume}')

                ret = self.tns_add_long(buy_volume)
                if not ret:
                    self.write_error(u'执行买入仓位事务失败')

        elif diff_volume < 0:

            sell_volume = 0
            short_volume = abs(diff_volume)
            if self.position.long_pos > 0:
                sell_volume = abs(self.position.long_pos)
                if sell_volume > abs(diff_volume):
                    sell_volume = abs(diff_volume)
                    short_volume = 0
                else:
                    short_volume = abs(diff_volume) - sell_volume
                self.write_log(f'需要减少{self.vt_symbol}仓位{diff_volume} = [多平:{sell_volume}] + 空开{short_volume}]')

            if sell_volume > 0:
                self.write_log(f'执行 {self.vt_symbol}sell:{sell_volume}')
                ret = self.tns_process_sell(sell_volume=sell_volume)
                if ret:
                    self.write_log(f'委托平仓多单成功')
                    return
                else:
                    self.write_log(u'执行平仓失败，转移做空数量:{} => {}'.format(short_volume, short_volume + sell_volume))
                    short_volume += sell_volume

            if short_volume > 0:
                self.write_log(f'执行 {self.vt_symbol} short:{short_volume}')

                ret = self.tns_add_short(short_volume)
                if not ret:
                    self.write_error(u'执行做空仓位事务失败')

        self.policy.save()

    def tns_add_long(self, volume):
        """
        事务开多仓
        :return:
        """

        grid = self.tns_open_from_lock(open_symbol=self.vt_symbol, open_volume=volume, grid_type="",
                                       open_direction=Direction.LONG)

        if grid is None:
            if self.activate_today_lock:
                if self.position.long_pos >= volume * 3 > 0:
                    self.write_log(u'多单数量:{}(策略多单:{}),总数超过策略开仓手数:{}的3倍,不再开多仓'
                                   .format(self.position.long_pos, self.position.pos, volume))
                    return False

            grid = CtaGrid(direction=Direction.LONG,
                           open_price=self.cur_99_price,
                           vt_symbol=self.idx_symbol,
                           close_price=sys.maxsize,
                           volume=volume)

            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price,'target': True,'cci':True})
            ref = self.buy(vt_symbol=self.vt_symbol,
                           price=self.cur_mi_price,
                           volume=grid.volume,
                           grid=grid,
                           order_type=self.order_type,
                           order_time=self.cur_datetime)
            if len(ref) > 0:
                self.write_log(u'创建{}事务多单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                               .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))
                self.gt.dn_grids.append(grid)
                self.gt.save()
                return True
            else:
                self.write_error(u'创建{}事务多单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                                 .format(grid.type, grid.open_price, grid.volume, grid.close_price))
                return False
        else:
            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'reuse long {}=>{}'.format(grid.type, '')
            dist_record['volume'] = volume
            self.save_dist(dist_record)

            self.write_log(u'使用对锁仓位,释放空单,保留多单,gid:{}'.format(grid.id))
            grid.open_price = self.cur_99_price
            grid.close_price = sys.maxsize
            grid.stop_price = 0
            self.write_log(u'多单 {} =>{},更新开仓价:{},止损价:{}'.format(grid.type, '', grid.open_price, grid.stop_price))
            grid.type = ''
            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})
            grid.open_status = True
            grid.close_status = False
            grid.order_status = False
            grid.order_ids = []
            return True

    def tns_add_short(self, volume):
        """
        事务开空仓
        :return:
        """

        grid = self.tns_open_from_lock(open_symbol=self.vt_symbol, open_volume=volume,grid_type="",
                                       open_direction=Direction.SHORT)
        if grid is None:
            if self.activate_today_lock:
                if abs(self.position.short_pos) >= volume * 3 > 0:
                    self.write_log(u'空单数量:{}(含实际策略空单:{}),总数超过策略开仓手数:{}的3倍,不再开多仓'
                                   .format(abs(self.position.short_pos), abs(self.position.pos), volume))
                    return False

            grid = CtaGrid(direction=Direction.SHORT,
                           open_price=self.cur_99_price,
                           vt_symbol=self.idx_symbol,
                           close_price=-sys.maxsize,
                           stop_price=0,
                           volume=volume)
            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price,'target': True,'cci':True})
            ref = self.short(vt_symbol=self.vt_symbol,
                             price=self.cur_mi_price,
                             volume=grid.volume,
                             grid=grid,
                             order_type=self.order_type,
                             order_time=self.cur_datetime)
            if len(ref) > 0:
                self.write_log(u'创建{}事务空单,指数开空价：{}，主力开仓价:{},数量：{}，止盈价:{},止损价:{}'
                               .format(grid.type, grid.open_price, self.cur_mi_price, grid.volume, grid.close_price,
                                       grid.stop_price))
                self.gt.up_grids.append(grid)
                self.gt.save()
                return True
            else:
                self.write_error(u'创建{}事务空单,委托失败,开仓价：{}，数量：{}，止盈价:{}'
                                 .format(grid.type, grid.open_price, grid.volume, grid.close_price))
                return False
        else:
            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'reuse short {}=>{}'.format(grid.type, '')
            dist_record['volume'] = volume
            self.save_dist(dist_record)

            self.write_log(u'使用对锁仓位,释放多单,保留空单,gid:{}'.format(grid.id))
            grid.open_price = self.cur_99_price
            grid.close_price = 0 - sys.maxsize
            grid.stop_price = 0
            self.write_log(u'空单 {} =>{},开仓价:{},止损价:{}'.format(grid.type, '', grid.open_price, grid.stop_price))
            grid.type = ''
            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})
            grid.open_status = True
            grid.close_status = False
            grid.order_status = False
            grid.order_ids = []
            return True

    def tns_process_cover(self, cover_volume):
        """事务执行平空计划"""

        # 合约得持仓信息
        if self.account_pos is None:
            self.write_error(u'当前{}合约得持仓信息获取不到'.format(self.vt_symbol))
            return False

        cover_grid = self.tns_get_grid(direction=Direction.SHORT, close_volume=cover_volume)
        if cover_grid is None:
            self.write_error(u'无法获取合适的平空网格')
            return False

        # 通过解锁/平仓方式
        return self.tns_close_short_pos(cover_grid)

    def tns_process_sell(self, sell_volume):
        """事务执行平多计划"""

        # 合约得持仓信息
        if self.account_pos is None:
            self.write_error(u'当前{}合约得持仓信息获取不到'.format(self.vt_symbol))
            return False

        sell_grid = self.tns_get_grid(direction=Direction.LONG, close_volume=sell_volume)
        if sell_grid is None:
            self.write_error(u'无法获取合适的平多网格')
            return False

        # 通过解锁/平仓方式
        return self.tns_close_long_pos(sell_grid)

    def tns_get_grid(self, direction, close_volume):
        """根据需要平仓的volume，选取/创建出一个grid"""

        opened_grids = self.gt.get_opened_grids(direction=direction)
        if len(opened_grids) == 0:
            self.write_error(u'当前没有{}单得网格'.format(direction))
            return None

        select_grid = None
        remove_gids = []

        for g in opened_grids:
            if g.order_status:
                self.write_log(f'该网格正在委托中，不选择:{g.__dict__}')
                continue

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
                    remain_grid = copy.copy(select_grid)
                    remain_grid.id = str(uuid.uuid1())
                    remain_grid.volume = remain_volume
                    if direction == Direction.SHORT:
                        self.gt.up_grids.append(remain_grid)
                    else:
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
                    self.write_log(u'cover_volume已满足')
                    break

        if select_grid is None:
            self.write_error(u'没有可选择的{}单网格'.format(direction))
            return None

        if round(select_grid.volume, 7) != close_volume:
            self.write_error(u'没有可满足数量{}的{}单网格'.format(close_volume, direction))
            return None

        self.gt.remove_grids_by_ids(direction=direction, ids=remove_gids)

        return select_grid

    def display_tns(self):
        if not self.inited:
           return
        if self.backtesting:
            return
