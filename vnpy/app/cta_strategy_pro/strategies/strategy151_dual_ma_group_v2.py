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
from vnpy.app.cta_strategy_pro.template import CtaProFutureTemplate, Direction, get_underlying_symbol, Interval, \
    TickData, BarData
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar
from vnpy.component.cta_utility import check_duan_not_rt, check_bi_not_rt, check_chan_xt, DI_BEICHI_SIGNALS, \
    DING_BEICHI_SIGNALS,duan_bi_is_end
from vnpy.data.tdx.tdx_future_data import TdxFutureData
from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date


########################################################################
class Strategy151DualMaGroupV2(CtaProFutureTemplate):
    """CTA 双均线 组合轧差策略
    原始版本：
        金叉做多；死叉做空
        轧差
    v1版本：
        金叉时，生成突破线 = 取前n根bar的最高价，乘以1.03，或者加上1个ATR,或者两个缠论分笔高度。
        发生金叉后的m根bar内，如果价格触碰突破线，则开多；
        如果出现下跌线段，在底分型逆势进场

        持仓期间，在出现顶背驰信后，触碰x根bar的前低，离场
        离场后，允许再次进场

    v2版本：
        增加开仓时的亏损保护，减少回撤
        缠论开仓时，如果开仓位置在长均线之下，止损位置为一个缠论分笔平均高度
        缠论开仓时，如果开场位置在长均线之上，止损位置为长均线
        突破开仓时，价格为inited的价格

        增加保护跟涨止盈
        如果最后一笔的高度，大于2个平均分笔，启动跟涨止盈保护

    """
    author = u'大佳'
    # 输入参数 [ 快均线长度_慢均线长度_K线周期]
    bar_names = ['f60_s250_M15', 'f120_s500_M15', 'f250_s1000_M15']

    x_minute = 1  # 使用缠论的K线的时间周期

    # 策略在外部设置的参数
    parameters = ["max_invest_pos", "max_invest_margin", "max_invest_rate",
                  "bar_names", "x_minute",
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
        self.net_kline_count = 0  # 净仓位
        self.kline_x = None  # 使用缠论的K线

        self.init_past_3_4 = False  # 初始化经过2/3时间
        self.display_bars = False

        if setting:
            # 根据配置文件更新参数
            self.update_setting(setting)

            # 更新监控的k线总数
            self.kline_count = len(self.bar_names)

            # 创建信号K线
            for bar_name in self.bar_names:
                kline_setting = {}
                para_fast_len, para_slow_len, name = bar_name.split('_')
                kline_class, interval_num = get_cta_bar_type(name)
                kline_setting['name'] = bar_name

                para_fast_len = int(para_fast_len.replace('f', ''))
                para_slow_len = int(para_slow_len.replace('s', ''))

                kline_setting['bar_interval'] = interval_num  # K线的Bar时长
                kline_setting['para_atr1_len'] = max(20, para_fast_len)  # ATR均值
                kline_setting['para_ma1_len'] = para_fast_len  # 第1条均线
                kline_setting['para_ma2_len'] = para_slow_len  # 第2条均线
                kline_setting['para_active_chanlun'] = True  # 激活缠论
                kline_setting['price_tick'] = self.price_tick
                kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol.split('.')[0]).upper()
                self.write_log(f'创建K线:{kline_setting}')
                kline = kline_class(self, self.on_bar_k, kline_setting)
                self.klines.update({bar_name: kline})

            # 创建基础K线,使用缠论，精确入场点
            kline_setting = {}
            kline_setting['name'] = f'{self.vt_symbol}_M{self.x_minute}'
            kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
            kline_setting['price_tick'] = self.cta_engine.get_price_tick(self.vt_symbol)
            kline_setting['underly_symbol'] = self.vt_symbol.split('.')[0]
            kline_setting['para_macd_fast_len'] = 12
            kline_setting['para_macd_slow_len'] = 26
            kline_setting['para_macd_signal_len'] = 9
            kline_setting['para_active_chanlun'] = True

            self.kline_x = CtaMinuteBar(self, self.on_bar_k, kline_setting)
            self.klines.update({self.kline_x.name: self.kline_x})
            self.write_log(f'添加{self.vt_symbol} 缠论k线:{kline_setting}')

        if self.backtesting:
            # 回测时，自动输出K线数据
            self.export_klines()
            # 回测时,自动初始化
            self.on_init()

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

        # 写入文件
        import os

        # 输出信号K线
        for kline_name in self.bar_names:
            kline = self.klines.get(kline_name)
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
                {'name': f'ma{kline.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2', 'type_': 'list'}

            ]
            # 输出分笔csv文件
            kline.export_bi_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_bi.csv'.format(self.strategy_name, kline.name)))

            # 输出笔中枢csv文件
            kline.export_zs_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_zs.csv'.format(self.strategy_name, kline.name)))

            # 输出线段csv文件
            kline.export_duan_filename = os.path.abspath(
                os.path.join(self.cta_engine.get_logs_path(),
                             u'{}_{}_duan.csv'.format(self.strategy_name, kline.name)))

        # 输出缠论下单K线
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
            {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'}
        ]
        # 输出分笔csv文件
        self.kline_x.export_bi_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}_bi.csv'.format(self.strategy_name, self.kline_x.name)))

        # 输出笔中枢csv文件
        self.kline_x.export_zs_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}_zs.csv'.format(self.strategy_name, self.kline_x.name)))

        # 输出线段csv文件
        self.kline_x.export_duan_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}_duan.csv'.format(self.strategy_name, self.kline_x.name)))

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
            from vnpy.data.tdx.tdx_future_data import TdxFutureData

            # 优先从本地缓存文件，获取缓存
            last_bar_dt = self.load_klines_from_cache()

            # 创建接口
            tdx = TdxFutureData(self)

            # 开始时间
            if last_bar_dt:
                start_dt = last_bar_dt - timedelta(days=2)
            else:
                start_dt = datetime.now() - timedelta(days=120)

            # 通达信返回得bar，datetime属性是bar的结束时间，所以不能使用callback函数自动推送Bar
            # 这里可以直接取5分钟，也可以取一分钟数据
            result, min1_bars = tdx.get_bars(symbol=self.idx_symbol, period='1min', callback=None, bar_freq=1,
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
                for kline_name, kline in self.klines.items():
                    if kline_name.endswith('M1'):
                        bar_complete = True
                    else:
                        bar_complete = False
                    kline.add_bar(bar, bar_is_completed=bar_complete)

            return True

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
            # 更换合约检查
            if tick.datetime.minute >= 5:
                if self.position.long_pos > 0 and len(self.tick_dict) > 2:
                    # 有多单，且订阅的tick为两个以上
                    self.write_log(f'检查多单移仓换月')
                    self.tns_switch_long_pos(open_new=False)
                if self.position.short_pos < 0 and len(self.tick_dict) > 2:
                    self.write_log(f'检查空单移仓换月')
                    # 有空单，且订阅的tick为两个以上
                    self.tns_switch_short_pos(open_new=False)

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

        for kline_name in self.bar_names:
            kline = self.klines.get(kline_name)

            # 做多事务
            if kline.ma12_count > 0:
                signal = self.policy.signals.get(kline_name, {})
                long_break = signal.get('long_break', None)
                relong_break = signal.get('relong_break', None)

                # 首次金叉时, 做多突破线=前面15根bar的最高价1.03倍或者加1ATR
                if kline.ma12_count == 1 and signal.get('last_signal', '') != 'long':
                    long_break = max(kline.high_array[-int(kline.para_ma1_len / 2):])
                    long_break = min(long_break * 1.03,
                                     long_break + 2 * kline.cur_atr1,
                                     long_break + 2 * kline.bi_height_ma())
                    signal.update({"long_break": long_break})
                    signal.update({'last_signal': 'long',
                                   'last_signal_time': self.cur_datetime,
                                   'long_init_price': self.cur_99_price,
                                   'long_init_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')})
                    signal.pop("short_break", None)
                    signal.pop("relong_break", None)
                    signal.pop("reshort_break", None)
                    signal.pop("exit_bi_start", None)

                    self.policy.signals.update({kline_name: signal})
                    self.policy.save()
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = self.cur_99_price
                    dist_record['operation'] = 'long_init'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                # 价格没有回调，直接突破long_break，就马上追单进场
                # 也要防止跳空高开时追单
                if kline.ma12_count < kline.para_ma1_len \
                        and long_break \
                        and long_break + self.kline_x.bi_height_ma() > kline.cur_price >= long_break:
                    long_break = None
                    signal.pop('long_break', None)
                    if kline_name in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                        self.policy.short_klines.remove(kline_name)
                    if kline_name not in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                        self.policy.long_klines.append(kline_name)
                        signal.update({'long_open_price': self.cur_99_price,
                                       'long_open_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                       'long_stop_price': signal.get('long_init_price')})
                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = self.cur_99_price
                        dist_record['operation'] = 'long_break'
                        dist_record['volume'] = 0
                        self.save_dist(dist_record)
                        continue

                # 根据缠论K线，如果出现方向做空线段，且出现底分型，可以进场
                # 如果是顶部回落导致的离场，在当前一笔，不能接多
                if (long_break or (relong_break and kline.cur_bi.start != signal.get('exit_bi_start'))) \
                        and check_duan_not_rt(self.kline_x, Direction.SHORT) \
                        and len(self.kline_x.cur_duan.bi_list) >= 3 \
                        and kline.cur_duan \
                        and kline.cur_bi.direction == -1 \
                        and kline.cur_duan.direction == -1 \
                        and duan_bi_is_end(kline.cur_duan, Direction.SHORT):

                    # # 不允许两个均线同时向下时做多（此时还没有死叉）
                    # if kline.line_ma1[-1] < kline.line_ma1[-2] and kline.line_ma2[-1] < kline.line_ma2[-2]:
                    #     continue

                    if long_break:
                        long_break = None
                        signal.pop('long_break', None)
                    if relong_break:
                        relong_break = None
                        signal.pop('relong_break', None)
                        signal.pop('exit_bi_start', None)

                    if kline_name in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                        self.policy.short_klines.remove(kline_name)
                    if kline_name not in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                        self.policy.long_klines.append(kline_name)
                        if float(kline.cur_bi.low) > kline.line_ma2[-1]:
                            stop_price = kline.line_ma2[-1]
                        else:
                            stop_price = float(kline.cur_bi.low - kline.bi_height_ma())
                        signal.update({'entry_bi_start': kline.cur_bi.start,
                                       'long_open_price': self.cur_99_price,
                                       'long_stop_price': stop_price,
                                       'long_open_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')})

                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = self.cur_99_price
                        dist_record['operation'] = 'long_entry'
                        dist_record['stop_price'] = stop_price
                        self.save_dist(dist_record)
                        self.policy.save()
                        continue

                if relong_break and kline.cur_price >= relong_break:
                    relong_break = None
                    signal.pop('relong_break', None)
                    if kline_name in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中移除:{}'.format(kline_name))
                        self.policy.short_klines.remove(kline_name)
                    if kline_name not in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中增加:{}'.format(kline_name))
                        self.policy.long_klines.append(kline_name)
                        if float(kline.cur_bi.low) > kline.line_ma2[-1]:
                            stop_price = kline.line_ma2[-1]
                        else:
                            stop_price = float(kline.cur_bi.low - kline.bi_height_ma())

                        signal.update({'long_open_price': self.cur_99_price,
                                       'long_stop_price': stop_price,
                                       'long_open_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')})
                        self.policy.save()
                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = self.cur_99_price
                        dist_record['operation'] = 'long_reentry'
                        dist_record['stop_price'] = stop_price
                        self.save_dist(dist_record)
                        continue

            # 做空事务
            if kline.ma12_count < 0:
                signal = self.policy.signals.get(kline_name, {})
                short_break = signal.get('short_break', None)
                reshort_break = signal.get('reshort_break', None)

                # 首次死叉时, 做空突破线=前面15根bar的最低价0.997倍或者减1ATR
                if kline.ma12_count == -1 and signal.get('last_signal', '') != 'short':
                    short_break = min(kline.low_array[-int(kline.para_ma1_len / 2):])
                    short_break = max(short_break * 0.97,
                                      short_break - 2 * kline.cur_atr1,
                                      short_break - 2 * float(kline.bi_height_ma()))
                    signal.update({"short_break": short_break})
                    signal.update({'last_signal': 'short',
                                   'last_signal_time': self.cur_datetime,
                                   'short_init_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                   'short_init_price': self.cur_99_price})
                    signal.pop("long_break", None)
                    signal.pop("relong_break", None)
                    signal.pop("reshort_break", None)
                    signal.pop("exit_bi_start", None)
                    self.policy.signals.update({kline_name: signal})
                    self.policy.save()

                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = self.cur_99_price
                    dist_record['operation'] = 'short_init'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                # 突破做空价格位置，进场
                # 也要防止跳空时追单
                if kline.ma12_count > -kline.para_ma1_len \
                        and short_break \
                        and short_break - self.kline_x.bi_height_ma() < kline.cur_price <= short_break:
                    short_break = None
                    signal.pop('short_break', None)

                    if kline_name in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中移除:{}'.format(kline_name))
                        self.policy.long_klines.remove(kline_name)
                    if kline_name not in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中增加:{}'.format(kline_name))
                        self.policy.short_klines.append(kline_name)
                        signal.update({'short_open_price': self.cur_99_price,
                                       'short_open_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                       'short_stop_price': signal.get('short_init_price')})

                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = self.cur_99_price
                        dist_record['operation'] = 'short_break'
                        dist_record['stop_price'] =signal.get('short_init_price',0)
                        self.save_dist(dist_record)
                        self.policy.save()
                        continue

                # 存在开空突破时，如果kline_x出现做多线段，或者 kline出现做多分笔
                if (short_break or (reshort_break and kline.cur_bi.start != signal.get('exit_bi_start'))) \
                        and check_duan_not_rt(self.kline_x, Direction.LONG) \
                        and len(self.kline_x.cur_duan.bi_list) >= 3 \
                        and kline.cur_duan \
                        and kline.cur_bi.direction == 1 \
                        and kline.cur_duan.direction == 1 \
                        and duan_bi_is_end(kline.cur_duan, Direction.LONG):

                    # # 不允许两个均线同时向上时做空(这时候还没金叉）
                    # if kline.line_ma1[-1] > kline.line_ma1[-2] and kline.line_ma2[-1] > kline.line_ma2[-2]:
                    #     continue

                    if short_break:
                        short_break = None
                        signal.pop('short_break', None)
                    if reshort_break:
                        reshort_break = None
                        signal.pop('reshort_break', None)

                    if kline_name in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中移除:{}'.format(kline_name))
                        self.policy.long_klines.remove(kline_name)
                    if kline_name not in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中增加:{}'.format(kline_name))
                        self.policy.short_klines.append(kline_name)
                        if float(kline.cur_bi.high) < kline.line_ma2[-1]:
                            stop_price = kline.line_ma2[-1]
                        else:
                            stop_price = float(kline.cur_bi.high + kline.bi_height_ma())

                        signal.update({'entry_bi_start': kline.cur_bi.start,
                                       'short_open_price': self.cur_99_price,
                                       'short_stop_price': stop_price,
                                       'short_open_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')})

                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = self.cur_99_price
                        dist_record['operation'] = 'short_entry'
                        dist_record['stop_price'] = stop_price
                        self.save_dist(dist_record)
                        self.policy.save()
                        continue

                if reshort_break and kline.cur_price <= reshort_break:
                    reshort_break = None
                    signal.pop('reshort_break', None)
                    if kline_name in self.policy.long_klines:
                        self.write_log(u'从做多信号队列中移除:{}'.format(kline_name))
                        self.policy.long_klines.remove(kline_name)
                    if kline_name not in self.policy.short_klines:
                        self.write_log(u'从做空信号队列中增加:{}'.format(kline_name))
                        self.policy.short_klines.append(kline_name)
                        if float(kline.cur_bi.high) < kline.line_ma2[-1]:
                            stop_price = kline.line_ma2[-1]
                        else:
                            stop_price = float(kline.cur_bi.high + kline.bi_height_ma())

                        signal.update({
                                       'short_open_price': self.cur_99_price,
                                       'short_stop_price': stop_price,
                                       'short_open_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')})

                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = self.cur_99_price
                        dist_record['operation'] = 'short_reentry'
                        dist_record['stop_price'] = stop_price
                        self.save_dist(dist_record)

    def tns_close_logic(self):
        """
        平仓逻辑
        :return:
        """
        if not self.trading or self.entrust != 0:
            return

        for kline_name in self.bar_names:
            kline = self.klines.get(kline_name)
            signal = self.policy.signals.get(kline_name, {})
            if not kline.cur_duan:
                continue

            if kline_name in self.policy.long_klines:

                # 顶背驰得高点价格
                ding_beichi_price = signal.get('ding_beichi_price', None)
                if ding_beichi_price != float(kline.cur_duan.high) \
                        and kline.cur_duan.direction == 1 \
                        and kline.fenxing_list[-1].direction == 1 \
                        and check_bi_not_rt(kline, direction=Direction.LONG):
                    ding_beichi_signal = check_chan_xt(kline=kline, bi_list=kline.cur_duan.bi_list)
                    if ding_beichi_signal in DING_BEICHI_SIGNALS:
                        ding_beichi_price = float(kline.cur_duan.high)
                        self.write_log(f'添加顶背驰信号，价格:{ding_beichi_price}')
                        signal.update({'ding_beichi_price': ding_beichi_price})
                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = ding_beichi_price
                        dist_record['operation'] = ding_beichi_signal
                        dist_record['volume'] = 0
                        self.save_dist(dist_record)

                    elif ding_beichi_price:
                        self.write_log(f'移除{ding_beichi_price}的顶背驰信号')
                        signal.pop('ding_beichi_price', None)

                # 多头离场逻辑2： 事务保护止损
                long_stop_price = signal.get('long_stop_price', None)
                if long_stop_price and  kline.cur_price < long_stop_price:
                    self.write_log(u'{}多头,{} 周期止损离场'.format(self.vt_symbol, kline_name))
                    self.policy.long_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = kline.cur_price
                    dist_record['operation'] = 'long_stop_exit'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    # 设置前10根bar的最高，作为重新入场
                    if kline.ma12_count > 0:
                        signal.pop('long_stop_price', None)
                        signal.update({"relong_break": max(kline.high_array[-int(kline.para_ma1_len / 2):]),
                                       "relong_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})
                        self.policy.signals.update({kline_name: signal})
                    continue

                # 多头离场逻辑2： 下坡最近若干根bar的前低
                long_exit = min(kline.low_array[- int(kline.para_ma1_len / 3):])
                if ding_beichi_price \
                        and kline.cur_price <= long_exit < ding_beichi_price \
                        and kline.cur_price < kline.line_ma1[-1] \
                        and kline.cur_bi.start != signal.get('entry_bi_start')\
                        and kline.cur_bi.direction == -1\
                        and check_bi_not_rt(kline,Direction.SHORT):
                    self.write_log(u'{}多头,{} 周期背驰回落离场'.format(self.vt_symbol, kline_name))
                    self.policy.long_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = kline.cur_price
                    dist_record['operation'] = 'long_tail_exit'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    # 设置前10根bar的最高，作为重新入场
                    if kline.ma12_count > 0:
                        signal.update({"relong_break": max(kline.high_array[-int(kline.para_ma1_len / 2):]),
                                       "relong_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})
                        self.policy.signals.update({kline_name: signal})
                    continue

                # 多头离场条件3: 线段最后一笔，超长，回抽第一笔或者回抽第二笔，结束时，下坡均线1
                if kline.cur_duan.direction == 1 and len(kline.cur_duan.bi_list)>=3\
                    and kline.cur_duan.bi_list[-1].height > 2.5 * kline.bi_height_ma()\
                    and kline.cur_bi.start >= kline.cur_duan.end \
                    and kline.cur_bi.direction == -1 \
                    and kline.cur_price <= long_exit \
                    and kline.cur_price < kline.line_ma1[-1] \
                    and not kline.fenxing_list[-1].is_rt:

                    self.write_log(u'{}多头,{} 周期超长分笔回抽分笔破均线离场'.format(self.vt_symbol, kline_name))
                    self.policy.long_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = kline.cur_price
                    dist_record['operation'] = 'long_tail_exit2'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    # 设置前10根bar的最高，作为重新入场
                    if kline.ma12_count > 0:
                        signal.update({"relong_break": max(kline.high_array[-int(kline.para_ma1_len / 2):]),
                                       "relong_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})
                        self.policy.signals.update({kline_name: signal})
                    continue

                # 多头离场逻辑4：突破开仓后，马上出现顶背驰，并回落离场
                if ding_beichi_price \
                    and kline.cur_price < signal.get('long_open_price') < ding_beichi_price\
                    and kline.bi_list[-2].start == signal.get('entry_bi_start'):
                    self.write_log(u'{}多头,{} 周期背驰前开仓回落离场'.format(self.vt_symbol, kline_name))
                    self.policy.long_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = ding_beichi_price
                    dist_record['operation'] = 'long_tail_exit3'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    # 设置前10根bar的最高，作为重新入场
                    if kline.ma12_count > 0:
                        signal.update({"relong_break": max(kline.high_array[-int(kline.para_ma1_len / 2):]),
                                       "relong_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})
                        self.policy.signals.update({kline_name: signal})
                    continue

            if kline_name in self.policy.short_klines:
                # 底背驰得低点价格
                di_beichi_price = signal.get('di_beichi_price', None)
                short_open_price = signal.get('short_open_price', None)
                if di_beichi_price != float(kline.cur_duan.low) \
                        and kline.cur_duan.direction == -1 \
                        and kline.fenxing_list[-1].direction == -1 \
                        and not kline.fenxing_list[-1].is_rt:
                    di_beichi_signal = check_chan_xt(kline=kline, bi_list=kline.cur_duan.bi_list)
                    if di_beichi_signal in DI_BEICHI_SIGNALS:
                        di_beichi_price = float(kline.cur_duan.low)
                        self.write_log(f'添加底背驰信号，价格:{di_beichi_price}')
                        signal.update({'di_beichi_price': di_beichi_price})
                        dist_record = OrderedDict()
                        dist_record['datetime'] = self.cur_datetime
                        dist_record['symbol'] = self.idx_symbol
                        dist_record['price'] = kline.cur_price
                        dist_record['operation'] = di_beichi_signal
                        dist_record['volume'] = 0
                        self.save_dist(dist_record)
                    elif di_beichi_price:
                        self.write_log(f'移除{di_beichi_price}的顶背驰信号')
                        signal.pop('di_beichi_price', None)

                # 空头离场逻辑1： 事务保护止损
                short_stop_price = signal.get('short_stop_price', None)
                if short_stop_price and short_stop_price < kline.cur_price:
                    self.write_log(u'{}空头, {} 周期事务止损离场'.format(self.vt_symbol, kline_name))
                    self.policy.short_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = kline.cur_price
                    dist_record['operation'] = 'short_stop_exit'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    if kline.ma12_count < 0:
                        signal.pop('short_stop_price', None)
                        signal.update({"reshort_break": min(kline.low_array[-int(kline.para_ma1_len / 2):]),
                                       "reshort_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})

                        self.policy.signals.update({kline_name: signal})
                    continue

                # 空头离场逻辑2： 突破若干根bar的前高
                short_exit = max(kline.high_array[-int(kline.para_ma1_len / 3):])
                if di_beichi_price \
                        and kline.cur_price >= short_exit > di_beichi_price \
                        and kline.cur_price > kline.line_ma1[-1]\
                        and kline.cur_bi.start != signal.get('entry_bi_start')\
                        and kline.cur_bi.direction == 1\
                        and check_bi_not_rt(kline, Direction.LONG):
                    self.write_log(u'{}空头, {} 周期跟随回落离场'.format(self.vt_symbol, kline_name))
                    self.policy.short_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = di_beichi_price
                    dist_record['operation'] = 'short_tail_exit'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    if kline.ma12_count < 0:
                        signal.update({"reshort_break": min(kline.low_array[-int(kline.para_ma1_len / 2):]),
                                       "reshort_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})

                        self.policy.signals.update({kline_name: signal})
                    continue

                # 空头离场条件3: 线段最后一笔，超长，回抽第一笔或者回抽第二笔，结束时，上破均线1
                if kline.cur_duan.direction == -1 and len(kline.cur_duan.bi_list) >= 3 \
                        and kline.cur_duan.bi_list[-1].height > 2.5 * kline.bi_height_ma() \
                        and kline.cur_bi.start >= kline.cur_duan.end \
                        and kline.cur_bi.direction == 1 \
                        and kline.cur_price <= short_exit \
                        and kline.cur_price > kline.line_ma1[-1] \
                        and not kline.fenxing_list[-1].is_rt:
                    self.write_log(u'{}空头, {} 周期超长分笔回抽均线离场'.format(self.vt_symbol, kline_name))
                    self.policy.short_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = kline.cur_price
                    dist_record['operation'] = 'short_tail_exit2'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    if kline.ma12_count < 0:
                        signal.update({"reshort_break": min(kline.low_array[-int(kline.para_ma1_len / 2):]),
                                       "reshort_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})

                        self.policy.signals.update({kline_name: signal})
                    continue

                # 空头离场逻辑4：底背离前开空（追空），出现底背离，价格反抽破开仓价
                if di_beichi_price and short_open_price\
                        and kline.cur_price > short_open_price > di_beichi_price \
                        and kline.bi_list[-2].start == signal.get('entry_bi_start'):
                    self.write_log(u'{}空头, {} 周期底背驰前开仓，跟随回落离场'.format(self.vt_symbol, kline_name))
                    self.policy.short_klines.remove(kline_name)
                    dist_record = OrderedDict()
                    dist_record['datetime'] = self.cur_datetime
                    dist_record['symbol'] = self.idx_symbol
                    dist_record['price'] = kline.cur_price
                    dist_record['operation'] = 'short_tail_exit3'
                    dist_record['volume'] = 0
                    self.save_dist(dist_record)

                    if kline.ma12_count < 0:
                        signal.update({"reshort_break": min(kline.low_array[-int(kline.para_ma1_len / 2):]),
                                       "reshort_count": 12,
                                       "exit_bi_start": kline.cur_bi.start})

                        self.policy.signals.update({kline_name: signal})

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
            self.write_error(f'[轧差处理] 未能获取{self.vt_symbol}持仓')

        # 校验多一次多空信号
        long_klines = [s for s in self.policy.long_klines if s in self.bar_names]
        short_klines = [s for s in self.policy.short_klines if s in self.bar_names]

        if len(long_klines) != len(self.policy.long_klines):
            self.send_wechat(f'[轧差处理]{self.strategy_name}多头信号校验不一致,修正{self.policy.long_klines} => {long_klines}')
            self.policy.long_klines = copy.copy(long_klines)

        if len(short_klines) != len(self.policy.short_klines):
            self.send_wechat(f'[轧差处理]{self.strategy_name}空头信号校验不一致，修正:{self.policy.short_klines} => {short_klines}')
            self.policy.short_klines = copy.copy(short_klines)

        # 多/空/净仓分数
        long_kline_count = len(self.policy.long_klines)
        short_kline_count = len(self.policy.short_klines)
        net_kline_count = long_kline_count - short_kline_count

        # 计算目标头寸，(正数：多头， 负数：空头，0：持平）
        if self.max_invest_pos > 0:
            # 采用固定最大仓位时
            target_volume = int(self.max_invest_pos * net_kline_count / self.kline_count)
            single_volume = max(1, int(self.max_invest_pos / self.kline_count))
            max_volume = self.max_invest_pos
        else:
            # 采用资金投入百分比
            balance, avaliable, _, _ = self.cta_engine.get_account()
            invest_margin = balance * self.max_invest_rate
            self.write_log(f"[轧差处理] balance {balance}, available {avaliable} invest_margin {invest_margin}")
            if invest_margin > self.max_invest_margin > 0:
                invest_margin = self.max_invest_margin
            margin = self.cta_engine.get_margin(self.vt_symbol)
            if margin is None:
                margin = self.cur_99_price * self.margin_rate * self.symbol_size
            max_volume = max(1, invest_margin / margin)
            self.write_log(f"[轧差处理] margin {margin}, max_volume {max_volume}")
            single_volume = max(1, int(max_volume / self.kline_count))
            target_volume = int(max_volume * net_kline_count / self.kline_count)

        diff_volume = target_volume - self.position.pos
        diff_volume = round(diff_volume, 7)
        single_volume = round(single_volume, 7)

        self.write_log(f"[轧差处理]{self.vt_symbol}, 账号多单:{self.account_pos.long_pos},账号空单:{self.account_pos.short_pos}"
                       f"策略净仓:{self.position.pos}，多单:{self.position.long_pos},空单：{self.position.short_pos}\n"
                       f"目标仓位:{target_volume}，偏差仓位:{diff_volume},"
                       f"最大限仓:{max_volume}, 单次变动:{single_volume}")

        # K线净仓变化     （此处有可能更新轧差了，并没有执行完整开仓平仓）
        if net_kline_count != self.policy.last_net_count:
            self.write_log(u'[轧差处理]信号K线净仓变化 {} =>{}'.format(self.policy.last_net_count, net_kline_count))
            self.policy.last_net_count = net_kline_count
            self.policy.save()

        # K线净仓没有变化，由于资金、价格，保证金变化导致的调整
        else:
            if self.position.long_pos > 0 and diff_volume > 0 and net_kline_count > 0:
                self.write_log(f"[轧差处理], K线净仓：{net_kline_count}无变化,现多单:{self.position.long_pos},不做{diff_volume}调整")
                return

            if self.position.short_pos < 0 and diff_volume < 0 and net_kline_count < 0:
                self.write_log(f"[轧差处理], K线净仓：{net_kline_count}无变化,现空单:{self.position.short_pos},不做{diff_volume}调整")
                return
            #
            # 排除一些噪音（根据净值百分比出来的偏差）
            if abs(diff_volume) < single_volume * 0.8:
                self.write_log(f"[轧差处理] 排除一些噪音: {diff_volume} < {single_volume} * 0.8")
                return

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

            self.write_log(f'[轧差处理] {self.vt_symbol}需要增加仓位{diff_volume} = [平空:{cover_volume}] + 开多{buy_volume}]')

            if cover_volume > 0:
                self.write_log(f'[轧差处理] {self.vt_symbol} 执行cover事务:{cover_volume}')
                ret = self.tns_process_cover(cover_volume=cover_volume)
                if ret:
                    self.write_log(f'[轧差处理] {self.vt_symbol}委托cover事务成功')
                    return
                else:
                    self.write_log(u'[轧差处理]执行平仓失败，转移买入数量:{} => {}'.format(buy_volume, buy_volume + cover_volume))
                    buy_volume += cover_volume
                    buy_volume = buy_volume

            if buy_volume > 0:
                self.write_log(f'[轧差处理] {self.vt_symbol} 执行buy事务:{buy_volume}')

                ret = self.tns_add_long(buy_volume)
                if not ret:
                    self.write_error(f'[轧差处理] {self.vt_symbol} 执行buy事务失败')

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
                self.write_log(f'[轧差处理] {self.vt_symbol}需要减少仓位{diff_volume} = [多平:{sell_volume}] + 空开{short_volume}]')

            if sell_volume > 0:
                self.write_log(f'[轧差处理] {self.vt_symbol}执行sell事务:{sell_volume}')
                ret = self.tns_process_sell(sell_volume=sell_volume)
                if ret:
                    self.write_log(f'[轧差处理] {self.vt_symbol} 委托sell事务成功')
                    return
                else:
                    self.write_log(f'[轧差处理] 执行sell事务失败，转移做空数量:{short_volume} => {short_volume + sell_volume}')
                    short_volume += sell_volume

            if short_volume > 0:
                self.write_log(f'[轧差处理] {self.vt_symbol} 执行short事务:{short_volume}')

                ret = self.tns_add_short(short_volume)
                if not ret:
                    self.write_error(f'[轧差处理] {self.vt_symbol} 执行short事务失败')

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

            grid.snapshot.update(
                {'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price, 'target': True, 'cci': True})
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

        grid = self.tns_open_from_lock(open_symbol=self.vt_symbol, open_volume=volume, grid_type="",
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
            grid.snapshot.update(
                {'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price, 'target': True, 'cci': True})
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
            save_signal = copy.deepcopy(signal)

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
