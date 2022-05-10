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
from vnpy.component.cta_policy import CtaPolicy, TNS_STATUS_READY, TNS_STATUS_OBSERVATE, TNS_STATUS_OPENED
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar
from vnpy.component.cta_utility import check_duan_not_rt, check_bi_not_rt, check_chan_xt, DI_BEICHI_SIGNALS, \
    DING_BEICHI_SIGNALS, duan_bi_is_end, ChanSignals,check_zs_3rd

from vnpy.data.tdx.tdx_future_data import TdxFutureData
from vnpy.trader.utility import extract_vt_symbol, get_full_symbol, get_trading_date


########################################################################
class Strategy151DualMaGroupV3(CtaProFutureTemplate):
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

    v3版本：
        持仓期间，进行若干止损价格提升保护，如开仓价格保本提损、新中枢提损；
        持仓期间，对反向信号出现并确认时，提升止损价
        出现做空信号时，主动离场
        记录离场信号时间、价格，判断相同信号的再次进场时间
    """
    author = u'大佳'
    # 输入参数 [ 快均线长度_慢均线长度_K线周期]
    bar_names = ['f60_s250_M15', 'f120_s500_M15', 'f250_s1000_M15']

    x_minute = 1  # 使用缠论的K线的时间周期
    export_csv = []

    # 策略在外部设置的参数
    parameters = ["max_invest_pos", "max_invest_margin", "max_invest_rate",
                  "bar_names", "x_minute", "export_csv",
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

        self.debug_date_hours = ['2020-09-30 14']  # 回测时使用，['yyyy-mm-dd HH',]

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
                kline_setting['para_macd_fast_len'] = 12  # 激活macd，供趋势背驰的一些判断
                kline_setting['para_macd_slow_len'] = 26
                kline_setting['para_macd_signal_len'] = 9

                kline_setting['para_active_chanlun'] = True  # 激活缠论
                kline_setting['para_active_chan_xt'] = True  # 激活缠论的形态分析
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
            kline_setting['para_active_chan_xt'] = True  # 激活缠论的形态分析

            self.kline_x = CtaMinuteBar(self, self.on_bar_x, kline_setting)
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
            if len(self.export_csv) > 0 and kline_name not in self.export_csv:
                continue

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

        if len(self.export_csv) > 0 and self.kline_x.name in self.export_csv:
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
                start_dt = datetime.now() - timedelta(days=60)
                # 首次初始化时，数据较多，先暂停了kline_x的形态分析
                self.kline_x.para_active_chan_xt = False

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

            # 重新激活klinex的形态激活
            self.kline_x.para_active_chan_xt = True
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
        for kline_name, kline in self.klines.items():
            if kline_name.endswith('M1'):
                bar_complete = True
            else:
                bar_complete = False
            kline.add_bar(bar=copy.copy(bar), bar_is_completed=bar_complete)

        if self.inited and self.trading:

            if self.cur_datetime.strftime('%Y-%m-%d %H') in self.debug_date_hours:
                a = 1
            # 事务平衡仓位
            self.tns_calcute_net_pos()

        # 显示各指标信息
        self.display_tns()

    def fit_zs_signal(self, kline, signal_direction=Direction.LONG, strict=False):
        """
        判断当前K线，是否处于两个连续的下跌中枢
        :param kline:
        :param stict: 严格：True 两个中枢不存在中枢扩展；False，两个中枢
        :return:
        """
        if len(kline.bi_zs_list) < 2:
            return False

        if signal_direction == Direction.LONG:
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
        else:
            if not strict and kline.bi_zs_list[-2].low < kline.bi_zs_list[-1].low:
                return True

            # 倒2中枢顶部 < 倒1中枢底部
            if kline.bi_zs_list[-2].high < kline.bi_zs_list[-1].low:
                # 倒2中枢的低点
                max_high = max([bi.high for bi in kline.bi_zs_list[-2].bi_list])
                # 倒1中枢的底点
                min_low = min([bi.low for bi in kline.bi_zs_list[-1].bi_list])
                if max_high < min_low:
                    return True

        return False

    def fit_xt_signal(self, kline, signal_values, bi_num_list=None, look_back=None):
        """
        判断是否满足缠论心态信号要求
        在倒n笔内，出现信号.如类一买点
        :param kline:
        :param signal_values: [ChanSignals的value]
        :param 多笔形态清单，在5~13得奇数范围内
        :param look_back: 回溯得倒b笔范围，例如 -6:-1 就是 最后六笔
        :return:
        """
        if bi_num_list is None:
            bi_num_list = [9, 11, 13]
        if look_back is None:
            look_back = slice(-6, -1)

        # 检查9~13笔的信号
        for n in bi_num_list:
            # 倒5~倒1的信号value清单
            all_signals = [s.get('signal') for s in getattr(kline, f'xt_{n}_signals', [])[look_back]]
            # 判断是否有类一买点信号存在
            for signal_value in signal_values:
                if signal_value in all_signals:
                    return True

        return False

    def tns_open_logic(self, kline_name):
        """
        开仓逻辑： 无 或 空 => 开多[Ready] 模式； 无或多 => 开空[Ready] 模式
        :param kline_name: K线对应得开开仓信号
        :return:
        """
        # 获取K线
        kline = self.klines.get(kline_name, None)
        # 要求至少有三个线段
        if kline is None or kline.tre_duan is None:
            return

        # 获取K线对应得事务逻辑
        signal = self.policy.signals.get(kline_name, {})
        last_signal = signal.get('last_signal', '')
        tns_status = signal.get('tns_status', None)

        # 做多事务得判断逻辑
        if last_signal != 'long':

            # 反转信号1：两个以上下跌笔中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
            ret, signal_name, stop_price, tns = self.tns_open_long_rev_01(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return

            # 反转信号2：倒6~倒1笔内出现类一买点，然后均线金叉+下跌分笔
            ret, signal_name, stop_price, tns = self.tns_open_long_rev_02(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return

            # 反转信号3：线段背驰 + 分笔背驰 + 金叉 + close 价格站在均线上方
            ret, signal_name, stop_price, tns = self.tns_open_long_rev_03(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return
            # 反转信号四：超长下跌分笔 + 三角形整理+突破+金叉 + 下跌分笔在金叉上方
            ret, signal_name, stop_price, tns = self.tns_open_long_rev_04(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return
            # 反转信号五：超长下跌分笔 + 超长分笔反弹+回调+金叉 + 下跌分笔在金叉上方
            ret, signal_name, stop_price, tns = self.tns_open_long_rev_05(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return
            # # 反转信号6：下跌中枢被破坏+回调+金叉 + 下跌分笔在金叉上方
            # ret, signal_name, stop_price, tns = self.tns_open_long_rev_06(kline, signal)
            # if ret:
            #     self.tns_create_signal('long', signal_name, kline, stop_price, tns)
            #     return

            # 进攻信号1：
            # 倒1笔出现三类买点形态，然后才出现金叉
            ret, signal_name, stop_price, tns = self.tns_open_long_3rd_buy(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return

            # 进攻信号2：线段级别，形成扩张中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
            ret, signal_name, stop_price, tns = self.tns_open_long_zs_enlarge(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return

            # 重新进场：多单在金叉前止损离场后，反抽一笔，站在金叉上方，重新考虑进场
            ret, signal_name, stop_price, tns = self.tns_open_long_reentry(kline, signal)
            if ret:
                self.tns_create_signal('long', signal_name, kline, stop_price, tns)
                return

        # 做空事务得判断逻辑
        if last_signal != 'short':

            # 反转信号1：两个以上上涨笔中枢，最后一个线段[单笔]反抽中枢之下，形成死叉
            ret, signal_name, stop_price, tns = self.tns_open_short_rev_01(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

            # 反转信号2：倒6~倒1笔内出现类一卖点，然后均线死叉+上涨分笔
            ret, signal_name, stop_price, tns = self.tns_open_short_rev_02(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

            # 反转信号3： 线段背驰 + 分笔背驰 + 死叉 + high 价格站在均线下方
            ret, signal_name, stop_price, tns = self.tns_open_short_rev_03(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

            # 反转信号4： 超长上涨分笔 + 三角形整理+ 死叉 + 分笔站在死叉下方
            ret, signal_name, stop_price, tns = self.tns_open_short_rev_04(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return
            # 反转信号5： 超长上涨分笔 + 超长分笔下跌+回调+ 死叉 + 分笔站在死叉下方
            ret, signal_name, stop_price, tns = self.tns_open_short_rev_05(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

            # # 反转信号6：上涨中枢被破坏+回调+ 死叉 + 分笔站在死叉下方
            # ret, signal_name, stop_price, tns = self.tns_open_short_rev_06(kline, signal)
            # if ret:
            #     self.tns_create_signal('short', signal_name, kline, stop_price, tns)
            #     return

            # 进攻信号1：倒1笔出现三类卖点形态，然后才出现死叉
            ret, signal_name, stop_price, tns = self.tns_open_short_3rd_sell(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

            # 进攻信号2：线段级别，形成扩张中枢，最后一个线段[单笔]反抽中枢之下，形成死叉
            ret, signal_name, stop_price, tns = self.tns_open_short_zs_enlarge(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

            # 重新进场：空单在死叉前止损离场后，反抽一笔，站在死叉下方，重新考虑进场
            ret, signal_name, stop_price, tns = self.tns_open_short_reentry(kline, signal)
            if ret:
                self.tns_create_signal('short', signal_name, kline, stop_price, tns)
                return

    def tns_create_signal(self, signal_direction, signal_name, kline, stop_price, tns):
        """
        事务开启
        :param signal_name:
        :param kline:
        :param stop_price:
        :param tns:
        :return:
        """
        signal = {'last_signal': signal_direction,
                  'last_signal_time': self.cur_datetime,
                  'signal_name': signal_name,
                  'tns_status': tns,
                  'kline_name': kline.name,
                  'stop_price': float(stop_price),
                  'open_price': kline.cur_price,
                  'init_price': kline.cur_price,
                  'bi_start': kline.bi_list[-1].start,
                  'opened': False if tns != TNS_STATUS_OPENED else True,
                  'profit_rate': 0}

        # 立刻进场，无需等待小周期
        if tns == TNS_STATUS_OPENED:
            if signal_direction == 'long':
                if kline.name not in self.policy.long_klines:
                    self.policy.long_klines.append(kline.name)
                if kline.name in self.policy.short_klines:
                    self.policy.short_klines.remove(kline.name)
            elif signal_direction == 'short':
                if kline.name not in self.policy.short_klines:
                    self.policy.short_klines.append(kline.name)
                if kline.name in self.policy.long_klines:
                    self.policy.long_klines.remove(kline.name)

        self.policy.signals[kline.name] = signal
        self.policy.save()

        d = {
            "datetime": self.cur_datetime,
            "price": kline.cur_price,
            "operation": signal_name,
            "signal": f'{kline.name}.{signal_direction}',
            "stop_price": stop_price
        }
        self.save_dist(d)
        return

    def tns_open_long_rev_01(self, kline, signal):
        """
        多头开仓（反转信号01)
        两个以上下跌笔中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        # 反转信号1：两个以上下跌笔中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
        if self.fit_zs_signal(kline, signal_direction=Direction.LONG) \
                and kline.cur_duan.direction == 1 \
                and len(kline.cur_duan.bi_list) == 1 \
                and kline.cur_duan.low < kline.cur_bi_zs.min_low < kline.cur_duan.high \
                and check_bi_not_rt(kline, direction=Direction.SHORT) \
                and kline.cur_duan.end == kline.cur_bi.start \
                and kline.ma12_count > 0:
            min_low = kline.cur_duan.low

            return True, "趋势反转", min_low, TNS_STATUS_OBSERVATE
        else:
            return False, None, None, None

    def tns_open_long_rev_02(self, kline, signal):
        """
        多头开仓（反转信号02)
        倒6~倒1笔内出现类一买点，然后均线金叉+下跌分笔
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        # 反转信号2：倒3~倒1笔内出现类一买点，然后均线金叉+下跌分笔
        if self.fit_xt_signal(kline=kline, signal_values=[ChanSignals.Q1L0.value], look_back=slice(-3, -1)):

            # 防止出现瞬间跌破一类买点
            if kline.cur_duan.low < kline.cur_bi.low:

                # 均线金叉，下跌分笔
                if kline.ma12_count > 0 and check_bi_not_rt(kline, direction=Direction.SHORT) \
                        and float(kline.cur_bi.low) > kline.ma12_cross_list[-1].get('cross'):
                    min_low = kline.ma12_cross_list[-1].get('cross')

                    return True, "类一买点", min_low, TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_long_rev_03(self, kline, signal):
        """
        多头开仓（反转信号03)
        线段背驰 + 分笔背驰 + 金叉 + close 价格站在均线上方
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        # 线段背驰 + 分笔背驰 + 金叉 + close 价格站在均线上方
        if kline.cur_duan.direction == -1 \
                and kline.is_duan_divergence(direction=Direction.SHORT) \
                and kline.ma12_count > 0 \
                and kline.cur_bi.direction == 1 \
                and kline.low_array[-2] > kline.line_ma1[-3] > kline.ma12_cross_list[-1].get('cross')\
                and kline.low_array[-1] > kline.line_ma1[-2] > kline.ma12_cross_list[-1].get('cross'):
            if kline.cur_duan.end == kline.cur_bi.start \
                    and kline.cur_bi.high > kline.bi_list[-2].high \
                    and float(kline.cur_duan.low) < kline.ma12_cross_list[-1].get('cross'):
                min_low = kline.ma12_cross_list[-1].get('cross')

                return True, "段背驰买点", min_low, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_long_rev_04(self, kline, signal):
        """
        多头开仓（反转信号04)
        超长下跌分笔 + 收敛三角形反转 突破 + 金叉 + 下跌分笔底部价格站在均线上方
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        ret = False
        # 超长下跌分笔 + 收敛分笔 + 金叉 + 下跌分笔底部价格站在均线上方
        if kline.cur_duan.direction == -1 and kline.ma12_count > 0 \
                and float(kline.cur_duan.low) < kline.ma12_cross_list[-1].get('cross') < float(kline.cur_bi.low) \
                and kline.cur_bi.direction == -1 \
                and kline.cur_duan.bi_list[-1].height > 2 * kline.bi_height_ma() \
                and len(kline.cur_duan.bi_list) >= 3 \
                and kline.cur_duan.end == kline.bi_list[-6].start \
                and kline.bi_list[-6].high > kline.bi_list[-4].high \
                and kline.bi_list[-6].low < kline.bi_list[-4].low < kline.bi_list[-2].low \
                and kline.bi_list[-1].high > kline.bi_list[-3].high:
            ret = True

        if kline.cur_duan.direction == 1 and kline.ma12_count > 0 \
                and kline.cur_duan.low < kline.ma12_cross_list[-1].get('cross') \
                and kline.cur_bi.start == kline.cur_duan.end \
                and kline.pre_duan.bi_list[-1].height > 2 * kline.bi_height_ma() \
                and len(kline.pre_duan.bi_list) >= 3 \
                and kline.pre_duan.end == kline.bi_list[-6].start \
                and kline.cur_duan.height < kline.pre_duan.height \
                and kline.bi_list[-6].high > kline.bi_list[-4].high \
                and kline.bi_list[-6].low < kline.bi_list[-4].low < kline.bi_list[-2].low \
                and kline.bi_list[-1].high > kline.bi_list[-3].high:
            ret = True

        if ret:
            min_low = float(kline.bi_list[-2].low)
            return True, "三角突破买点", min_low, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_long_rev_05(self, kline, signal):
        """
        反转信号5：超长一笔下跌，超长一笔回调，反抽后高于金叉位置
        :param kline:
        :param signal:
        :return:
        """
        if kline.cur_duan.direction == -1 and kline.cur_duan.bi_list[-1].height > 1.5 * kline.bi_height_ma() \
                and kline.bi_list[-2].height > kline.bi_height_ma() \
                and check_bi_not_rt(kline, Direction.SHORT) \
                and len(kline.cur_duan.bi_list) >= 3 \
                and kline.cur_duan.end == kline.bi_list[-2].start \
                and kline.ma12_count > 0 \
                and float(kline.cur_bi.low) > kline.ma12_cross_list[-1].get('cross')\
                and float(kline.bi_list[-2].low) < kline.ma12_cross_list[-1].get('cross'):
            return True, "5浪反转", float(kline.bi_list[-2].low), TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_long_rev_06(self, kline, signal):
        """
        中枢突破后，上涨段成立，且上涨段刺破中枢，回调下跌笔，在金叉上方。
        :param kline:
        :param signal:
        :return:
        """
        if kline.cur_bi_zs and kline.pre_duan.end > kline.cur_bi_zs.end > kline.pre_duan.start \
            and kline.cur_duan.direction == 1 \
            and kline.cur_duan.end == kline.cur_bi.start \
            and check_bi_not_rt(kline, Direction.SHORT)\
            and kline.cur_duan.high > kline.cur_bi_zs.low\
            and kline.ma12_count > 0 \
            and kline.cur_duan.low < kline.ma12_cross_list[-1].get('cross') < kline.cur_bi.low:
            return True, "下跌中枢破坏", kline.ma12_cross_list[-1].get('cross'), TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_long_3rd_buy(self, kline, signal):
        """
        多头开仓（三买+金叉)
        线段背驰 + 分笔背驰 + 金叉 + close 价格站在均线上方
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        xt_5_signal = kline.get_xt_signal(xt_name='xt_5_signals', x=1)
        # 倒1笔出现三类买点形态，然后才出现金叉
        if xt_5_signal.get('signal') == ChanSignals.LI0.value \
                and kline.cur_bi.direction == 1 \
                and kline.ma12_count > 0 \
                and xt_5_signal.get('price') > kline.ma12_cross_list[-1].get('cross'):
                # and (kline.pre_duan.height > kline.cur_duan.height or kline.cur_duan.height > 2 * kline.bi_height_ma()) \
                #     and kline.pre_duan.direction == -1 \

            # 三类买点的时间，不能是刚刚止损
            if len(self.policy.long_exit_dt) > 0 \
                and kline.bi_list[-2].start < self.policy.long_exit_dt:
                return False, None, None, None

            zs_high = kline.bi_list[-4].high
            return True, "三类买点", zs_high, TNS_STATUS_OPENED

        # 使用中枢
        if kline.cur_bi_zs and kline.cur_bi_zs.end == kline.bi_list[-3].start\
            and kline.ma12_count > 0 \
            and kline.cur_bi.direction == 1\
            and kline.cur_bi.low > kline.cur_bi_zs.high \
            and float(kline.cur_bi.low) > kline.ma12_cross_list[-1].get('cross') \
            and kline.bi_list[-2].high > kline.cur_bi_zs.high\
            and kline.cur_bi.low - kline.cur_bi_zs.high < kline.bi_height_ma():
            return True, "三类买点", float(kline.cur_bi_zs.high), TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_long_zs_enlarge(self, kline, signal):
        """
        多头开仓（线段+金叉)
        线段级别，形成扩张中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        # 进攻信号2：线段级别，形成扩张中枢，最后一个线段[单笔]反抽中枢之上，形成金叉
        if kline.tre_duan.height < kline.pre_duan.height < kline.cur_duan.height \
                and kline.cur_duan.direction == 1 \
                and len(kline.cur_duan.bi_list) == 1 \
                and kline.cur_bi_zs\
                and kline.cur_duan.low < kline.cur_bi_zs.low < kline.cur_duan.high \
                and check_bi_not_rt(kline, direction=Direction.SHORT) \
                and kline.cur_duan.end == kline.cur_bi.start \
                and kline.ma12_count > 0:
            min_low = kline.cur_duan.low
            return True, "扩张中枢买点", min_low, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_long_reentry(self, kline,signal):
        """
        多单止损后，重新入场, 或空单止损后，反手进场
        :param kline:
        :param signal:
        :return:
        """
        if self.policy.long_exit_price > 0 and len(self.policy.long_exit_dt)>0 and  signal.get('last_signal', '') == '':
            if kline.ma12_count>0 \
                    and kline.cur_bi.direction == 1\
                    and float(kline.cur_bi.low) > max(kline.line_ma1[-1],kline.line_ma2[-1])\
                    and self.policy.long_exit_price < min(kline.line_ma1[-1],kline.line_ma2[-1])\
                    and kline.bi_list[-4].start < self.policy.long_exit_dt <= kline.bi_list[-4].end:
                min_low = max(kline.line_ma1[-1],kline.line_ma2[-1])
                return True, "重新入场", min_low, TNS_STATUS_OBSERVATE

        if self.policy.short_exit_price > 0 and len(self.policy.short_exit_dt) > 0 and signal.get('last_signal', ''):
            if len(kline.ma12_cross_list) > 3:
                tre_cross,pre_cross, cur_cross = kline.ma12_cross_list[-3:]

                if kline.ma12_count>0 \
                        and pre_cross.get('cross') - cur_cross.get('cross') < kline.bi_height_ma()\
                        and pre_cross.get('cross') - tre_cross.get('cross') > 2 * kline.bi_height_ma()\
                        and kline.cur_bi.direction == 1\
                        and float(kline.cur_bi.low) > cur_cross.get('cross')\
                        and self.policy.short_exit_price > cur_cross.get('cross')\
                        and kline.bi_list[-3].start < self.policy.short_exit_dt <= kline.bi_list[-3].end:
                    min_low = max(kline.line_ma1[-1], kline.line_ma2[-1])
                    return True, "重新入场", min_low, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_short_rev_01(self, kline, signal):
        """
        开空反转信号1：两个以上上涨笔中枢，最后一个线段[单笔]反抽中枢之下，形成死叉
        :param kline:
        :param signal:
        :return:
        """
        # 反转信号1：两个以上上涨笔中枢，最后一个线段[单笔]反抽中枢之下，形成死叉
        if self.fit_zs_signal(kline, signal_direction=Direction.SHORT) \
                and kline.cur_duan.direction == -1 \
                and len(kline.cur_duan.bi_list) == 1 \
                and kline.cur_duan.high > kline.cur_bi_zs.max_high > kline.cur_duan.low \
                and check_bi_not_rt(kline, direction=Direction.LONG) \
                and kline.cur_duan.end == kline.cur_bi.start \
                and kline.ma12_count < 0:
            max_high = kline.cur_duan.high
            return True, '趋势反转', max_high, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_short_rev_02(self, kline, signal):
        """
        反转信号2：倒3~倒1笔内出现类一卖点，然后均线死叉+上涨分笔
        :param kline:
        :param signal:
        :return:
        """
        if self.fit_xt_signal(kline=kline, signal_values=[ChanSignals.Q1S0.value], look_back=slice(-3, -1)):

            # 防止出现瞬间涨破一类卖点
            if kline.cur_duan.high > kline.cur_bi.high:

                # 均线死叉，上涨分笔, 顶部低于死叉点
                if kline.ma12_count < 0 and check_bi_not_rt(kline, direction=Direction.LONG) \
                        and float(kline.cur_bi.high) < kline.ma12_cross_list[-1].get('cross'):
                    max_high = kline.ma12_cross_list[-1].get('cross')
                    return True, '类一卖点', max_high, TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_short_rev_03(self, kline, signal):
        """
        反转信号3：线段背驰 + 分笔背驰 + 死叉 + high 价格站在均线下方
        :param kline:
        :param signal:
        :return:
        """
        # 线段背驰 + 分笔背驰 + 死叉 + high 价格站在均线下方
        if kline.cur_duan.direction == 1 \
                and kline.is_duan_divergence(direction=Direction.LONG) \
                and kline.ma12_count < 0 \
                and kline.cur_bi.direction == -1 \
                and kline.high_array[-2] < kline.line_ma1[-3] < kline.ma12_cross_list[-1].get('cross')\
                and kline.high_array[-1] < kline.line_ma1[-2] < kline.ma12_cross_list[-1].get('cross'):
            if kline.cur_duan.end == kline.cur_bi.start \
                    and kline.cur_bi.low < kline.bi_list[-2].low \
                    and float(kline.cur_duan.high) > kline.ma12_cross_list[-1].get('cross'):
                max_high = kline.ma12_cross_list[-1].get('cross')
                return True, '段背驰卖点', max_high, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_short_rev_04(self, kline, signal):
        """
        空头开仓（反转信号04)
        超长上涨分笔 + 收敛三角形反转 突破 + 死叉 + 上涨分笔底部价格站在死叉下方
        :param kline: K线
        :param signal: 当前事务
        :return: True/False, signal_name, stop_price，TNS
        """
        ret = False
        # 超长上涨分笔 + 收敛分笔 + 死叉 + 上涨分笔顶部价格站在死叉下方
        if kline.cur_duan.direction == 1 and kline.ma12_count < 0 \
                and len(kline.ma12_cross_list) > 1 \
                and kline.cur_duan.high > kline.ma12_cross_list[-1].get('cross') > kline.cur_bi.high \
                and kline.cur_bi.direction == 1 \
                and kline.cur_duan.bi_list[-1].height > 2 * kline.bi_height_ma() \
                and len(kline.cur_duan.bi_list) >= 3 \
                and kline.cur_duan.end == kline.bi_list[-6].start \
                and kline.bi_list[-6].low < kline.bi_list[-4].low \
                and kline.bi_list[-6].high > kline.bi_list[-4].high > kline.bi_list[-2].high \
                and kline.bi_list[-1].low < kline.bi_list[-3].low:
            ret = True

        if kline.cur_duan.direction == -1 and kline.ma12_count < 0 \
                and len(kline.ma12_cross_list) > 1 \
                and kline.cur_duan.high > kline.ma12_cross_list[-1].get('cross') > kline.cur_bi.high \
                and kline.cur_bi.start == kline.cur_duan.end \
                and kline.pre_duan.bi_list[-1].height > 2 * kline.bi_height_ma() \
                and len(kline.pre_duan.bi_list) >= 3 \
                and kline.pre_duan.end == kline.bi_list[-6].start \
                and kline.cur_duan.height < kline.pre_duan.height \
                and kline.bi_list[-6].low < kline.bi_list[-4].low \
                and kline.bi_list[-6].high > kline.bi_list[-4].high > kline.bi_list[-2].high \
                and kline.bi_list[-1].low < kline.bi_list[-3].low:
            ret = True

        if ret:
            return True, "三角突破卖点", float(kline.bi_list[-2].high), TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_short_rev_05(self, kline, signal):
        """
        反转信号5：超长一笔上涨跌，超长一笔回调，反抽后低于死叉位置
        :param kline:
        :param signal:
        :return:
        """
        if kline.cur_duan.direction == 1 and kline.cur_duan.bi_list[-1].height > 1.5 * kline.bi_height_ma() \
                and kline.bi_list[-2].height > kline.bi_height_ma() \
                and check_bi_not_rt(kline, Direction.LONG) \
                and len(kline.cur_duan.bi_list) >= 3 \
                and kline.cur_duan.end == kline.bi_list[-2].start \
                and kline.ma12_count < 0 \
                and float(kline.cur_bi.high) < kline.ma12_cross_list[-1].get('cross')\
                and float(kline.bi_list[-2].high) > kline.ma12_cross_list[-1].get('cross'):
            return True, "5浪反转", float(kline.bi_list[-2].high), TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_short_rev_06(self, kline, signal):
        """
        上涨中枢被破坏，反转做空
        :param kline:
        :param signal:
        :return:
        """
        if kline.cur_bi_zs and kline.pre_duan.end > kline.cur_bi_zs.end > kline.pre_duan.start \
            and kline.cur_duan.direction == -1 \
            and kline.cur_duan.end == kline.cur_bi.start \
            and check_bi_not_rt(kline, Direction.LONG)\
            and kline.cur_duan.low < kline.cur_bi_zs.high\
            and kline.ma12_count < 0 \
            and kline.cur_duan.high > kline.ma12_cross_list[-1].get('cross') > kline.cur_bi.high:
            return True, "上涨中枢破坏", kline.ma12_cross_list[-1].get('cross'), TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_short_3rd_sell(self, kline, signal):
        """
        反转信号2：倒6~倒1笔内出现类一卖点，然后均线死叉+上涨分笔
        :param kline:
        :param signal:
        :return:
        """
        xt_5_signal = kline.get_xt_signal(xt_name='xt_5_signals', x=1)
        # 倒1笔出现三类卖点形态，然后才出现死叉
        if xt_5_signal.get('signal') == ChanSignals.SI0.value \
                and kline.cur_bi.direction == -1 \
                and kline.ma12_count < 0 \
                and xt_5_signal.get('price') < kline.ma12_cross_list[-1].get('cross'):
            # and (kline.pre_duan.height > kline.cur_duan.height or kline.cur_duan.height > 2 * kline.bi_height_ma()) \
            #     and kline.pre_duan.direction == 1 \
                # # 三类卖点的时间，在死叉之前
            # if xt_5_signal.get('start', "") < kline.ma12_cross_list[-1].get('datetime', ""):
            if len(self.policy.short_exit_dt) > 0 \
                    and kline.bi_list[-2].start < self.policy.short_exit_dt:
                return False, None, None, None

            zs_low = kline.bi_list[-4].low
            return True, '三类卖点', zs_low, TNS_STATUS_OPENED

        # 使用中枢
        if kline.cur_bi_zs and kline.cur_bi_zs.end == kline.bi_list[-3].start \
                and kline.ma12_count < 0 \
                and kline.cur_bi.direction == -1 \
                and kline.cur_bi.high < kline.cur_bi_zs.low \
                and float(kline.cur_bi.high) < kline.ma12_cross_list[-1].get('cross')\
                and kline.bi_list[-2].low < kline.cur_bi_zs.low \
                and kline.cur_bi_zs.low - kline.cur_bi.high < kline.bi_height_ma():
            return True, '三类卖点', kline.cur_bi_zs.low, TNS_STATUS_OPENED

        return False, None, None, None

    def tns_open_short_zs_enlarge(self, kline, signal):
        """
        反转信号2：倒6~倒1笔内出现类一卖点，然后均线死叉+上涨分笔
        :param kline:
        :param signal:
        :return:
        """
        # 进攻信号2：线段级别，形成扩张中枢，最后一个线段[单笔]反抽中枢之下，形成死叉
        if kline.tre_duan.height < kline.pre_duan.height < kline.cur_duan.height \
                and kline.cur_duan.direction == -1 \
                and len(kline.cur_duan.bi_list) == 1 \
                and kline.cur_bi_zs \
                and kline.cur_duan.low < kline.cur_bi_zs.high < kline.cur_duan.high \
                and check_bi_not_rt(kline, direction=Direction.LONG) \
                and kline.cur_duan.end == kline.cur_bi.start \
                and kline.ma12_count < 0:
            max_high = kline.cur_duan.high
            return True, '中枢扩张', max_high, TNS_STATUS_OBSERVATE

        return False, None, None, None

    def tns_open_short_reentry(self, kline,signal):
        """
        空止损后，重新入场
        :param kline:
        :param signal:
        :return:
        """
        if self.policy.short_exit_price > 0 and len(self.policy.short_exit_dt)>0 and  signal.get('last_signal', ''):

            if kline.ma12_count < 0 \
                    and kline.cur_bi.direction ==-1\
                    and float(kline.cur_bi.high) < min(kline.line_ma1[-1],kline.line_ma2[-1])\
                    and self.policy.short_exit_price > max(kline.line_ma1[-1],kline.line_ma2[-1])\
                    and kline.bi_list[-4].start < self.policy.short_exit_dt <= kline.bi_list[-4].end:
                max_high = min(kline.line_ma1[-1],kline.line_ma2[-1])
                return True, "重新入场", max_high, TNS_STATUS_OBSERVATE
        if self.policy.long_exit_price > 0 and len(self.policy.long_exit_dt) > 0 and signal.get('last_signal', '') == '':
            if len(kline.ma12_cross_list) > 3:
                tre_cross,pre_cross, cur_cross = kline.ma12_cross_list[-3:]

                if kline.ma12_count< 0 \
                        and cur_cross.get('cross') - pre_cross.get('cross') < kline.bi_height_ma()\
                        and tre_cross.get('cross') - pre_cross.get('cross')  > 2 * kline.bi_height_ma()\
                        and kline.cur_bi.direction == -1\
                        and float(kline.cur_bi.high) < cur_cross.get('cross')\
                        and self.policy.long_exit_price < cur_cross.get('cross')\
                        and kline.bi_list[-3].start < self.policy.long_exit_dt <= kline.bi_list[-3].end:
                    max_high = cur_cross.get('cross')
                    return True, "重新入场", max_high, TNS_STATUS_OBSERVATE
        return False, None, None, None

    def tns_close_logic(self, kline_name):
        """
        平仓逻辑
        :param kline_name: K线对应得平仓信号
        :return:
        """
        # kline_name => kline
        kline = self.klines.get(kline_name, None)
        if not kline or kline.cur_bi is None:
            return False

        # kline_name => 事务
        signal = self.policy.signals.get(kline_name, None)
        if signal is None:
            return False

        # 事务信号
        last_signal = signal.get('last_signal', None)
        last_signal_time = signal.get('last_signal_time', None)

        # 事务状态（是否已经开仓了？）
        tns_status = signal.get('tns_status', None)
        if tns_status != TNS_STATUS_OPENED:
            return False

        # datetime => str
        if last_signal_time and isinstance(last_signal_time, datetime):
            last_signal_time = last_signal_time.strftime('%Y-%m-%d %H:%M:%S')
        signal_name = signal.get('signal_name', None)
        open_price = signal.get('open_price', None)
        init_price = signal.get('init_price', None)
        bi_start = signal.get('bi_start', None)
        stop_price = signal.get('stop_price', None)

        if last_signal == 'long':
            # 持有做多事务,进行更新
            if kline_name in self.policy.long_klines:
                # 更新收益率
                if init_price:
                    profit = kline.cur_price - init_price
                    profit_rate = round(profit / init_price, 4)
                    signal.update({'profit_rate': profit})

            # 【多单】保本提损：开仓后，有新的中枢产生，且价格比开仓价格高
            if stop_price and kline.cur_bi_zs \
                    and stop_price < open_price \
                    and kline.cur_bi_zs.start > last_signal_time \
                    and self.cur_99_price > kline.cur_bi_zs.low > open_price:
                # 保本+一点点利润
                stop_price = open_price + (kline.cur_bi_zs.low - open_price) * 0.18
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

            # 【多单】保本提损：开仓后，有新的笔中点价格比开仓价格高
            if stop_price and stop_price < open_price \
                    and self.cur_99_price > kline.bi_list[-2].middle > open_price \
                    and kline.cur_bi.direction == -1 \
                    and float(kline.cur_bi.high) - open_price > 1.6 * float(kline.bi_height_ma()):
                # 保本+一点点利润
                stop_price = open_price + (float(kline.cur_bi.high) - open_price) * 0.18
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

            # 【多单】保本提损：开仓后，有新的笔中点价格比开仓价格高
            if stop_price and stop_price < open_price \
                    and self.cur_99_price > kline.cur_bi.middle > open_price \
                    and kline.cur_bi.direction == 1 \
                    and check_bi_not_rt(kline, Direction.LONG) \
                    and float(kline.cur_bi.high) - open_price > 2 * float(kline.bi_height_ma()):
                # 保本+一点点利润
                stop_price = open_price + (float(kline.cur_bi.high) - open_price) * 0.18
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

            # 【多单】中枢提损：如果止损价低于当前中枢底部，且当前上涨笔出现在中枢上方 1.5笔高
            if stop_price and kline.cur_bi_zs \
                    and stop_price < float(kline.cur_bi_zs.low) \
                    and kline.cur_bi.high > kline.cur_bi_zs.high + 1.5 * kline.bi_height_ma() \
                    and kline.cur_bi_zs.end >= kline.bi_list[-3].start \
                    and kline.cur_bi.direction == 1 \
                    and check_duan_not_rt(kline, Direction.LONG) \
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
            # 【多单】反转信号1提损：两个以上上涨笔中枢，最后一个线段[单笔]反抽中枢之下
            if stop_price and kline.cur_bi_zs\
                    and stop_price < float(kline.cur_bi.low)\
                    and self.fit_zs_signal(kline, signal_direction=Direction.SHORT) \
                    and kline.cur_duan.direction == -1 \
                    and len(kline.cur_duan.bi_list) == 1 \
                    and kline.cur_duan.low < kline.cur_bi_zs.low < kline.cur_duan.high \
                    and check_bi_not_rt(kline, direction=Direction.LONG) \
                    and kline.cur_duan.end == kline.cur_bi.start \
                    and float(kline.cur_bi.high) < kline.line_ma1[-1]:
                # 止损价，移动至当前分笔底部
                stop_price = float(kline.cur_bi.low)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '反转信号1提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【多单】 极限一上涨笔后死叉
            if kline.cur_duan.direction == 1 \
                    and kline.cur_duan.end <= kline.cur_bi.start \
                    and kline.cur_duan.bi_list[-1].height > 1.6 * kline.bi_height_ma() \
                    and len( kline.ma12_cross_list) > 1 \
                    and kline.cur_duan.bi_list[-1].start > kline.ma12_cross_list[-2].get('datetime', "") \
                    and kline.cur_bi.direction == 1 \
                    and kline.ma12_count < 0 \
                    and stop_price < float(kline.cur_duan.bi_list[-1].low) \
                    and float(kline.cur_bi.low) < kline.line_ma2[-1]:
                # 止损价，移动至当前笔的底部,或者线段高点回落2个平均笔高度，或者均线
                stop_price = float(kline.cur_bi.low)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '极限一上涨笔后死叉提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【多单】 趋势背驰提损
            if kline.cur_duan.direction == 1 \
                    and kline.cur_duan.end == kline.cur_bi.start \
                    and self.fit_xt_signal(kline=kline,
                                           signal_values=[ChanSignals.Q1S0.value],
                                           look_back=slice(-2, -1)) \
                    and stop_price < float(kline.cur_duan.high) - 2 * kline.bi_height_ma():
                # 止损价，移动至当前笔的底部,或者线段高点回落2个平均笔高度，或者均线
                stop_price = float(kline.cur_duan.high) - 2 * kline.bi_height_ma()
                if stop_price < open_price:
                    stop_price = open_price + 2 * self.price_tick
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '趋势1卖提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【多单】 趋势类二卖提损
            if kline.cur_bi.direction == -1 \
                    and (self.fit_xt_signal(kline=kline,
                                            signal_values=[ChanSignals.Q2S0.value],
                                            look_back=slice(-2, -1)) \
                         or kline.get_xt_signal(xt_name='xt_2nd_signals', x=1) == ChanSignals.Q2S0.value) \
                    and stop_price < max(float(kline.bi_list[-2].low), kline.line_ma2[-1]):
                # 止损价，移动至当前笔的底部,或者线段高点回落2个平均笔高度，或者均线
                stop_price = max(float(kline.bi_list[-2].low), kline.line_ma2[-1])
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '趋势类2卖提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 三角形收敛提损
            if kline.cur_duan.direction == 1\
                and kline.bi_list[-4].start == kline.cur_duan.end \
                and kline.bi_list[-4].low <= kline.bi_list[-2].low \
                and kline.bi_list[-3].high > kline.bi_list[-1].high\
                and stop_price < float(kline.cur_bi.low):
                # 止损价，移动至当前笔的底部
                stop_price = float(kline.cur_bi.low)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '三角收敛提损',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【多单】下跌线段站稳提损：上涨过程中，出现回调线段，此时，向上一笔+死叉，提升止损
            if kline.cur_duan.direction == -1 \
                    and kline.cur_duan.start > last_signal_time \
                    and kline.cur_duan.low == kline.cur_bi.low \
                    and len(kline.cur_duan.bi_list)>=3 \
                    and kline.cur_bi.direction == 1 \
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

            # 【多单】 超涨一笔后v反破快均线
            if kline.cur_duan.direction == 1 \
                and kline.cur_bi.start == kline.cur_duan.end \
                and kline.cur_duan.bi_list[-1].height > 2 * kline.bi_height_ma()\
                and kline.cur_bi.low < kline.line_ma1[-1]:
                # 止损价，移动至当前笔的底部
                stop_price = float(kline.cur_bi.low)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '超涨一笔v反破均线',
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【多单】三卖信号提损，实时出现三卖信号，立刻止损价提高到笔的下方
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

            # 【多单】获取五笔形态的倒1笔的信号
            xt_5_signal = kline.get_xt_signal(xt_name='xt_5_signals', x=1)
            signal_leave = False
            stop_leave = False
            leave_type = None

            # 【多单】次级别的卖点判断，需要遵循本级别的买点判断
            # 使用线段判断是否存在三买,如果存在，则不离场
            duan_xt_leave = True
            if len(kline.duan_list) > 5:
                duan_xt_signal = check_chan_xt(kline, kline.duan_list[-5:])
                if duan_xt_signal in [ChanSignals.LI0.value]:
                    duan_xt_leave = False

            # 【多单】倒1笔为三类卖点形态，且死叉; 或者下破止损点
            if duan_xt_leave \
                    and xt_5_signal.get('signal') == ChanSignals.SI0.value \
                    and kline.ma12_count < 0 \
                    and xt_5_signal.get('start', '') > last_signal_time:
                leave_type = '三卖+死叉'
                self.write_log(u'{} => {} 做多信号离场'.format(leave_type, kline_name))
                signal_leave = True

            if stop_price and stop_price > kline.cur_price:
                leave_type = '止损'
                self.write_log(u'{} => {} 做多信号离场'.format(leave_type, kline_name))
                stop_leave = True

            # 【多单】满足离场信号
            if signal_leave or stop_leave:

                # 移除事务
                self.policy.signals.pop(kline_name, None)

                # 移除做多信号
                if kline_name in self.policy.long_klines:
                    self.policy.long_exit_dt = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    self.policy.long_exit_price = self.cur_99_price
                    self.write_log(f'{kline_name} => 做多信号移除,离场时间{self.policy.long_exit_dt},离场价格:{self.cur_99_price}')
                    self.policy.long_klines.remove(kline_name)

                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": leave_type,
                    "signal": f'{kline.name}.long',
                    "stop_price": stop_price
                }
                self.save_dist(d)
                return True

        if last_signal == "short":
            # 持有做空事务,进行更新
            if kline_name in self.policy.short_klines:
                # 更新收益率
                if init_price:
                    profit = init_price - kline.cur_price
                    profit_rate = round(profit / init_price, 4)
                    signal.update({'profit_rate': profit})

            # 【空单】保本提损：开仓后，有新的中枢产生，且价格比开仓价格低
            if stop_price and stop_price > open_price \
                    and kline.cur_bi_zs\
                    and kline.cur_bi_zs.start > last_signal_time \
                    and self.cur_99_price < float(kline.cur_bi_zs.high) < open_price\
                    and open_price - float(kline.cur_bi.low) > 1.6 * float(kline.bi_height_ma()):
                # 保本+一点点利润
                stop_price = open_price - (open_price - kline.cur_bi_zs.high) * 0.18
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '保本提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】保本提损：开仓后，有新的笔中点价格比开仓价格低
            if stop_price and stop_price > open_price \
                    and self.cur_99_price < kline.bi_list[-2].middle < open_price \
                    and kline.cur_bi.direction == 1 \
                    and open_price - float(kline.cur_bi.low) > 1.6 * float(kline.bi_height_ma()):
                # 保本+一点点利润
                stop_price = open_price - (open_price - float(kline.cur_bi.low)) * 0.18
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '保本提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】保本提损：开仓后，有新的笔中点价格比开仓价格低
            if stop_price and stop_price > open_price \
                    and self.cur_99_price < kline.cur_bi.middle < open_price \
                    and kline.cur_bi.direction == -1 \
                    and check_bi_not_rt(kline, Direction.SHORT) \
                    and open_price - float(kline.cur_bi.low) > 2 * float(kline.bi_height_ma()):
                # 保本+一点点利润
                stop_price = open_price - (open_price - float(kline.cur_bi.low)) * 0.18
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '保本提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】中枢提损：如果止损价高于当前中枢顶部，且当前下跌笔出现在中枢下方1.5均高
            if stop_price and kline.cur_bi_zs \
                    and stop_price > float(kline.cur_bi_zs.high) \
                    and kline.cur_bi.low < kline.cur_bi_zs.low - 1.5 * kline.bi_height_ma() \
                    and kline.cur_bi_zs.end >= kline.bi_list[-3].start \
                    and kline.cur_bi.direction == -1 \
                    and check_duan_not_rt(kline, Direction.SHORT) \
                    and last_signal_time \
                    and kline.cur_bi.start > last_signal_time:
                # 止损价，移动至当前中枢顶部
                stop_price = float(kline.cur_bi_zs.high)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '中枢提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】反转信号1提损：两个以上下跌笔中枢，最后一个线段[单笔]反抽中枢之上
            if stop_price and kline.cur_bi_zs\
                    and stop_price > float(kline.cur_bi.high) \
                    and self.fit_zs_signal(kline, signal_direction=Direction.LONG) \
                    and kline.cur_duan.direction == 1 \
                    and len(kline.cur_duan.bi_list) == 1 \
                    and kline.cur_duan.low < kline.cur_bi_zs.high < kline.cur_duan.high \
                    and check_bi_not_rt(kline, direction=Direction.SHORT) \
                    and kline.cur_duan.end == kline.cur_bi.start \
                    and float(kline.cur_bi.low) > kline.line_ma1[-1]:
                # 止损价，移动至当前分笔顶部
                stop_price = float(kline.cur_bi.high)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '反转信号1提损',
                     "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】 极限一下跌笔后金叉
            if kline.cur_duan.direction == -1 \
                    and kline.cur_duan.end <= kline.cur_bi.start \
                    and kline.cur_duan.bi_list[-1].height > 1.6 * kline.bi_height_ma() \
                    and len(kline.ma12_cross_list) > 1\
                    and kline.cur_duan.bi_list[-1].start > kline.ma12_cross_list[-2].get('datetime', "") \
                    and kline.cur_bi.direction == -1 \
                    and kline.ma12_count > 0 \
                    and stop_price > float(kline.cur_duan.bi_list[-1].high) \
                    and float(kline.cur_bi.high) > kline.line_ma2[-1]:
                # 止损价，移动至当前笔的顶部
                stop_price = float(kline.cur_bi.high)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '极限一下跌笔后金叉提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】 趋势1买背驰提损
            if kline.cur_duan.direction == -1 \
                    and kline.cur_duan.end == kline.cur_bi.start \
                    and self.fit_xt_signal(kline=kline,
                                           signal_values=[ChanSignals.Q1L0.value],
                                           look_back=slice(-2, -1)) \
                    and stop_price > float(kline.cur_duan.low) + 2 * kline.bi_height_ma():
                # 止损价，移动至当前笔的顶部,或者线段低点回升2个平均笔高度，或者均线2
                stop_price = float(kline.cur_duan.low) + 2 * kline.bi_height_ma()
                if stop_price > open_price:
                    stop_price = open_price - 2 * self.price_tick
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '趋势1买提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】 趋势类二买提损
            if kline.cur_bi.direction == 1 \
                    and (self.fit_xt_signal(kline=kline,
                                            signal_values=[ChanSignals.Q2L0.value],
                                            look_back=slice(-2, -1)) \
                         or kline.get_xt_signal(xt_name='xt_2nd_signals', x=1) == ChanSignals.Q2L0.value) \
                    and stop_price > min(float(kline.bi_list[-2].high), kline.line_ma2[-1]):
                # 止损价，移动至当前笔的顶部,或者均线
                stop_price = min(float(kline.bi_list[-2].high), kline.line_ma2[-1])
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '趋势类2买提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 三角形收敛提损
            if kline.cur_duan.direction == -1 \
                    and kline.bi_list[-4].start == kline.cur_duan.end \
                    and kline.bi_list[-4].high >= kline.bi_list[-2].high \
                    and kline.bi_list[-3].low < kline.bi_list[-1].low \
                    and stop_price > float(kline.cur_bi.high):
                # 止损价，移动至当前笔的顶部
                stop_price = float(kline.cur_bi.high)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '三角收敛提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】上涨线段站稳提损：下跌过程中，出现回调线段，此时，向下一笔+金叉，提升止损
            if kline.cur_duan.direction == 1 \
                    and kline.cur_duan.start > last_signal_time \
                    and kline.cur_duan.high == kline.cur_bi.high \
                    and check_bi_not_rt(kline, Direction.SHORT) \
                    and kline.ma12_count > 0 \
                    and stop_price > float(kline.cur_bi.high):
                # 止损价，移动至当前笔的顶部
                stop_price = float(kline.cur_bi.high)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '上涨线段站稳提损',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】 超跌一笔后v反破快均线
            if kline.cur_duan.direction == -1 \
                    and kline.cur_bi.start == kline.cur_duan.end \
                    and kline.cur_duan.bi_list[-1].height > 2 * kline.bi_height_ma() \
                    and kline.cur_bi.high > kline.line_ma1[-1]:
                # 止损价，移动至当前笔的底部
                stop_price = float(kline.cur_bi.high)
                signal.update({'stop_price': stop_price})
                self.policy.signals.update({kline_name: signal})
                d = {
                    "datetime": self.cur_datetime,
                    "price": stop_price,
                    "operation": '超跌一笔v反破均线',
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)

            # 【空单】三买信号提损，实时出现三买信号，立刻止损价提高到笔的上方
            if check_bi_not_rt(kline, Direction.SHORT) \
                    and kline.get_xt_signal(xt_name='xt_5_signals').get('signal') == ChanSignals.LI0.value:
                if stop_price and stop_price > kline.cur_bi.high:
                    stop_price = float(kline.cur_bi.high)
                    signal.update({'stop_price': stop_price})
                    self.policy.signals.update({kline_name: signal})
                    d = {
                        "datetime": self.cur_datetime,
                        "price": stop_price,
                        "operation": '三买提损',
                        "signal": f'{kline.name}.short',
                        "stop_price": stop_price
                    }
                    self.save_dist(d)

            # 【空单】获取五笔形态的倒1笔的信号
            xt_5_signal = kline.get_xt_signal(xt_name='xt_5_signals', x=1)
            signal_leave = False
            stop_leave = False
            leave_type = None

            # 【空单】次级别的买点判断，需要遵循本级别的卖点判断
            # 使用线段判断是否存在三卖,如果存在，则不离场
            duan_xt_leave = True
            if len(kline.duan_list) > 5:
                duan_xt_signal = check_chan_xt(kline, kline.duan_list[-5:])
                if duan_xt_signal in [ChanSignals.SI0.value]:
                    duan_xt_leave = False

            # 【空单】倒1笔为三类买点形态，且金叉; 或者上破止损点
            if duan_xt_leave \
                    and xt_5_signal.get('signal') == ChanSignals.LI0.value \
                    and kline.ma12_count > 0 \
                    and xt_5_signal.get('start', '') > last_signal_time:
                leave_type = '三买+金叉'
                self.write_log(u'{} => {} 做空信号离场'.format(leave_type, kline_name))
                signal_leave = True

            if stop_price and stop_price < kline.cur_price:
                leave_type = '止损'
                self.write_log(u'{} => {} 做空信号离场'.format(leave_type, kline_name))
                stop_leave = True

            # 【空单】满足离场信号
            if signal_leave or stop_leave:

                # 移除事务
                self.policy.signals.pop(kline_name, None)

                # 移除做空信号
                if kline_name in self.policy.short_klines:
                    self.policy.short_exit_dt = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    self.policy.short_exit_price = self.cur_99_price
                    self.write_log(f'{kline_name} => 做空信号移除,离场时间{self.policy.short_exit_dt},离场价格:{self.cur_99_price}')
                    self.policy.short_klines.remove(kline_name)

                d = {
                    "datetime": self.cur_datetime,
                    "price": kline.cur_price,
                    "operation": leave_type,
                    "signal": f'{kline.name}.short',
                    "stop_price": stop_price
                }
                self.save_dist(d)
                return True

        return False

    def on_bar_k(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        for kline_name in self.bar_names:
            # 优先进行平仓
            self.tns_close_logic(kline_name)

            # 如果没有平仓，判断开仓
            self.tns_open_logic(kline_name)

    def on_bar_x(self, *args, **kwargs):
        """
        K线数据
        :param bar: 预定的周期Bar
        :return:
        """
        if not self.inited or not self.trading:
            return

        for kline_name in list(self.policy.signals.keys()):
            signal = self.policy.signals.get(kline_name, None)
            kline = self.klines.get(kline_name)
            last_signal = signal.get('last_signal', None)
            last_signal_time = signal.get('last_signal_time', None)
            tns_status = signal.get('tns_status', None)
            init_price = signal.get('init_price', None)
            stop_price = signal.get('stop_price', None)
            # 多头信号，处于观测状态
            if last_signal == 'long':

                if tns_status == TNS_STATUS_OPENED:


                    # 如果持仓期间发现止损行为，退出
                    if stop_price and self.cur_99_price < stop_price and kline.close_array[0] < stop_price:
                        if kline_name in self.policy.long_klines:
                            self.policy.long_exit_dt = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')
                            self.policy.long_exit_price = self.cur_99_price
                            self.policy.long_klines.remove(kline_name)
                        self.policy.signals.pop(kline_name, None)
                        self.write_log(f'{kline_name} 做多事务在{self.kline_x.name},价格:{self.cur_99_price}止损离场')
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": 'klinex 多单离场',
                            "signal": f'{kline_name}.long',
                            "stop_price": stop_price
                        }
                        self.save_dist(d)
                        self.policy.save()


                        cross_price = kline.ma12_cross_list[-1].get('cross')
                        if cross_price and kline.ma12_count < 0 and self.cur_99_price < cross_price:
                            signal = {'last_signal': 'short',
                                      'last_signal_time': self.cur_datetime,
                                      'signal_name': '反手空单',
                                      'tns_status': TNS_STATUS_OBSERVATE,
                                      'kline_name': kline_name,
                                      'stop_price': max(float(cross_price), kline.bi_height_ma()),
                                      'open_price': kline.cur_price,
                                      'init_price': kline.cur_price,
                                      'bi_start': kline.bi_list[-1].start,
                                      'opened': False,
                                      'profit_rate': 0}

                            self.policy.signals[kline_name] = signal
                            self.policy.save()

                    continue
                # if tns_status in [TNS_STATUS_OBSERVATE]
                # 如果期间发现止损行为，退出
                if stop_price and self.cur_99_price < stop_price:
                    continue

                # 判断基础K线，是否出现买点
                if self.fit_xt_signal(
                        kline=self.kline_x,
                        signal_values=[ChanSignals.LA0.value,
                                       ChanSignals.LI0.value,
                                       ChanSignals.Q1L0.value,
                                       ChanSignals.Q2L0.value,
                                       ChanSignals.Q3L0.value],
                        bi_num_list=[5, 7, 9, 11, 13],
                        look_back=slice(-3, -1)):
                    tns_status = TNS_STATUS_OPENED
                    signal.update({'tns_status': tns_status,
                                   'open_price': self.cur_99_price})

                    if kline_name not in self.policy.long_klines:
                        self.policy.long_klines.append(kline_name)
                    if kline_name in self.policy.short_klines:
                        self.policy.short_klines.remove(kline_name)

                    self.policy.signals.update({kline_name: signal})
                    self.write_log(f'{kline_name} 做多信号进场,[{last_signal_time},价格:{init_price}] =>' +
                                   f'[{self.cur_datetime}, 价格:{self.cur_99_price}] ')
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": 'klinex开多',
                        "signal": f'{kline_name}.long',
                        "stop_price": stop_price
                    }
                    self.save_dist(d)
                    self.policy.save()

            # 空头信号，处于观测状态
            if last_signal == 'short':

                # 持仓时，就判断是否止损退出事务
                if tns_status in [TNS_STATUS_OPENED]:
                    if stop_price and self.cur_99_price > stop_price and kline.close_array[-1] > stop_price:
                        if kline_name in self.policy.short_klines:
                            self.policy.short_exit_dt = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')
                            self.policy.short_exit_price = self.cur_99_price
                            self.policy.short_klines.remove(kline_name)
                        self.policy.signals.pop(kline_name, None)
                        self.write_log(f'{kline_name} 做空事务在{self.kline_x.name},价格:{self.cur_99_price}止损离场')
                        d = {
                            "datetime": self.cur_datetime,
                            "price": self.cur_99_price,
                            "operation": 'klinex 空单离场',
                            "signal": f'{kline_name}.short',
                            "stop_price": stop_price
                        }
                        self.save_dist(d)
                        self.policy.save()
                        kline = self.klines.get(kline_name)
                        cross_price = kline.ma12_cross_list[-1].get('cross')
                        if cross_price and kline.ma12_count > 0 and self.cur_99_price > cross_price:
                            signal = {'last_signal': 'long',
                                      'last_signal_time': self.cur_datetime,
                                      'signal_name': '反手多单',
                                      'tns_status': TNS_STATUS_OBSERVATE,
                                      'kline_name': kline_name,
                                      'stop_price': min(float(cross_price), self.cur_99_price - kline.bi_height_ma()),
                                      'open_price': kline.cur_price,
                                      'init_price': kline.cur_price,
                                      'bi_start': kline.bi_list[-1].start,
                                      'opened': False,
                                      'profit_rate': 0}

                            self.policy.signals[kline_name] = signal
                            self.policy.save()

                    continue

                # 以下是判断 观测 => 小周期寻找卖点 => 开空
                # 如果期间发现止损行为，退出
                if stop_price and self.cur_99_price > stop_price:
                    continue

                # 判断基础K线，是否出现卖点
                if self.fit_xt_signal(
                        kline=self.kline_x,
                        signal_values=[ChanSignals.SA0.value,
                                       ChanSignals.SI0.value,
                                       ChanSignals.Q1S0.value,
                                       ChanSignals.Q2S0.value,
                                       ChanSignals.Q3S0.value],
                        bi_num_list=[5, 7, 9, 11, 13],
                        look_back=slice(-3, -1)):
                    tns_status = TNS_STATUS_OPENED
                    signal.update({'tns_status': tns_status,
                                   'open_price': self.cur_99_price})

                    if kline_name not in self.policy.short_klines:
                        self.policy.short_klines.append(kline_name)
                    if kline_name in self.policy.long_klines:
                        self.policy.long_klines.remove(kline_name)

                    self.policy.signals.update({kline_name: signal})
                    self.write_log(f'{kline_name} 做空信号进场,[{last_signal_time},价格:{init_price}] =>' +
                                   f'[{self.cur_datetime}, 价格:{self.cur_99_price}] ')
                    d = {
                        "datetime": self.cur_datetime,
                        "price": self.cur_99_price,
                        "operation": 'kline_x开空',
                        "signal": f'{kline_name}.short',
                        "stop_price": stop_price
                    }
                    self.save_dist(d)

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
            target_volume = round_to(int(max_volume * net_kline_count / self.kline_count),
                                     self.cta_engine.get_volume_tick(self.vt_symbol))

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

        self.long_exit_dt = ""     # 做多信号的止损离场时间
        self.long_exit_price = 0   # 做多信号的止损离场价格

        self.short_exit_dt = ""    # 做空信号的止损离场时间
        self.short_exit_price = 0  # 做空信号的止损离场价格


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

        j['long_exit_dt'] = self.long_exit_dt
        j['long_exit_price'] = self.long_exit_price
        j['short_exit_dt'] = self.short_exit_dt
        j['short_exit_price'] = self.short_exit_price

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

        self.long_exit_dt = json_data.get('long_exit_dt', "")
        self.long_exit_price = json_data.get('long_exit_price', 0)
        self.short_exit_dt = json_data.get('short_exit_dt', "")
        self.short_exit_price = json_data.get('short_exit_price', 0)

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
