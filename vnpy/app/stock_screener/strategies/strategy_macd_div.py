from vnpy.app.stock_screener.template import *
from vnpy.component.cta_line_bar import CtaMinuteBar, CtaHourBar, CtaDayBar,CtaWeekBar, get_cta_bar_type
from vnpy.component.cta_policy import CtaPolicy
from vnpy.trader.utility import append_data
import pandas as pd
import os
import numpy as np

class StrategyMacdDiv(ScreenerTemplate):
    """
    双周期MACD底背离共振信号选股策略
    """
    author = u'大佳'
    small_bar = 'D1'
    big_bar = 'W1'
    all_stocks = False
    exchange = None
    exclude_st = True
    vt_symbols = []
    start_date = None
    end_date = None
    parameters = ['small_bar', 'big_bar','vt_symbols', 'all_stocks', 'exchange', 'start_date', 'end_date', 'exclude_st']

    def __init__(
            self,
            engine: Any,
            strategy_name: str,
            setting: dict,
    ):
        """
        构造函数
        :param engine:
        :param strategy_name:
        :param setting:
        """
        super().__init__(engine, strategy_name, setting)

        self.policy = StockPolicy(self)

        if isinstance(self.exchange, str):
            self.exchange = Exchange(self.exchange)

        if self.all_stocks and len(self.vt_symbols) == 0:
            self.vt_symbols = self.engine.get_all_vt_symbols(exchange=Exchange(self.exchange) if self.exchange else None)

        self.klines = {}

    def on_init(self):
        """
        策略初始化
        :return:
        """
        if self.inited:
            return

        self.inited = True

    def on_start(self):
        """
        策略开始运行
        :return:
        """
        if self.running:
            return

        try:
            self.run()
        except Exception as ex:
            self.write_error(f'选股异常:{str(ex)}')
            self.write_error(traceback.format_exc())

    def on_bar_x(self, **kwargs):
        pass

    def run(self):
        """
        逐一执行选股
        :return:
        """

        export_file = os.path.abspath(os.path.join(
            self.engine.get_data_path(),
            '{}_{}.csv'.format(self.strategy_name, datetime.now().strftime('%Y-%m-%d'))))

        if os.path.exists(export_file):
            self.write_log(f'移除旧得csv文件:{export_file}')
            os.remove(export_file)

        progress = 0
        c = 0
        n = len(self.vt_symbols)
        for vt_symbol in self.vt_symbols:

            # 排除ST得股票
            stock_name = self.engine.get_name(vt_symbol)
            if self.exclude_st and ('ST' in stock_name or '退' in stock_name):
                continue

            try:
                # 创建K线，恢复数据
                load_bar_names = self.get_klines(vt_symbol=vt_symbol, bar_names=[self.small_bar,self.big_bar])

                # 执行选个股信号
                if len(load_bar_names) == 2:
                    ret = self.calculate_signal(vt_symbol=vt_symbol)

                    if ret:
                        # 满足信号，输出到csv文件中
                        result = {'vt_symbol': vt_symbol,
                                  'name': stock_name,
                                  'bar_names': load_bar_names,
                                  'signal': 'macd div',
                                  'datetime': self.klines[load_bar_names[0]].cur_datetime}

                        self.results.append(
                            result
                        )
                        append_data(file_name=export_file, dict_data=result)

                    # 更新缓存=》文件
                    self.save_klines_to_cache(symbol=vt_symbol.split('.')[0], kline_names=load_bar_names)

                    # 移除 kline
                    [self.klines.pop(kline_name, None) for kline_name in load_bar_names]

            except Exception as ex:
                self.write_error(f'处理{vt_symbol}异常')

            c += 1
            new_progress = c * 100 / n
            # if int(new_progress) != int(progress):
            progress = new_progress
            self.write_log(f'当前进度:{progress}%')

        if progress > 99:
            self.running = False
            msg = f'{self.strategy_name}运行运行，一共:{len(self.results)}条结果'
            self.engine.send_wechat(msg)
            self.write_log(msg)
            self.completed = True

    def get_klines(self, vt_symbol, bar_names):
        """
        获取合约得K线
        :param vt_symbol:
        :param bar_names:
        :return: [kline_names]
        """
        stock_name = self.engine.get_name(vt_symbol)
        symbol, ex = extract_vt_symbol(vt_symbol)
        interval = Interval.MINUTE
        pre_load_days = 180

        # 为vt_symbol 创建 bar_names 得K线
        load_kline_names = []
        for bar_name in bar_names:
            # 获取K线
            bar_class, bar_interval = get_cta_bar_type(bar_name)
            if bar_class == CtaMinuteBar:
                interval = Interval.MINUTE
                pre_load_days = max(pre_load_days, 180)
            elif bar_class == CtaHourBar:
                interval = Interval.HOUR
                pre_load_days = max(pre_load_days,360)
            elif bar_class == CtaDayBar:
                interval = Interval.DAILY
                pre_load_days = max(pre_load_days, 720)
            elif bar_class == CtaWeekBar:
                interval = Interval.DAILY
                pre_load_days = max(pre_load_days, 1420)

            kline_name = f'{vt_symbol}_{bar_name}'
            kline_setting = {}
            kline_setting['name'] = f'{vt_symbol}_{bar_name}'  # k线名称
            kline_setting['bar_interval'] = bar_interval  # X K线得周期
            kline_setting['para_macd_fast_len'] = 12
            kline_setting['para_macd_slow_len'] = 26
            kline_setting['para_macd_signal_len'] = 9
            kline_setting['para_active_chanlun'] = True
            kline_setting['price_tick'] = 0.01  # 合约最小跳动
            kline = bar_class(self, self.on_bar_x, kline_setting)
            self.klines[kline_name] = kline

            load_kline_names.append(kline_name)

        last_bar_dt = None
        if not self.check_adjust(vt_symbol):
            # 从缓存中获取K线
            last_bar_dt = self.load_klines_from_cache(symbol=symbol, kline_names=load_kline_names)

        if isinstance(last_bar_dt,datetime):
            pre_load_days = (datetime.now() - last_bar_dt).days + 1
        # 获取历史bar
        self.write_log(f'开始获取{vt_symbol}[{stock_name}]bar数据')

        bars = self.engine.get_bars(
            vt_symbol=vt_symbol,
            days=pre_load_days,
            interval=interval,
            interval_num=1
        )

        if len(bars) == 0:
            return load_kline_names

        try:
            self.write_log('推送至K线[{}]，共{}根bar'.format(load_kline_names, len(bars)))
            for bar in bars:
                if np.isnan(bar.close_price):
                    continue
                for kline_name in load_kline_names:
                    kline = self.klines.get(kline_name)
                    kline.add_bar(bar)
        except Exception as ex:
            self.write_error(f'推送失败:{str(ex)}')
            self.write_error(traceback.format_exc())

        return load_kline_names

    def calculate_signal(self, vt_symbol):
        """
        搜索信号
        :param symbol: 股票
        :return:
        """
        # 大周期bar名称、小周期bar名称
        big_bar_name = f'{vt_symbol}_{self.big_bar}'
        small_bar_name = f'{vt_symbol}_{self.small_bar}'

        big_kline = self.klines.get(big_bar_name,None)   # 大周期K线
        small_kline = self.klines.get(small_bar_name, None)  # 小周期K线

        # 排除
        if big_kline is None or small_kline is None:
            return False

        # 大周期未处于下跌段
        if big_kline.cur_duan is None or big_kline.cur_duan.direction == 1:
            return False

        # 小周期也是下跌段
        if small_kline.cur_duan and small_kline.cur_duan.direction == 1:
            return False

        if big_kline.cur_duan.end < small_kline.cur_duan.start:
            return False

        if len(big_kline.macd_segment_list) < 3:
            return False

        tre_seg, pre_seg, cur_seg = big_kline.macd_segment_list[-3:]

        # macd 下跌segment，开始时间，需要在下跌线段内
        if tre_seg['start'] < big_kline.cur_duan.start:
            return False
        # 最后一个segment，处于下跌、且未收口状态
        if cur_seg['macd_count'] > 0:
            return False

        # 周线 dif 指标背离 或者 macd绿柱背离  vs 价格新低
        if (tre_seg['min_dif'] > cur_seg['min_dif'] or tre_seg['min_macd'] > cur_seg['min_macd'])\
                and tre_seg['min_price'] > cur_seg['min_price']:

            is_fx_div = small_kline.is_fx_macd_divergence(direction =Direction.SHORT, cur_duan = small_kline.cur_duan)

            is_macd_div = False
            # 重新计算, 最后一个segment是做多的，所以从倒数4取三个segment
            tre_seg, pre_seg, cur_seg = small_kline.macd_segment_list[-4:-1]
            if tre_seg['start'] > small_kline.cur_duan.start \
                        and (tre_seg['min_dif'] > cur_seg['min_dif'] or tre_seg['min_macd'] > cur_seg['min_macd'])\
                        and tre_seg['min_price'] > cur_seg['min_price'] \
                        and cur_seg['macd_count'] < 0:
                is_macd_div = True

            # 任一满足，即返回True
            if is_fx_div or is_macd_div:
                return True

        return False


class StockPolicy(CtaPolicy):

    def __init__(self, strategy):
        super().__init__(strategy)
        self.cur_trading_date = None  # 已执行pre_trading方法后更新的当前交易日
        self.signals = {}  # kline_name: { 'last_signal': '', 'last_signal_time': datetime }
        self.sub_tns = {}  # 子事务

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)

        self.cur_trading_date = json_data.get('cur_trading_date', None)
        self.sub_tns = json_data.get('sub_tns', {})
        signals = json_data.get('signals', {})
        for k, signal in signals.items():
            last_signal = signal.get('last_signal', "")
            str_ast_signal_time = signal.get('last_signal_time', "")
            try:
                if len(str_ast_signal_time) > 0:
                    last_signal_time = datetime.strptime(str_ast_signal_time, '%Y-%m-%d %H:%M:%S')
                else:
                    last_signal_time = None
            except Exception as ex:
                last_signal_time = None
            self.signals.update({k: {'last_signal': last_signal, 'last_signal_time': last_signal_time}})

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['cur_trading_date'] = self.cur_trading_date
        j['sub_tns'] = self.sub_tns
        d = {}
        for kline_name, signal in self.signals.items():
            last_signal_time = signal.get('last_signal_time', None)
            c_signal = {}
            c_signal.update(signal)
            c_signal.update({'last_signal': signal.get('last_signal', ''),
                             'last_signal_time': last_signal_time.strftime(
                                 '%Y-%m-%d %H:%M:%S') if last_signal_time is not None else ""
                             })
            d.update({kline_name: c_signal})
        j['signals'] = d
        return j
