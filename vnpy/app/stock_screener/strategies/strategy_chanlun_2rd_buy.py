from vnpy.app.stock_screener.template import *
from vnpy.component.cta_line_bar import CtaLineBar, CtaMinuteBar, CtaHourBar, CtaDayBar, get_cta_bar_type
from vnpy.component.cta_policy import CtaPolicy
from vnpy.component.cta_utility import *
from vnpy.trader.utility import append_data
import numpy as np
import os


class StrategyChanlunSecondBuy(ScreenerTemplate):
    """
    缠论二买信号选股策略
    """
    author = u'大佳'
    bar_name = 'M30'
    all_stocks = False
    exchange = None
    exclude_st = True
    vt_symbols = []
    start_date = None
    end_date = None
    parameters = ['bar_name', 'vt_symbols', 'all_stocks', 'exchange', 'start_date', 'end_date', 'exclude_st']

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
            self.vt_symbols = self.engine.get_all_vt_symbols(self.exchange)
        self.klines = {}

        # M30 => CtaMinuteBar，30    D1 =》 CtaDayBar, 1
        self.bar_class, self.bar_interval = get_cta_bar_type(self.bar_name)

        # 最好筹齐2000根bar
        if self.bar_class == CtaMinuteBar:
            self.interval = Interval.MINUTE
            self.pre_load_days = 300
        elif self.bar_class == CtaHourBar:
            self.interval = Interval.HOUR
            self.pre_load_days = 600
        elif self.bar_class == CtaDayBar:
            self.interval = Interval.DAILY
            self.pre_load_days = 1200
        else:
            raise Exception(f'{self.bar_name} 类型不支持')

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

        export_file = os.path.abspath(os.path.join(self.engine.get_data_path(), '{}_{}.csv'.format(self.strategy_name,
                                                                                                   datetime.now().strftime(
                                                                                                       '%Y-%m-%d'))))

        if os.path.exists(export_file):
            self.write_log(f'移除旧得csv文件:{export_file}')
            os.remove(export_file)

        progress = 0
        c = 0
        n = len(self.vt_symbols)
        for vt_symbol in self.vt_symbols:
            stock_name = self.engine.get_name(vt_symbol)
            if self.exclude_st and ('ST' in stock_name or '退' in stock_name):
                continue
            symbol, ex = extract_vt_symbol(vt_symbol)

            kline = self.get_kline(vt_symbol)

            # 判断是否有信号
            ret, signal = self.calculate_signal(kline=kline, vt_symbol=vt_symbol)
            if ret and signal:
                # 有二买信号，添加记录
                result = {'vt_symbol': vt_symbol,
                          'name': self.engine.get_name(vt_symbol),
                          'bar_name': self.bar_name,
                          'datetime': kline.cur_datetime}
                result.update(signal)

                self.results.append(
                    result
                )
                # 追加到csv文件
                fields = ['vt_symbol', 'name', 'bar_name', 'datetime',
                          'signal', 'type', 'locate',
                          'bi_start', 'bi_end',
                          'bi_low', 'pre_low']
                append_data(file_name=export_file, dict_data=result, field_names=fields)

                # # 输出截图至文件
                # data = kline.get_data()
                # snapshot = {
                #     'strategy': "demo",
                #     'datetime': datetime.now(),
                #     "kline_names": [kline.name],
                #     "klines": {kline.name: data}}
                # ui = UiSnapshot()
                # png_folder = os.path.abspath(os.path.join(self.engine.get_data_path(), 'png'))
                # if not os.path.exists(png_folder):
                #     os.makedirs(png_folder)
                # export_png_file = os.path.abspath(
                #     os.path.join(self.engine.get_data_path(),
                #                  'png',
                #                  '{}_{}_{}.png'.format(
                #                     self.strategy_name,
                #                     datetime.now().strftime('%Y-%m-%d'),
                #                     vt_symbol)))
                # # 保存切片到截图文件
                # ui.export(snapshot_file="", d=snapshot, export_file=export_png_file)
                # # 保存截图文件=》 excel中
                # if os.path.exists(export_png_file):
                #     png_excel = os.path.abspath(os.path.join(self.engine.get_data_path(), '{}_{}.xlsx'.format(self.strategy_name,
                #                                                                                    datetime.now().strftime(
                #                                                                              '%Y-%m-%d'))))
                #     save_images_to_excel(file_name=png_excel,sheet_name=vt_symbol,image_names=[export_png_file])
                #     # 移除临时png文件
                #     os.remove(export_png_file)
            else:
                # 移除kline
                self.klines.pop(kline.name, None)

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

    def get_kline(self, vt_symbol):
        """
        自动创建、获取K线
        :param vt_symbol:
        :return:
        """
        stock_name = self.engine.get_name(vt_symbol)
        symbol, ex = extract_vt_symbol(vt_symbol)
        # 创建K线
        kline_name = f'{vt_symbol}_{self.bar_name}'
        # kline = self.klines.get(kline_name, None)
        kline_setting = {}
        kline_setting['name'] = f'{vt_symbol}_{self.bar_name}'  # k线名称
        kline_setting['bar_interval'] = self.bar_interval  # X K线得周期
        kline_setting['para_pre_len'] = 60
        kline_setting['para_ma1_len'] = 55
        kline_setting['para_ma2_len'] = 89
        kline_setting['para_macd_fast_len'] = 12
        kline_setting['para_macd_slow_len'] = 26
        kline_setting['para_macd_signal_len'] = 9
        kline_setting['para_active_chanlun'] = True
        kline_setting['para_active_chan_xt'] = True
        kline_setting['price_tick'] = 0.01  # 合约最小跳动
        kline_setting['is_stock'] = True
        kline = self.bar_class(self, self.on_bar_x, kline_setting)
        self.klines[kline_name] = kline

        last_bar_dt = None
        if self.check_adjust(vt_symbol):
            # 从缓存中获取K线
            last_bar_dt = self.load_klines_from_cache(symbol=symbol, kline_names=[kline_name])

        if isinstance(last_bar_dt, datetime):
            self.pre_load_days = (datetime.now() - last_bar_dt).days + 1

        # 获取历史bar
        self.write_log(f'开始获取{vt_symbol}[{stock_name}]bar数据')
        bars = self.engine.get_bars(
            vt_symbol=vt_symbol,
            days=self.pre_load_days,
            interval=self.interval,
            interval_num=self.bar_interval
        )

        if len(bars) == 0:
            return kline

        self.write_log('推送{}K线，共{}根bar'.format(vt_symbol, len(bars)))

        # 逐一推送bar
        for bar in bars:
            if np.isnan(bar.close_price):
                continue
            kline.add_bar(bar, bar_is_completed=True)

        # 更新缓存=》文件
        self.save_klines_to_cache(symbol=vt_symbol.split('.')[0])

        return kline

    def calculate_signal(self, kline: CtaLineBar, vt_symbol: str):
        """
        搜索信号
        :param kline:
        :param vt_symbol:
        :return:
        """
        # 计算逻辑
        if not kline.tre_duan or not kline.cur_bi_zs:
            return False, None

        signal_name = None
        signal = {}
        for x in [0, 1]:

            trend_2nd_signal = kline.get_xt_signal(f'xt_2nd_signals', x)
            if trend_2nd_signal is not None and trend_2nd_signal.get('signal') == ChanSignals.Q2L0.value:
                signal_name = f'倒{x}笔_{ChanSignals.Q2L0.value}'
                signal = {
                    'signal': signal_name,
                    'type': '趋势',
                    'locate': f'倒{x}笔',
                    'bi_start': kline.bi_list[-1 - x].start,  # 二买笔的开始时间
                    'bi_end': kline.bi_list[-1 - x].end,  # 二买笔的结束时间
                    'bi_low': kline.bi_list[-1 - x].low,  # 二买笔的低点
                    'pre_low': kline.bi_list[-2 - x].low  # 一买的低点，供后续交易策略判断止损点
                }
                break

        if not signal_name:
            for n in [9, 11]:
                for x in [0, 1]:
                    xt_signal = kline.get_xt_signal(f'xt_{n}_signals', x)
                    if xt_signal is not None and xt_signal.get('signal') == ChanSignals.Q2L0.value:
                        signal_name = f'倒{x}笔_{n}分笔_{ChanSignals.Q2L0.value}'
                        signal = {
                            'signal': signal_name,
                            'type': f'{n}分笔',
                            'locate': f'倒{x}笔',
                            'bi_start': kline.bi_list[-1 - x].start,  # 二买笔的开始时间
                            'bi_end': kline.bi_list[-1 - x].end,  # 二买笔的结束时间
                            'bi_low': kline.bi_list[-1 - x].low,  # 二买笔的低点
                            'pre_low': min([bi.low for bi in kline.bi_list[-5 - x:]])  # 一买的低点，供后续交易策略判断止损点
                        }
                        break

        # 发现了二买信号
        if signal_name and len(signal) > 0:
            self.write_log(
                f'{vt_symbol},发现{signal_name}信号，'
                f'段:{kline.cur_duan.start} => {kline.cur_duan.end}, '
                f'low:{kline.cur_duan.low}, high: {kline.cur_duan.high}')
            return True, signal

        return False, None


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
