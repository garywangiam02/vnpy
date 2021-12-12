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
from vnpy.app.cta_crypto.template import CtaFutureTemplate, Direction, get_underlying_symbol, Interval
from vnpy.component.cta_policy import (
    CtaPolicy, TNS_STATUS_OBSERVATE, TNS_STATUS_READY, TNS_STATUS_ORDERING, TNS_STATUS_OPENED, TNS_STATUS_CLOSED
)
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_grid_trade import CtaGridTrade, uuid, CtaGrid
from vnpy.component.cta_line_bar import get_cta_bar_type, TickData, BarData, CtaMinuteBar, CtaHourBar, CtaDayBar


########################################################################
class StrategyGridTradeFuture_v2(CtaFutureTemplate):
    """期货网格交易策略
    # 按照当前价，往下n%开始布网格
    # 当创新高，没有网格时，重新布
    # v2:
        增加缠论线段的支持,做多为例，在出现下跌线段后，才启动开仓交易
    """
    author = u'大佳'

    # 网格数量
    grid_lots = 25
    # 网格高度百分比
    grid_height_percent = 2
    x_minute = 5  # 缠论辅助的5分钟K线
    # 策略在外部设置的参数
    parameters = [
        "activate_market",
        "max_invest_pos", "max_invest_margin", "max_invest_rate",
        "grid_height_percent","x_minute","grid_lots",
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

        self.policy = Future_Grid_Trade_Policy(self)

        # 仓位状态
        self.position = CtaPosition(strategy=self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)
        self.kline_x = None

        if setting:
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

    def on_bar_k(self, bar: BarData):
        """K线on bar事件"""
        if self.inited:
            if len(self.kline_x.duan_list) > 0:
                d = self.kline_x.duan_list[-1]
                self.write_log(f'当前段方向:{d.direction},{d.start}=>{d.end},low:{d.low},high:{d.high}')

    # ----------------------------------------------------------------------
    def on_init(self, force=False):
        """初始化"""
        self.write_log(f'{self.strategy_name}策略开始初始化')

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
        self.init_policy()
        self.display_tns()
        if not self.backtesting:
            self.init_position()  # 初始持仓数据

        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.put_event()

        self.write_log(f'{self.strategy_name} => 开始加载历史数据')
        # 从本地缓存文件中加载K线，并取得最后的bar时间
        last_bar_dt = self.load_klines_from_cache()
        dt_now = datetime.now()
        if isinstance(last_bar_dt, datetime):
            self.write_log(u'缓存数据bar最后时间:{}'.format(last_bar_dt))
            self.cur_datetime = last_bar_dt
            load_days = max((dt_now - last_bar_dt).days, 1)

        else:
            # 取 1分钟bar
            load_days = 30
            self.display_bars = False
            self.write_log(f'无本地缓存文件，取{load_days}天 bar')

        def on_bar_cb(bar, **kwargs):
            """历史数据的回调处理 =>推送到k线"""
            if last_bar_dt and bar.datetime < last_bar_dt:
                return
            self.cur_price = bar.close_price
            self.cur_datetime = bar.datetime

            if self.kline_x.cur_datetime and bar.datetime < self.kline_x.cur_datetime:
                return
            self.kline_x.add_bar(bar)

        if not self.backtesting:
            self.cta_engine.load_bar(vt_symbol=self.vt_symbol,
                                 days=load_days,
                                 interval=Interval.MINUTE,
                                 callback=on_bar_cb)
        self.inited = True
        self.write_log(f'{self.strategy_name} => init()执行完成')
        return True

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

        if not self.inited or not self.trading:
            return

        if not self.backtesting:
            self.kline_x.on_tick(tick)

        if tick.datetime.second % 10 == 0:
            self.update_grids(self.vt_symbol)

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime, reopen=False)

        if tick.datetime.second % 10 == 0:
            # 网格逐一开仓/止盈检查
            self.tns_check_grids()

        # 实盘这里是每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute

            self.display_grids()
            self.display_tns()

            self.put_event()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """
        分钟K线数据（仅用于回测时，从策略外部调用)
        :param bar:
        :return:
        """
        # 转换为tick
        tick = bar_to_tick(bar)
        self.kline_x.add_bar(bar)
        self.on_tick(tick)

    def update_grids(self, vt_symbol):
        """
        更新网格
        1. 不存在policy时，初始化该vt_symbol的policy配置，包括当前最高价（取当前价格和pre_close价格)
        2. 如果价格高于最高价，从最高价往下，构造10个网格的买入价格。
        3. 记录最后一次开仓价格。

        :return:
        """

        grid_info = self.policy.grids.get(vt_symbol, {})
        cur_price = self.cta_engine.get_price(vt_symbol)

        # 初始化/价格高于最高价
        if len(grid_info) == 0 or cur_price > grid_info.get('high_price', cur_price):
            # 每个格，按照4%计算，价格跳动统一最小0.01
            grid_height = round(cur_price * self.grid_height_percent / 100, 2)
            grid_info = {
                'update_time': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'high_price': cur_price,
                'grid_height': grid_height,
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
        grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.open_price == open_price]
        if len(grids) == 0:
            # v1.2，获取K线
            # 如果K线得线段为空白，则不开仓
            if len(self.kline_x.duan_list) == 0 and len(self.kline_x.bi_list) == 0:
                self.write_log(f'{self.kline_x.name}的K线线段、分笔都未生成')
                return

            if len(self.kline_x.duan_list) > 0:
                # 如果线段为上涨线段，不做买入
                cur_duan = self.kline_x.duan_list[-1]
                if cur_duan.direction == 1:
                    self.write_log(f'{self.kline_x.name}的K线线段为向上')
                    return
            else:
                # 如果改分笔为上涨分笔，不做买入
                cur_bi = self.kline_x.bi_list[-1]
                if cur_bi.direction == 1:
                    self.write_log(f'{self.kline_x.name}的K线分笔为向上')
                    return

            # 必须有确认的底分型
            cur_fx = self.kline_x.fenxing_list[-1]
            if cur_fx.direction == -1 and cur_fx.is_rt:
                self.write_log(f'{self.kline_x.name}的K线底部分型未形成.{self.cur_datetime}')
                return

            opened_grids = [g for g in self.gt.dn_grids if g.vt_symbol == vt_symbol and g.open_status]
            if len(opened_grids) == 0:
                volume_rate = len(open_prices)
                self.write_log(f'没有持有多单，待开仓价格:{opened_grids}, 一共{volume_rate}个网格单元')
            else:
                lowest_open_price = min([g.open_price for g in opened_grids])
                open_prices = [p for p in open_prices if p < lowest_open_price]
                if len(open_prices) == 0:
                    return
                volume_rate = min(len(opened_grids), 1)
                self.write_log(f'持有多单，最低开仓价格:{lowest_open_price},可开仓价格:{open_prices}, 一共{volume_rate}个网格单元')

            self.write_log(f'计划添加做多网格:{vt_symbol}')
            close_price = round(open_price + grid_height, 7)
            self.tns_add_long_grid(
                vt_symbol=vt_symbol,
                open_price=open_price,
                close_price=close_price,
                volume_rate=volume_rate
            )

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

        sigle_volume = round_to(
            value=invest_margin / (self.cur_price * self.margin_rate * self.grid_lots),
            target=self.volumn_tick)
        target_volume = sigle_volume * volume_rate
        self.write_log(f'{vt_symbol} 策略最大投入:{invest_margin},每格投入:{sigle_volume}, 当前投入：{target_volume}')

        if target_volume <= 0:
            self.write_log(f'{vt_symbol} 目标增仓为{target_volume}。不做买入')
            return True

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
        dist_record['operation'] = 'entry'

        self.save_dist(dist_record)

        return True

    def tns_check_grids(self):
        """事务检查持仓网格，进行止盈/止损"""
        if self.entrust != 0:
            return

        if not self.trading and not self.inited:
            self.write_error(u'当前不允许交易')
            return

        remove_gids = []
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
                if self.cur_price >= grid.close_price:
                    self.write_log(f'多单满足止盈条件')
                    if self.grid_sell(grid):
                        grid.close_status = True
                        grid.order_status = True
                        continue

            # 检查未开仓网格，检查是否满足开仓条件
            if not grid.close_status and not grid.open_status:
                if grid.open_price >= self.cur_price > grid.open_price * (1 - self.grid_height_percent/100):

                    # 如果K线得线段为空白，则不开仓
                    if len(self.kline_x.duan_list) == 0 and len(self.kline_x.bi_list) == 0:
                        self.write_log(f'{self.kline_x.name}的K线线段、分笔都未生成')
                        continue

                    if len(self.kline_x.duan_list) > 0:
                        # 如果线段为上涨线段，不做买入
                        cur_duan = self.kline_x.duan_list[-1]
                        if cur_duan.direction == 1:
                            self.write_log(f'{self.kline_x.name}的K线线段为向上')
                            continue
                    else:
                        # 如果改分笔为上涨分笔，不做买入
                        cur_bi = self.kline_x.bi_list[-1]
                        if cur_bi.direction == 1:
                            self.write_log(f'{self.kline_x.name}的K线分笔为向上')
                            continue

                    # 必须有确认的底分型
                    cur_fx = self.kline_x.fenxing_list[-1]
                    if cur_fx.direction == -1 and cur_fx.is_rt:
                        self.write_log(f'{self.kline_x.name}的K线底部分型未形成')
                        continue

                    self.write_log(f'当前价:{self.cur_price}满足开仓价格:{grid.open_price}，进行开多，止盈价:{grid.close_price}')
                    if self.grid_buy(grid):
                        grid.order_status = True
                        grid.stop_price = 0
                        continue

        if len(remove_gids) > 0:
            self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_gids)

    def grid_buy(self, grid):
        """
        事务开多仓
        :return:
        """
        if self.backtesting:
            buy_price = self.cur_price + self.price_tick
        else:
            buy_price = self.cur_tick.ask_price_1

        vt_orderids = self.buy(vt_symbol=self.vt_symbol,
                               price=buy_price,
                               volume=grid.volume,
                               order_type=self.order_type,
                               order_time=self.cur_datetime,
                               grid=grid)
        if len(vt_orderids) > 0:
            self.write_log(u'创建{}事务多单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                           .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))
            #self.gt.dn_grids.append(grid)
            self.gt.save()
            return True
        else:
            self.write_error(u'创建{}事务多单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                             .format(grid.type, grid.open_price, grid.volume, grid.close_price))
            return False

class Future_Grid_Trade_Policy(CtaPolicy):

    def __init__(self, strategy):
        super().__init__(strategy)
        # vt_symbol: {
        # name: "币名",
        # high_price: xxx,
        # grid_height: x,
        # open_prices: [x,x,x,x]
        self.grids = {}

    def from_json(self, json_data):
        """将数据从json_data中恢复"""
        super().from_json(json_data)
        self.grids = json_data.get('grids')

    def to_json(self):
        """转换至json文件"""
        j = super().to_json()
        j['grids'] = self.grids

        return j


def bar_to_tick(bar):
    """ 通过b分时bar转换为tick数据 """

    tick = TickData(
        gateway_name='backtesting',
        symbol=bar.symbol,
        exchange=bar.exchange,
        datetime=bar.datetime + timedelta(minutes=1)
    )
    tick.date = tick.datetime.strftime('%Y-%m-%d')
    tick.time = tick.datetime.strftime('%H:%M:%S.000')
    tick.trading_day = bar.trading_day if bar.trading_day else tick.datetime.strftime('%Y-%m-%d')
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
