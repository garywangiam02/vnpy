from vnpy.app.cta_crypto import (
    CtaFutureTemplate,
    StopOrder,
    Direction,
    Offset,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)

from vnpy.trader.utility import round_to


class TurtleSignalBtcStrategy_v3(CtaFutureTemplate):
    """"""
    author = "大佳"

    x_minute = 15
    entry_window = 20  # 入场
    exit_window = 10  # 出场
    atr_window = 20  # atr
    fixed_size = 1
    invest_pos = 1
    invest_percent = 10  # 投资比例

    entry_up = 0
    entry_down = 0
    exit_up = 0
    exit_down = 0
    atr_value = 0

    long_entry = 0
    short_entry = 0
    long_stop = 0
    short_stop = 0

    parameters = ["x_minute", "entry_window", "exit_window", "atr_window", "fixed_size", "backtesting"]
    variables = ["entry_up", "entry_down", "exit_up", "exit_down", "atr_value"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        # 获取合约乘数，保证金比例
        self.symbol_size = self.cta_engine.get_size(self.vt_symbol)
        self.symbol_margin_rate = self.cta_engine.get_margin_rate(self.vt_symbol)
        self.symbol_price_tick = self.cta_engine.get_price_tick(self.vt_symbol)

        self.bg = BarGenerator(self.on_bar, window=self.x_minute)
        self.am = ArrayManager()

        self.cur_mi_price = None  # 当前价格
        self.cur_datetime = None  # 当前时间

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_bar(20)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.cancel_all()

        self.cur_mi_price = bar.close_price

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # Only calculates new entry channel when no position holding
        if not self.pos:
            self.entry_up, self.entry_down = self.am.donchian(
                self.entry_window
            )

        self.exit_up, self.exit_down = self.am.donchian(self.exit_window)

        # if bar.datetime.strftime('%Y-%m-%d %H') == '2016-03-07 09':
        #     a = 1  # noqa

        if not self.pos:
            self.atr_value = self.am.atr(self.atr_window)
            self.atr_value = max(4 * self.symbol_price_tick, self.atr_value)

            self.long_entry = 0
            self.short_entry = 0
            self.long_stop = 0
            self.short_stop = 0

            self.send_buy_orders(self.entry_up)
            self.send_short_orders(self.entry_down)
        elif self.pos > 0:
            self.send_buy_orders(self.entry_up)

            sell_price = max(self.long_stop, self.exit_down)
            refs = self.sell(sell_price, abs(self.pos), True)
            if len(refs) > 0:
                self.write_log(f'平多委托编号:{refs}')

        elif self.pos < 0:
            self.send_short_orders(self.entry_down)

            cover_price = min(self.short_stop, self.exit_up)
            refs = self.cover(cover_price, abs(self.pos), True)
            if len(refs) > 0:
                self.write_log(f'平空委托编号:{refs}')

        self.put_event()

    def update_invest_pos(self):
        """计算获取投资仓位"""
        # 获取账号资金
        capital, available, cur_percent, percent_limit = self.cta_engine.get_account()
        # 按照投资比例计算保证金
        invest_margin = capital * self.invest_percent / 100
        max_invest_pos = int(invest_margin / (self.cur_mi_price * self.symbol_size * self.symbol_margin_rate))
        self.invest_pos = max(int(max_invest_pos / 4), 1)

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        pre_pos = self.pos
        if trade.direction == Direction.LONG:
            if trade.offset == Offset.OPEN:
                self.long_entry = trade.price  # 多头入场价格
                self.long_stop = max(self.long_stop, self.long_entry - 2 * self.atr_value)  # 止损价格

            self.pos += trade.volume
            d = {'datetime': trade.time,
                 'symbol': self.vt_symbol,
                 'volume': trade.volume,
                 'price': trade.price,
                 'operation': 'buy' if trade.offset == Offset.OPEN else 'cover',
                 'signal': str(int(self.pos / trade.volume)) if trade.offset == Offset.OPEN else '',
                 'stop_price': self.long_stop,
                 'target_price': 0,
                 'long_pos': self.pos if self.pos > 0 else 0,
                 'short_pos': self.pos if self.pos < 0 else 0
                 }
            self.save_dist(d)
        else:
            if trade.offset == Offset.OPEN:
                self.short_entry = trade.price
                if self.short_entry == 0:
                    self.short_stop = self.short_entry + 2 * self.atr_value
                else:
                    self.short_entry = min(self.short_entry, self.short_entry + 2 * self.atr_value)
            self.pos -= trade.volume
            d = {'datetime': trade.time,
                 'symbol': self.vt_symbol,
                 'volume': trade.volume,
                 'price': trade.price,
                 'operation': 'short' if trade.offset == Offset.OPEN else 'sell',
                 'signal': int(self.pos / abs(trade.volume)) if trade.offset == Offset.OPEN else '',
                 'stop_price': self.short_stop,
                 'target_price': 0,
                 'long_pos': self.pos if self.pos > 0 else 0,
                 'short_pos': self.pos if self.pos < 0 else 0
                 }
            self.save_dist(d)

        self.write_log(f'{self.vt_symbol},pos {pre_pos} => {self.pos}')

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def send_buy_orders(self, price):
        """"""

        if self.cur_mi_price <= price - self.atr_value / 2:
            return

        self.update_invest_pos()

        t = int(self.pos / self.invest_pos)

        if t >= 4:
            return

        if t < 1:
            refs = self.buy(price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'买入委托编号:{refs}')

        if t == 1 and self.cur_mi_price > self.long_entry:
            buy_price = round_to(self.long_entry + self.atr_value * 0.5, self.symbol_price_tick)
            self.write_log(u'发出做多停止单，触发价格为: {}'.format(buy_price))
            refs = self.buy(buy_price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'买入委托编号:{refs}')

        if t == 2 and self.cur_mi_price > self.long_entry:
            buy_price = round_to(self.long_entry + self.atr_value * 0.5, self.symbol_price_tick)
            self.write_log(u'发出做多停止单，触发价格为: {}'.format(buy_price))
            refs = self.buy(buy_price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'买入委托编号:{refs}')

        if t == 3 and self.cur_mi_price > self.long_entry:
            buy_price = round_to(self.long_entry + self.atr_value * 0.5, self.symbol_price_tick)
            self.write_log(u'发出做多停止单，触发价格为: {}'.format(buy_price))
            refs = self.buy(buy_price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'买入委托编号:{refs}')

    def send_short_orders(self, price):
        """"""
        if self.cur_mi_price >= price + self.atr_value / 2:
            return

        self.update_invest_pos()

        t = int(self.pos / self.invest_pos)

        if t <= -4:
            return

        if t > -1:
            refs = self.short(price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'卖出委托编号:{refs}')

        if t == -1 and self.cur_mi_price < self.short_entry:
            short_price = round_to(price - self.atr_value * 0.5, self.symbol_price_tick)
            self.write_log(u'发出做空停止单，触发价格为: {}'.format(short_price))
            refs = self.short(short_price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'卖出委托编号:{refs}')

        if t == -2 and self.cur_mi_price < self.short_entry:
            short_price = round_to(self.short_entry - self.atr_value * 0.5, self.symbol_price_tick)
            self.write_log(u'发出做空停止单，触发价格为: {}'.format(short_price))
            refs = self.short(short_price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'卖出委托编号:{refs}')

        if t == -3 and self.cur_mi_price < self.short_entry:
            short_price = round_to(self.short_entry - self.atr_value * 0.5, self.symbol_price_tick)
            self.write_log(u'发出做空停止单，触发价格为: {}'.format(short_price))
            refs = self.short(short_price, self.invest_pos, True)
            if len(refs) > 0:
                self.write_log(f'卖出委托编号:{refs}')
