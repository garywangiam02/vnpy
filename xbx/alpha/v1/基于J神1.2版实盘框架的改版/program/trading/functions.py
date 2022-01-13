"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""

from datetime import datetime, timedelta
from Signals import *
from config import *
from utility import Utility
import pandas as pd
from kline_crawler import get_klines_from_web, get_klines_from_db

utc_offset = int(time.localtime().tm_gmtoff / 60 / 60)


# =====获取数据
# 获取单个币种的1小时数据
# @robust


class Function(object):

    def __init__(self, config: Config):
        self.config = config
        self.utility = Utility(config)

    def fetch_binance_swap_candle_data(self, exchange, symbol, run_time, limit=None):
        """
        通过ccxt的接口fapiPublic_get_klines，获取永续合约k线数据
        获取单个币种的1小时数据
        :param exchange:
        :param symbol:
        :param limit:
        :param run_time:
        :return:
        """
        # 获取数据
        # kline = exchange.fapiPublic_get_klines({'symbol': symbol, 'interval': '1h', 'limit': limit})
        if limit is None:
            limit = self.config['system']['web_query_kline_size']
        kline = self.utility.robust(exchange.fapiPublic_get_klines,
                                    {'symbol': symbol, 'interval': '1h', 'limit': limit})

        # 将数据转换为DataFrame
        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                   'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(kline, columns=columns, dtype='float')
        df.sort_values('candle_begin_time', inplace=True)
        df['symbol'] = symbol  # 添加symbol列
        columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num']
        df = df[columns]

        # 整理数据
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=utc_offset)
        # 删除runtime那行的数据，如果有的话
        df = df[df['candle_begin_time'] != run_time]

        return symbol, df

    # 并行获取所有币种永续合约数据的1小时K线数据
    def fetch_all_binance_swap_candle_data(self, exchange, symbols, kline_type: str):
        """
        并行获取所有币种永续合约数据的1小时K线数据
        :param exchange:
        :param symbol_list:
        :param run_time:
        :return:
        """
        # 创建参数列表
        s_time = time.time()
        fetch_klines_from_db = self.config['system']['fetch_klines_from_db']
        print(f'fetch klines from db: {fetch_klines_from_db}')

        if fetch_klines_from_db:
            df = get_klines_from_db(self.config[pre_cal_key]['db_engine'], symbols)
        else:
            df = get_klines_from_web(symbols, kline_type)
        print('获取所有币种K线数据完成，花费时间：', time.time() - s_time, '\n')
        return df

    # 获取币安的ticker数据
    def fetch_binance_ticker_data(self, binance):
        """
        使用ccxt的接口fapiPublic_get_ticker_24hr()获取ticker数据
                           priceChange  priceChangePercent  weightedAvgPrice     lastPrice    lastQty  ...      openTime     closeTime      firstId       lastId      count
        symbol                                                                                 ...
        BTCUSDT     377.720000               3.517      10964.340000  11118.710000      0.039  ...  1.595927e+12  1.596013e+12  169966030.0  171208339.0  1242251.0
        ETHUSDT       9.840000               3.131        316.970000    324.140000      4.380  ...  1.595927e+12  1.596013e+12   72997450.0   73586755.0   589302.0
        ...
        XLMUSDT       0.002720               2.838          0.096520      0.098570    203.000  ...  1.595927e+12  1.596013e+12   12193167.0   12314848.0   121682.0
        ADAUSDT       0.002610               1.863          0.143840      0.142680   1056.000  ...  1.595927e+12  1.596013e+12   17919791.0   18260724.0   340914.0
        XMRUSDT       2.420000               3.013         81.780000     82.740000      0.797  ...  1.595927e+12  1.596013e+12    4974234.0    5029877.0    55644.0
        :param binance:
        :return:
        """
        tickers = binance.fapiPublic_get_ticker_24hr()
        tickers = pd.DataFrame(tickers, dtype=float)
        tickers.set_index('symbol', inplace=True)

        return tickers['lastPrice']

    # =====获取持仓
    # 获取币安账户的实际持仓
    # @robust
    def update_symbol_info(self, exchange, symbol_list):
        """
        使用ccxt接口：fapiPrivate_get_positionrisk，获取账户持仓
        返回值案例
                       positionAmt  entryPrice  markPrice  unRealizedProfit  liquidationPrice  ...  maxNotionalValue  marginType isolatedMargin  isAutoAddMargin
        positionSide
        symbol                                                                            ...
        XMRUSDT         0.003    63.86333  63.877630          0.000043             0.000  ...            250000       cross            0.0            false         LONG
        ATOMUSDT       -0.030     2.61000   2.600252          0.000292           447.424  ...             25000       cross            0.0            false        SHORT
        :param exchange:
        :param symbol_list:
        :return:
        """
        # 获取原始数据
        position_risk = self.utility.robust(exchange.fapiPrivate_get_positionrisk)

        # 将原始数据转化为dataframe
        position_risk = pd.DataFrame(position_risk, dtype='float')

        # 整理数据
        position_risk.rename(columns={'positionAmt': '当前持仓量'}, inplace=True)
        position_risk = position_risk[position_risk['当前持仓量'] != 0]  # 只保留有仓位的币种
        position_risk.set_index('symbol', inplace=True)  # 将symbol设置为index

        # 创建symbol_info
        symbol_info = pd.DataFrame(index=symbol_list, columns=['当前持仓量'])
        symbol_info['当前持仓量'] = position_risk['当前持仓量']
        symbol_info['当前持仓量'].fillna(value=0, inplace=True)

        return symbol_info

    # =====策略相关函数
    # 选币数据整理 & 选币
    def cal_factor_and_select_coin(self, stratagy_list, symbol_candle_data, run_time):
        """
        :param stratagy_list:
        :param symbol_candle_data:
        :param run_time:
        :return:
        """
        s_time = time.time()

        no_enough_data_symbol = []

        # ===逐个遍历每一个策略
        select_coin_list = []
        for strategy in stratagy_list:
            # 获取策略参数
            c_factor = strategy['c_factor']
            hold_period = strategy['hold_period']
            selected_coin_num = strategy['selected_coin_num']
            factors = strategy['factors']

            # ===逐个遍历每一个币种，计算其因子，并且转化周期
            period_df_list = []

            _symbol_list = symbol_candle_data.keys()
            # print(_symbol_list)

            symbol_list = [symbol for symbol in _symbol_list if 'USDT' in symbol]
            # print(symbol_list)

            for symbol in symbol_list:
                if symbol in no_enough_data_symbol:
                    continue

                # =获取相应币种1h的k线，深度拷贝
                df = symbol_candle_data[symbol].copy()

                # =空数据
                if df.empty:
                    print('no data', symbol)
                    if symbol not in no_enough_data_symbol:
                        no_enough_data_symbol.append(symbol)
                    continue

                if len(df) < 100:
                    print('no enough data', symbol)
                    if symbol not in no_enough_data_symbol:
                        no_enough_data_symbol.append(symbol)
                    continue

                if df['trade_num'].sum() <= 0:
                    print(f'not dealable symbol: {symbol}')
                    if symbol not in no_enough_data_symbol:
                        no_enough_data_symbol.append(symbol)
                    continue

                df[c_factor] = 0

                for factor_dict in factors:
                    factor = factor_dict['factor']
                    para = factor_dict['para']
                    if_reverse = factor_dict['if_reverse']

                    df = eval(f'signal_{factor}')(df, int(para))  # 计算信号

                    # 初始化
                    df[factor + '_因子'] = np.nan

                    # =空计算
                    if np.isnan(df.iloc[-1][factor]):
                        continue

                    if if_reverse:
                        df[factor + '_因子'] = - df[factor]
                    else:
                        df[factor + '_因子'] = df[factor]

                # =将数据转化为需要的周期
                df['s_time'] = df['candle_begin_time']
                df['e_time'] = df['candle_begin_time']
                df.set_index('candle_begin_time', inplace=True)

                agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'close': 'last', c_factor: 'last'}

                for factor_dict in factors:
                    factor = factor_dict['factor']
                    agg_dict[factor + '_因子'] = 'last'

                # 转换生成每个策略所有offset的因子
                for offset in range(int(hold_period[:-1])):
                    # 转换周期
                    period_df = df.resample(hold_period, base=offset).agg(agg_dict)
                    period_df['offset'] = offset
                    # 保存策略信息到结果当中
                    period_df['key'] = f'{c_factor}_{hold_period}_{offset}H'  # 创建主键值

                    # 截取指定周期的数据
                    period_df = period_df[
                        (period_df['s_time'] <= run_time - timedelta(hours=int(hold_period[:-1]))) &
                        (period_df['s_time'] > run_time - 2 * timedelta(hours=int(hold_period[:-1])))
                        ]
                    # 合并数据
                    period_df_list.append(period_df)

            # ===将不同offset的数据，合并到一张表
            df = pd.concat(period_df_list)
            df = df.sort_values(['s_time', 'symbol'])

            df[c_factor] = 0

            for factor_dict in factors:
                factor = factor_dict['factor']
                weight = factor_dict['weight']
                df[factor + '_排名'] = df.groupby('s_time')[factor + '_因子'].rank()
                df[c_factor] += df[factor + '_排名'] * weight

            # ===选币数据整理完成，接下来开始选币
            # 多空双向rank
            df['币总数'] = df.groupby(df.index).size()
            df['rank'] = df.groupby('s_time')[c_factor].rank(method='first')
            # 删除不要的币
            df['方向'] = 0

            df.loc[(df['rank'] <= selected_coin_num), '方向'] = 1
            df.loc[((df['币总数'] - df['rank']) < selected_coin_num), '方向'] = -1

            df = df[df['方向'] != 0]
            # ===将每个币种的数据保存到dict中
            # 删除不需要的列
            # df.drop([factor, '币总数', 'rank'], axis=1, inplace=True)
            df.drop(['币总数', 'rank'], axis=1, inplace=True)
            df.reset_index(inplace=True)
            select_coin_list.append(df)

        select_coin = pd.concat(select_coin_list)
        print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)
        print(select_coin)

        if self.config['system']['debug']:
            # 调试模式下，保存选币信息
            print(select_coin)
        return select_coin

    # 计算每个策略分配的资金
    def cal_strategy_trade_usdt(self, stratagy_list, trade_usdt):
        """
        计算每个策略分配的资金
        """
        df = pd.DataFrame()
        # 策略的个数
        strategy_num = len(stratagy_list)
        # 遍历策略
        for strategy in stratagy_list:
            c_factor = strategy['c_factor']
            hold_period = strategy['hold_period']
            selected_coin_num = strategy['selected_coin_num']

            offset_num = int(hold_period[:-1])
            for offset in range(offset_num):
                df.loc[
                    f'{c_factor}_{hold_period}_{offset}H', '策略分配资金'] = trade_usdt / strategy_num / 2 / offset_num \
                                                                       / selected_coin_num

        df.reset_index(inplace=True)
        df.rename(columns={'index': 'key'}, inplace=True)

        return df

    # 计算实际下单量
    def cal_order_amount(self, symbol_info, select_coin, strategy_trade_usdt):
        select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left')
        select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']

        # 对下单量进行汇总
        symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum()
        symbol_info['目标下单量'].fillna(value=0, inplace=True)
        symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
        symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']

        # 历史回溯
        print(select_coin[['key', 's_time', 'symbol', '方向', '策略分配资金', 'close']], '\n')

        # 删除实际下单量为0的币种
        symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
        return symbol_info

    # 下单
    # @robust
    def place_order(self, symbol_info, symbol_last_price):
        min_qty = self.config[pre_cal_key]['min_qty']
        price_precision = self.config[pre_cal_key]['price_precision']
        exchange = self.config[pre_cal_key]['exchange']

        for symbol, row in symbol_info.dropna(subset=['实际下单量']).iterrows():
            if symbol not in min_qty:
                continue

            # 计算下单量：按照最小下单量向下取整
            quantity = row['实际下单量']
            quantity = float(f'{quantity:.{min_qty[symbol]}f}')
            reduce_only = np.isnan(row['目标下单份数']) or row['目标下单量'] * quantity < 0

            quantity = abs(quantity)  # 下单量取正数
            if quantity == 0:
                print(symbol, quantity, '实际下单量为0，不下单')
                continue

            # 计算下单方向、价格
            if row['实际下单量'] > 0:
                side = 'BUY'
                price = symbol_last_price[symbol] * 1.02
            else:
                side = 'SELL'
                price = symbol_last_price[symbol] * 0.98

            # 对下单价格这种最小下单精度
            price = float(f'{price:.{price_precision[symbol]}f}')

            if symbol not in price_precision:
                continue

            if (quantity * price < 5) and not reduce_only:
                print('quantity * price < 5')
                quantity = 0
                continue

            # 下单参数
            params = {'symbol': symbol, 'side': side, 'type': 'LIMIT', 'price': price, 'quantity': quantity,
                      'clientOrderId': str(time.time()), 'timeInForce': 'GTC', 'reduceOnly': reduce_only}
            # 下单
            print('下单参数：', params)
            if self.config['system']['debug']:
                print('debug, 跳过下单')
            else:
                open_order = self.utility.robust(exchange.fapiPrivate_post_order, params)
                print('下单完成，下单信息：', open_order, '\n')

    # =====辅助功能函数
    # ===下次运行时间，和课程里面讲的函数是一样的
    def next_run_time(self, time_interval, ahead_seconds=5, cheat_seconds=100):
        """
        根据time_interval，计算下次运行的时间，下一个整点时刻。
        目前只支持分钟和小时。
        :param time_interval: 运行的周期，15m，1h
        :param ahead_seconds: 预留的目标时间和当前时间的间隙
        :return: 下次运行的时间
        案例：
        15m  当前时间为：12:50:51  返回时间为：13:00:00
        15m  当前时间为：12:39:51  返回时间为：12:45:00
        10m  当前时间为：12:38:51  返回时间为：12:40:00
        5m  当前时间为：12:33:51  返回时间为：12:35:00

        5m  当前时间为：12:34:51  返回时间为：12:40:00

        30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00

        30m  当前时间为：14:37:51  返回时间为：14:56:00

        1h  当前时间为：14:37:51  返回时间为：15:00:00

        """
        if time_interval.endswith('m') or time_interval.endswith('h'):
            pass
        elif time_interval.endswith('T'):
            time_interval = time_interval.replace('T', 'm')
        elif time_interval.endswith('H'):
            time_interval = time_interval.replace('H', 'h')
        else:
            print('time_interval格式不符合规范。程序exit')
            exit()
        ti = pd.to_timedelta(time_interval)
        now_time = datetime.now()
        # now_time = datetime(2019, 5, 9, 23, 50, 30)  # 修改now_time，可用于测试
        this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
        min_step = timedelta(minutes=1)

        target_time = now_time.replace(second=0, microsecond=0)

        while True:
            target_time = target_time + min_step
            delta = target_time - this_midnight
            if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
                # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
                break
        if cheat_seconds > 0.1:
            target_time = target_time - timedelta(seconds=cheat_seconds)
        print('程序下次运行的时间：', target_time, '\n')
        return target_time

    # ===依据时间间隔, 自动计算并休眠到指定时间
    def sleep_until_run_time(self, time_interval, ahead_time=1, if_sleep=True, cheat_seconds=120):
        """
        根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
        :param if_sleep:
        :param time_interval:
        :param ahead_time:
        :return:
        """
        # 计算下次运行时间
        run_time = self.next_run_time(time_interval, ahead_time, cheat_seconds)
        # sleep
        if if_sleep:
            time.sleep(max(0, (run_time - datetime.now()).seconds))
            while True:  # 在靠近目标时间时
                if datetime.now() > run_time:
                    break
        return run_time
