import time
from multiprocessing import Pool
from datetime import datetime, timedelta
from coin_alpha_kopo_V1.comon.Signals import *
from coin_alpha_kopo_V1.program.config import *

import random

# ===每次获取K线数据的数量
LIMIT = 1000


# =====获取数据
# 获取单个币种的1小时数据
def fetch_binance_swap_candle_data(exchange, symbol, run_time, limit=LIMIT):
    """
    通过ccxt的接口fapiPublic_get_klines，获取永续合约k线数据
    获取单个币种的1小时数据
    :param exchange:
    :param symbol:
    :param limit:
    :param run_time:
    :return:
    """
    for i in range(5):
        try:
            print('开始获取k线数据：', symbol, datetime.now())
            # 获取数据
            kline = exchange.fapiPublic_get_klines({'symbol': symbol, 'interval': '1h', 'limit': limit})

            # 将数据转换为DataFrame
            columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
            df = pd.DataFrame(kline, columns=columns, dtype='float')

            # 整理数据
            df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
            df['symbol'] = symbol  # 添加symbol列
            columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
            df = df[columns]

            # 删除runtime那行的数据，如果有的话
            df = df[df['candle_begin_time'] != run_time]

            print('结束获取k线数据：', symbol, datetime.now())

            return symbol, df
        except BaseException as e:
            print(e)
            pass
            time.sleep(random.uniform(1, 3))
            continue


# 并行获取所有币种永续合约数据的1小时K线数据_进程
def fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time):
    """
    并行获取所有币种永续合约数据的1小时K线数据
    :param exchange:
    :param symbol_list:
    :param run_time:
    :return:
    """
    try:
        # 创建参数列表
        arg_list = [(exchange, symbol, run_time) for symbol in symbol_list]
        # 多进程获取数据
        s_time = time.time()

        # len(arg_list)
        with Pool(processes=8) as pl:
            # 利用starmap启用多进程信息
            result = pl.starmap(fetch_binance_swap_candle_data, arg_list)

    except:

        # 串行
        # 创建参数列表
        arg_list = [(exchange, symbol, run_time) for symbol in symbol_list]

        s_time = time.time()
        result = []
        for arg in arg_list:
            (exchange, symbol, run_time) = arg
            res = fetch_binance_swap_candle_data(exchange, symbol, run_time)
            result.append(res)

    df = dict(result)

    print('获取所有币种K线数据完成，花费时间：', time.time() - s_time)
    return df


# 获取币安永续合约账户的当前净值
def fetch_binance_swap_equity(exchange):
    """
    获取币安永续合约账户的当前净值
    """
    # # 获取当前账户净值
    # balance = exchange.fapiPrivate_get_balance()  # 获取账户净值
    # balance = pd.DataFrame(balance)
    # equity = float(balance[balance['asset'] == 'USDT']['balance'])

    # 实时更新，可以把未实现盈亏也计算进来
    account_info = robust(exchange.fapiPrivateGetAccount, )  # 获取账户净值
    equity = round(float(account_info['totalMarginBalance']), 3)
    return equity


# 获取币安的ticker数据
def fetch_binance_ticker_data(binance):
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

    tickers = robust(binance.fapiPublic_get_ticker_24hr, )
    tickers = pd.DataFrame(tickers, dtype=float)

    tickers.set_index('symbol', inplace=True)

    return tickers['lastPrice']


# 获取币安账户的实际持仓
def update_symbol_info(exchange, symbol_list):
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
    position_risk = exchange.fapiPrivate_get_positionrisk()

    # 将原始数据转化为dataframe
    position_risk = pd.DataFrame(position_risk, dtype='float')

    # 整理数据
    position_risk.rename(columns={'positionAmt': '当前持仓量', 'entryPrice': '持仓价格', 'markPrice': '标记价格'}, inplace=True)
    position_risk = position_risk[position_risk['当前持仓量'] != 0]  # 只保留有仓位的币种
    position_risk.set_index('symbol', inplace=True)  # 将symbol设置为index

    # 创建symbol_info
    symbol_info = pd.DataFrame(index=symbol_list, columns=['当前持仓量'])
    symbol_info['当前持仓量'] = position_risk['当前持仓量']
    symbol_info['当前持仓量'].fillna(value=0, inplace=True)
    symbol_info['持仓价格'] = position_risk['持仓价格']
    symbol_info['持仓价格'].fillna(value=0, inplace=True)
    symbol_info['标记价格'] = position_risk['标记价格']
    symbol_info['标记价格'].fillna(value=0, inplace=True)
    condition = abs(symbol_info['当前持仓量'] * symbol_info['持仓价格'])
    symbol_info.loc[condition >= 1, '利润'] = (symbol_info['标记价格'] / symbol_info['持仓价格'] - 1) * (
            symbol_info['当前持仓量'] / (abs(symbol_info['当前持仓量']))) * 100
    symbol_info['利润'].fillna(value=0, inplace=True)

    return symbol_info


def a(symbol, symbol_candle_data, c_factor, factors, hold_period, run_time):
    # =获取相应币种1h的k线，深度拷贝
    period_df_list = []

    # =获取相应币种1h的k线，深度拷贝
    df = symbol_candle_data[symbol].copy()

    # =空数据
    if df.empty:
        print('no data', symbol)

    # 数据量不足12天
    if len(df) < 100:
        print('no enough data', symbol)

    else:

        df[c_factor] = 0

        for factor_dict in factors:
            factor = factor_dict['factor']
            para = factor_dict['para']
            if_reverse = factor_dict['if_reverse']

            if 'diff' in factor:
                factor_diff = factor.split('_')
                factor_name = '_'.join(factor_diff[i] for i in range(len(factor_diff) - 2))
                d_num = factor_diff[-1]
                df = eval(f'signal_{factor_name}')(df, int(para))  # 计算信号
                add_diff(df, d_num, factor_name)

            else:
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
            period_df_list.append(period_df)

        # print(period_df_list)
        # exit()
        return period_df_list


# =====策略相关函数
# 选币数据整理 & 选币
def cal_factor_and_select_coin(stratagy_list, symbol_candle_data, run_time):
    """
    :param stratagy_list:
    :param symbol_candle_data:
    :param run_time:
    :return:
    """
    s_time = time.time()

    # ===逐个遍历每一个策略
    select_coin_list = []
    for strategy in stratagy_list:
        # 获取策略参数
        c_factor = strategy['c_factor']
        hold_period = strategy['hold_period']
        selected_coin_num = strategy['selected_coin_num']
        factors = strategy['factors']

        # ===逐个遍历每一个币种，计算其因子，并且转化周期

        symbol_list = symbol_candle_data.keys()

        arg_list = [(symbol, symbol_candle_data, c_factor, factors, hold_period, run_time) for symbol in symbol_list]
        with Pool(processes=8) as pl:
            # 利用starmap启用多进程信息
            period_df_list = pl.starmap(a, arg_list)

        df = []
        for i in range(len(period_df_list)):
            if period_df_list[i]:
                df1 = pd.concat(period_df_list[i])
                df.append(df1)
        # 合并数据
        # ===将不同offset的数据，合并到一张表
        df = pd.concat(df)
        df = df.sort_values(['s_time', 'symbol'])

        df[c_factor] = 0

        for factor_dict in factors:
            factor = factor_dict['factor']
            weight = factor_dict['weight']
            df[factor + '_排名'] = df.groupby('s_time')[factor + '_因子'].rank()
            df[c_factor] += df[factor + '_排名'] * weight

        df = df.dropna(axis=0)  # 去除空值避免多空失衡

        # ===选币数据整理完成，接下来开始选币

        # 多空双向rank
        df['币总数'] = df.groupby(df.index).size()
        # ranks assigned in order they appear in the array
        df['rank'] = df.groupby('s_time')[c_factor].rank(method='first')
        # 删除不要的币
        df['方向'] = 0

        df.loc[(df['rank'] <= selected_coin_num), '方向'] = 1
        df.loc[((df['币总数'] - df['rank']) < selected_coin_num), '方向'] = -1

        df = df[df['方向'] != 0]
        # ===将每个币种的数据保存到dict中
        # 删除不需要的列
        df.drop(['币总数', 'rank'], axis=1, inplace=True)
        df.reset_index(inplace=True)
        select_coin_list.append(df)

    select_coin = pd.concat(select_coin_list)
    # print(select_coin)
    # exit()

    select_coin = select_coin[['candle_begin_time', 'symbol', 's_time', 'e_time', 'close', 'offset', 'key', '方向']]
    print(select_coin)
    print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)

    return select_coin


def cal_factor_and_select_coin_rank(stratagy_list, symbol_candle_data, run_time):
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

            df[c_factor] = 0

            for factor_dict in factors:
                factor = factor_dict['factor']
                para = factor_dict['para']
                if_reverse = factor_dict['if_reverse']

                if 'diff' in factor:
                    factor_diff = factor.split('_')
                    factor_name = '_'.join(factor_diff[i] for i in range(len(factor_diff) - 2))
                    d_num = factor_diff[-1]
                    df = eval(f'signal_{factor_name}')(df, int(para))  # 计算信号
                    add_diff(df, d_num, factor_name)

                else:
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
    select_coin = select_coin[['candle_begin_time', 'symbol', 's_time', 'e_time', 'close', 'offset', 'key', '方向']]
    print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)
    print(select_coin)

    return select_coin


# 计算旧的和新的策略分配资金
def cal_old_and_new_trade_usdt(exchange, Percent):
    """
    每隔一段时间修改一下trade_usdt
    """

    trade_usdt_old = fetch_binance_swap_equity(exchange) * Percent  # 读取币安账户最新的trade_usdt

    print('trade_usdt_old：', trade_usdt_old, '\n')
    return trade_usdt_old


# 计算每个策略分配的资金
def cal_strategy_trade_usdt(stratagy_list, trade_usdt_old):
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
                f'{c_factor}_{hold_period}_{offset}H', '策略分配资金'] = trade_usdt_old / strategy_num / 2 / offset_num \
                                                                   / selected_coin_num

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'key'}, inplace=True)

    return df


# 计算实际下单量
def cal_order_amount(symbol_info, select_coin, strategy_trade_usdt):
    """
    计算每个币种的实际下单量，并且聚会汇总，放到symbol_info变量中
    """
    # 合并每个策略分配的资金
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left')

    # 将策略选币时间end_time与当天的凌晨比较，越过凌晨时刻则用本周期的资金，否则用上周期资金
    # select_coin['策略分配资金'] = np.where(select_coin['e_time'] >= run_time.replace(hour=Update_Hour),
    #                                  select_coin['策略分配资金_新'], select_coin['策略分配资金_旧'])

    select_coin['策略分配资金'] = select_coin['策略分配资金']
    # 计算下单量
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']
    print(select_coin[['key', 's_time', 'symbol', '方向', '策略分配资金']], '\n')

    # 对下单量进行汇总
    symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum()
    symbol_info['目标下单量'].fillna(value=0, inplace=True)
    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']

    # 删除实际下单量为0的币种
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    return symbol_info


# 计算实际下单量
def cal_order_amount_equity(symbol_info, select_coin, strategy_trade_usdt, equity_signal):
    """
    计算每个币种的实际下单量，并且聚会汇总，放到symbol_info变量中
    """
    # 合并每个策略分配的资金
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left')

    # 将策略选币时间end_time与当天的凌晨比较，越过凌晨时刻则用本周期的资金，否则用上周期资金
    # select_coin['策略分配资金'] = np.where(select_coin['e_time'] >= run_time.replace(hour=Update_Hour),
    #                                  select_coin['策略分配资金_新'], select_coin['策略分配资金_旧'])

    select_coin['策略分配资金'] = select_coin['策略分配资金']
    # 计算下单量
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']
    print(select_coin[['key', 's_time', 'symbol', '方向', '策略分配资金']], '\n')

    # 对下单量进行汇总
    symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum() * equity_signal
    symbol_info['目标下单量'].fillna(value=0, inplace=True)
    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']

    # 删除实际下单量为0的币种
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    return symbol_info


# 下单

def place_order_reduce_only(exchange, symbol_info, symbol_last_price, min_qty, price_precision):
    for symbol, row in symbol_info.dropna(subset=['实际下单量']).iterrows():

        if symbol not in min_qty:
            continue

        # 计算下单量：按照最小下单量向下取整
        quantity = row['实际下单量']
        quantity = float(f'{quantity:.{min_qty[symbol]}f}')
        # 检测是否需要开启只减仓
        reduce_only = np.isnan(row['目标下单份数']) or row['目标下单量'] * quantity < 0  # nan就是清除 <0 减仓

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

        # 在只减仓的情况下可绕过最小5U下单量的检测
        if (quantity * price < 5) and not reduce_only:
            print('quantity * price < 5')
            quantity = 0
            continue

        # 下单参数
        params = {'symbol': symbol, 'side': side, 'type': 'LIMIT', 'price': price, 'quantity': quantity,
                  'clientOrderId': str(time.time()), 'timeInForce': 'GTC', 'reduceOnly': reduce_only}
        # 下单
        print('下单参数：', params)
        open_order = robust(exchange.fapiPrivate_post_order, params)
        print('下单完成，下单信息：', open_order, '\n')


# =====辅助功能函数
# 下次运行时间，和课程里面讲的函数是一样的
def next_run_time(time_interval, ahead_seconds=5):
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

    print('程序下次运行的时间：', target_time, '\n')
    return target_time


def send_dingding_msg_every_loop(name, equity, select_coin, symbol_info, symbol_amount, symbol_last_price, dingding_id, dingding_secret):
    """
    发送钉钉
    """
    # 获取多头仓位
    long_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] > 0]['当前持仓量']).dropna()
    # 获取空头仓位
    short_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] < 0]['当前持仓量']).dropna()

    dingding_msg = f'{name} 账户净值： {equity:8.2f}\n'
    dingding_msg += f'多仓净值: {sum(long_position_equity):8.2f}\n'
    dingding_msg += f'空仓净值: {sum(short_position_equity):8.2f}\n'
    if sum(short_position_equity) != 0:
        dingding_msg += f'多空比: {abs((sum(long_position_equity) / sum(short_position_equity))):8.2f}\n'

    # dingding_msg += '策略持仓\n\n'
    # dingding_msg += select_coin[['key', 'symbol', '方向']].to_string(index=False)
    # dingding_msg += '\n下单信息\n'
    # dingding_msg += symbol_amount.to_string(index=False)

    send_dingding_msg(dingding_msg, dingding_id, dingding_secret)
    print('发送钉钉成功')


# ===依据时间间隔, 自动计算并休眠到指定时间
def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param if_sleep:
    :param time_interval:
    :param ahead_time:
    :return:
    """
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time)
    # sleep
    if if_sleep:
        time.sleep(max(0, (run_time - datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if datetime.now() > run_time:
                break

    return run_time


# ===发送钉钉相关函数


# 发送钉钉消息
def send_dingding_msg(content, robot_id='',
                      secret=''):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。例如如下地址：https://oapi.dingtalk.com/robot/
    send?access_token=81a0e96814b4c8c3132445f529fbffd4bcce66
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """
    try:

        from dingtalkchatbot.chatbot import DingtalkChatbot
        xiaoding = DingtalkChatbot(webhook=robot_id, secret=secret)
        xiaoding.send_text(content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S"), is_at_all=True)

        print('成功发送钉钉')
    except Exception as e:
        print("发送钉钉失败:", e)


def run_function_till_success(function, tryTimes=5, sleepTimes=60):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    while True:
        if retry > tryTimes:
            return False
        try:
            result = function()
            return [result, retry]
        except (Exception) as reason:
            print(reason)
            # send_dingding_msg(':' + str(reason), dingding_id, dingding_secret)
            retry += 1
            if sleepTimes != 0:
                time.sleep(random.uniform(1, sleepTimes))  # 一分钟请求20次以内


def robust(actual_do, *args, **keyargs):
    tryTimes = 5
    sleepTimes = 3
    result = run_function_till_success(function=lambda: actual_do(*args, **keyargs), tryTimes=tryTimes, sleepTimes=sleepTimes)
    if result:
        return result[0]
    else:
        send_dingding_msg(':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数', dingding_id, dingding_secret)
        # os._exit(0)
        # exit()


def transfer_future_to_spot(exchange, asset, amount):
    info = robust(exchange.sapiPostFuturesTransfer, {
        'type': 2,  # 1：现货至u本位合约；2：u本位合约至现货
        'asset': asset,
        'amount': amount,
    })
    print(f'从U本位合约至现货账户划转成功：{info}，划转数量：{amount} {asset}，时间：{datetime.now()}')


def transfer_spot_to_future(exchange, asset, amount):
    info = robust(exchange.sapiPostFuturesTransfer, {
        'type': 1,  # 1：现货至u本位合约；2：u本位合约至现货
        'asset': asset,
        'amount': amount,
    })
    print(f'从现货至U本位合约账户划转{amount} {asset}成功：{info}，时间：{datetime.now()}')


def spot_buy_quote(exchange, symbol, quote_amount):
    info = robust(exchange.privatePostOrder, {
        'symbol': symbol,
        'side': 'BUY',
        'type': 'MARKET',
        'quoteOrderQty': quote_amount
    })
    print(f'市价买入{symbol}成功: {info}')


def get_spot_balance(exchange, asset):
    account = robust(exchange.private_get_account, )
    balance = account['balances']
    balance = pd.DataFrame(balance)
    # 如果子账号没有使用过现货账户，此处会返回空值
    if balance.empty:
        return 0.0

    amount_free = float(balance[balance['asset'] == asset]['free'])
    spot_sell1_price = exchange.fetchTicker(symbol='BNB/USDT')['ask']
    amount_20 = 20 / spot_sell1_price

    if amount_20 >= amount_free:
        amount = amount_free

    else:
        amount = amount_20

    print(f'查询到现货账户有{amount} {asset}')
    return amount


def replenish_bnb(exchange, balance):
    amount_bnb = float(balance[balance['asset'] == 'BNB']['balance'].iloc[0])
    print(f"当前账户剩余{amount_bnb} BNB")
    if amount_bnb < 0.01:

        spot_usdt_amount = get_spot_balance(exchange, 'USDT')
        time.sleep(2)

        # 计算所需的手续费
        account_balance = fetch_binance_swap_equity(exchange)
        fee = round(account_balance * 4 / 10000, 2)
        if fee <= 11:
            fee = 11
        print(f"从现货市场买入所需{fee} USDT等值BNB并转入合约账户")

        if spot_usdt_amount < fee:
            transfer_future_to_spot(exchange, 'USDT', fee - spot_usdt_amount)
            time.sleep(2)
        spot_buy_quote(exchange, 'BNBUSDT', fee)
        time.sleep(2)
        spot_bnb_amount = get_spot_balance(exchange, 'BNB')
        time.sleep(2)
        transfer_spot_to_future(exchange, 'BNB', spot_bnb_amount)
        time.sleep(2)
        message = f"成功买入{spot_bnb_amount} BNB并转入U本位合约账户"
        print(message)


# 增加了数据,供后续TWAP使用
def cal_order_amount_twap_freestep(symbol_info, select_coin, strategy_trade_usdt):
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left').fillna(0)
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']

    # 对下单量进行汇总
    symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum()
    symbol_info['目标下单量'].fillna(value=0, inplace=True)
    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']
    select_coin.sort_values('candle_begin_time', inplace=True)
    symbol_info['close'] = select_coin.groupby('symbol')[['close']].last()
    symbol_info['实际下单资金'] = symbol_info['实际下单量'] * symbol_info['close']
    del symbol_info['close']

    # 删除实际下单量为0的币种
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    return symbol_info


# 增加了数据,供后续TWAP使用
def cal_order_amount_twap_freestep_equity(symbol_info, select_coin, strategy_trade_usdt, equity_signal):
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left').fillna(0)
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']

    # 对下单量进行汇总
    symbol_info['目标下单量'] = (select_coin.groupby('symbol')[['目标下单量']].sum())

    symbol_info['目标下单量'] = symbol_info['目标下单量'] * equity_signal  # 应该改变的是目标下单量
    symbol_info['目标下单量'].fillna(value=0, inplace=True)

    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = (symbol_info['目标下单量'] - symbol_info['当前持仓量'])

    select_coin.sort_values('candle_begin_time', inplace=True)
    symbol_info['close'] = select_coin.groupby('symbol')[['close']].last()
    symbol_info['实际下单资金'] = symbol_info['实际下单量'] * symbol_info['close']
    del symbol_info['close']

    # 删除实际下单量为0的币种
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    return symbol_info


def get_twap_symbol_info_list(symbol_info, Max_one_order_amount):
    '''
    对超额订单进行拆分,并进行调整,尽可能每批中子订单、每批订单让多空平衡
    :param symbol_info 原始下单信息
    :param Max_one_order_amount:单次下单最大金额
    '''
    long = symbol_info[symbol_info['实际下单量'] >= 0]
    short = symbol_info[symbol_info['实际下单量'] < 0]
    long['下单金额排名'] = long['实际下单资金'].rank(ascending=False, method='first')
    short['下单金额排名'] = short['实际下单资金'].rank(method='first')
    symbol_info = pd.concat([long, short]).sort_values(['下单金额排名', '实际下单资金'], ascending=[True, False])

    twap_symbol_info_list = [symbol_info.copy()]
    num = 0
    is_twap = True if max(abs(symbol_info['实际下单资金'])) > Max_one_order_amount else False
    safe_amount = 0.1 * Max_one_order_amount

    while is_twap:
        symbol_info = twap_symbol_info_list[num]

        add_order_list = []
        drop_list = []
        for i in range(symbol_info.shape[0]):
            symbol = symbol_info.index[i]
            order = symbol_info.iloc[i:i + 1]

            if abs(order.iat[0, 7]) > Max_one_order_amount:
                ratio = (Max_one_order_amount - safe_amount) / abs(order.iat[0, 7])
                add_order = order.copy()
                add_order[['当前持仓量', '目标下单量', '目标下单份数', '实际下单量', '实际下单资金']] = add_order[['当前持仓量', '目标下单量', '目标下单份数', '实际下单量', '实际下单资金']] * (1 - ratio)
                symbol_info.iloc[i, :-1] = symbol_info.iloc[i, :-1] * ratio
                add_order_list.append(add_order)
        symbol_info.drop(drop_list, inplace=True)
        twap_symbol_info_list[num] = symbol_info.copy()
        add_df = pd.concat(add_order_list)
        twap_symbol_info_list.append(add_df.copy())
        is_twap = True if max(abs(add_df['实际下单资金'])) > Max_one_order_amount else False
        num += 1
    print(f'Twap批次:{len(twap_symbol_info_list)}\n')

    return twap_symbol_info_list


# 一键清仓
def clear_pos(exchange, min_qty, price_precision):
    print('开始清仓')
    exchange_info = robust(exchange.fapiPublic_get_exchangeinfo, )  # 获取账户净值
    _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
    _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # 过滤usdt合约

    symbol_info = update_symbol_info(exchange, _symbol_list)

    symbol_info['目标下单份数'] = 0
    symbol_info['目标下单量'] = 0

    # =====计算实际下单量
    symbol_info['实际下单量'] = - symbol_info['当前持仓量']
    symbol_info['实际下单资金'] = symbol_info['实际下单量'] * symbol_info['持仓价格']
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    # print(symbol_info)
    # exit()

    # 新增参数
    # 单次最大下单金额
    Max_one_order_amount = 800
    # 订单间隔时间
    Twap_interval = 3

    # 使用twap算法拆分订单
    twap_symbol_info_list = get_twap_symbol_info_list(symbol_info, Max_one_order_amount)

    for i in range(len(twap_symbol_info_list)):

        # =====获取币种的最新价格
        symbol_last_price = fetch_binance_ticker_data(exchange)

        # =====逐批下单
        place_order_reduce_only(exchange, twap_symbol_info_list[i], symbol_last_price, min_qty, price_precision)

        if i < len(twap_symbol_info_list) - 1:
            print(f'Twap {Twap_interval} s 等待')
            time.sleep(Twap_interval)

    order_info = robust(exchange.fapiPrivateGetOpenOrders, )
    if order_info:
        for order in order_info:
            orderId = order['orderId']
            symbol = order['symbol']
            print(symbol, orderId)
            params = {'symbol': symbol, 'orderId': orderId}
            delete_order = robust(exchange.fapiPrivateDeleteOrder, params)
            print(delete_order)
