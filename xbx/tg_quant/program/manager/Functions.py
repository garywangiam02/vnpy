import pandas as pd
import numpy as np
import time
import ccxt
from multiprocessing import Pool, cpu_count
from datetime import datetime
import os
import telegram
from config.Config import *


class TgNotifyRobot(object):

    def __init__(self, token, chat_id):
        super(TgNotifyRobot, self).__init__()
        self.token = token
        self.chat_id = chat_id
        self.bot = telegram.Bot(token)

    def send_text(self, content):
        '''
        发送文本消息
        '''
        if content == None:
            return
        self.bot.send_message(self.chat_id, content)

    def send_dataframe(self, content):
        '''
        发送dataframe消息
        '''
        self.bot.send_message(self.chat_id, content.to_markdown(), parse_mode='Markdown')

    def send_photo():
        pass

    def send_msg(self, *mssg):
        text = ''
        for i in mssg:
            text += str(i)
        self.bot.send_message(self.chat_id, text)


# 用于通知
tg_notify_robot = TgNotifyRobot(token=TELEGRAM_TOKEN, chat_id=TELEGRAM_CHAT_ID)


# =====获取数据
# 获取单个币种的1小时数据
# @robust
def fetch_binance_swap_candle_data(exchange, symbol, run_time, limit=1000):
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
    kline = robust(exchange.fapiPublic_get_klines, {'symbol': symbol, 'interval': '1h', 'limit': limit})

    # 将数据转换为DataFrame
    columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(kline, columns=columns, dtype='float')

    # 整理数据
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
    df['symbol'] = symbol  # 添加symbol列
    columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low',
               'close', 'volume', 'quote_volume', 'taker_buy_quote_asset_volume']
    df = df[columns]

    # 删除runtime那行的数据，如果有的话
    df = df[df['candle_begin_time'] != run_time]

    return symbol, df


# 并行获取所有币种永续合约数据的1小时K线数据
def fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time):
    """
    并行获取所有币种永续合约数据的1小时K线数据
    :param exchange:
    :param symbol_list:
    :param run_time:
    :return:
    """
    # 创建参数列表
    arg_list = [(exchange, symbol, run_time) for symbol in symbol_list]
    s_time = time.time()

    with Pool(processes=4) as pl:
        # 利用starmap启用多进程信息
        result = pl.starmap(fetch_binance_swap_candle_data, arg_list)

    df = dict(result)
    print('获取所有币种K线数据完成，花费时间：', time.time() - s_time, '\n')
    return df


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
    tickers = binance.fapiPublic_get_ticker_24hr()
    tickers = pd.DataFrame(tickers, dtype=float)
    tickers.set_index('symbol', inplace=True)

    return tickers['lastPrice']


# =====获取持仓
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
    position_risk = robust(exchange.fapiPrivate_get_positionrisk)

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
    amount = float(balance[balance['asset'] == asset]['free'])
    print(f'查询到现货账户有{amount} {asset}')
    return amount


def replenish_bnb(exchange, balance):
    amount_bnb = float(balance[balance['asset'] == 'BNB']['balance'].iloc[0])
    print(f"当前账户剩余{amount_bnb} BNB")
    if amount_bnb < 0.001:
        spot_bnb_amount = get_spot_balance(exchange, 'BNB')
        print(f"当前现货账户持有{spot_bnb_amount} BNB")
        if spot_bnb_amount < 0.01:
            print("从现货市场买入10 USDT等值BNB并转入合约账户")
            spot_usdt_amount = get_spot_balance(exchange, 'USDT')
            if spot_usdt_amount < 10:
                transfer_future_to_spot(exchange, 'USDT', 10.01 - spot_usdt_amount)
            spot_buy_quote(exchange, 'BNBUSDT', 10)
            time.sleep(2)
            spot_bnb_amount_new = get_spot_balance(exchange, 'BNB')
            transfer_spot_to_future(exchange, 'BNB', spot_bnb_amount_new)
            message = f"成功买入{spot_bnb_amount} BNB并转入U本位合约账户"
            print(message)
        else:
            transfer_spot_to_future(exchange, 'BNB', spot_bnb_amount)
            print(f'把已有{spot_bnb_amount}BNB转入合约账户')


# 增加了数据,供后续TWAP使用
def twap_cal_order_amount(symbol_info, select_coin, strategy_trade_usdt):
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left').fillna(0)
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']

    # 对下单量进行汇总
    symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum()
    symbol_info['目标下单量'].fillna(value=0, inplace=True)
    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']
    select_coin.sort_values('s_time', inplace=True)
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
            if abs(order.iat[0, 4]) > Max_one_order_amount:
                ratio = (Max_one_order_amount - safe_amount) / abs(order.iat[0, 4])
                add_order = order.copy()
                add_order[['当前持仓量', '目标下单量', '目标下单份数', '实际下单量', '实际下单资金']] = add_order[[
                    '当前持仓量', '目标下单量', '目标下单份数', '实际下单量', '实际下单资金']] * (1 - ratio)
                symbol_info.iloc[i, :-1] = symbol_info.iloc[i, :-1] * ratio
                add_order_list.append(add_order)
        symbol_info.drop(drop_list, inplace=True)
        twap_symbol_info_list[num] = symbol_info.copy()
        add_df = pd.concat(add_order_list)
        twap_symbol_info_list.append(add_df.copy())
        is_twap = True if max(abs(add_df['实际下单资金'])) > Max_one_order_amount else False
        num += 1
    print(f'Twap批次:{len(twap_symbol_info_list)}\n')

    # 以下代码块主要功能为调整不同批次间的多空平衡问题，资金量不是特别大的老板可以注释掉
    '''*******************************************************************************'''
    bl = []
    for x in twap_symbol_info_list:
        bl.append([round(abs(x['实际下单资金']).sum(), 1), round(x['实际下单资金'].sum(), 1)])
    _summary_df = pd.DataFrame(bl, columns=['下单金额', '多空失衡金额'])
    print(_summary_df, '\n')
    print(_summary_df.sum())
    is_adjust = False
    for j in range(len(twap_symbol_info_list)):
        adjust_df = pd.DataFrame()
        main_order_df = twap_symbol_info_list[j].copy()
        for i in range(5, 25, 1):
            if abs(main_order_df.iloc[:int(len(main_order_df) * (i - 2) / i)]['实际下单资金'].sum()) > 1000:
                adjust_df = main_order_df.iloc[int(len(main_order_df) * (i - 2) / i):]
                main_order_df = main_order_df.iloc[:int(len(main_order_df) * (i - 2) / i)]
                print((i - 2) / i, f'\n第 {j+1} 批twap订单需要调整\n')
                is_adjust = True
                twap_symbol_info_list[j] = main_order_df.copy()
                break
            else:
                continue
        else:
            pass
            # twap订单无需调整

        for i in range(adjust_df.shape[0]):
            temp = [x['实际下单资金'].sum() for x in twap_symbol_info_list[j + 1:]]
            max_ind = temp.index(max(temp))
            min_ind = temp.index(min(temp))
            if adjust_df.iat[i, 4] < 0:
                twap_symbol_info_list[max_ind + j + 1] = twap_symbol_info_list[max_ind + j + 1].append(adjust_df.iloc[i:i + 1])
            else:
                twap_symbol_info_list[min_ind + j + 1] = twap_symbol_info_list[min_ind + j + 1].append(adjust_df.iloc[i:i + 1])

    if is_adjust:
        bl = []
        for x in twap_symbol_info_list:
            bl.append([round(abs(x['实际下单资金']).sum(), 1), round(x['实际下单资金'].sum(), 1)])
        summary_df = pd.DataFrame(bl, columns=['下单金额', '多空失衡金额'])
        print(summary_df, '\n')
        print(summary_df.sum())
    '''*******************************************************************************'''
    all_df = pd.concat(twap_symbol_info_list)
    try:
        assert abs(all_df['目标下单份数'].sum()) < 1e-6
    except Exception as e:
        print('多空不平衡，Twap订单生产出错')
        #send_dingding_msg('多空不平衡，Twap订单生产出错： %s'%str(e))
    return twap_symbol_info_list


# 获取币安的ticker数据
def fetch_binance_ticker_price(binance, symbol_info):
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
    ticker_df_list = []
    for i in range(symbol_info.shape[0]):
        symbol = symbol_info.index[i]
        ticker = robust(binance.fapiPublic_get_ticker_price, {'symbol': symbol})
        ticker_df_list.append(ticker)

    tickers = pd.DataFrame(ticker_df_list, dtype=float)
    tickers.set_index('symbol', inplace=True)
    return tickers['price']


def twap_place_order(exchange, symbol_info, select_coin, strategy_trade_usdt, min_qty, price_precision):
    # 新增参数
    # 单次最大下单金额
    Max_one_order_amount = 800  # 100
    # 订单间隔时间
    Twap_interval = 2

    # 这里使用本文件内的twap_cal_order_amount函数
    symbol_info = twap_cal_order_amount(symbol_info, select_coin, strategy_trade_usdt)
    # 补全历史持仓的最新价格信息
    if symbol_info['实际下单资金'].isnull().any():
        symbol_last_price = fetch_binance_ticker_price(exchange, symbol_info)
        nan_symbol = symbol_info.loc[symbol_info['实际下单资金'].isnull(), '实际下单资金'].index
        symbol_info.loc[nan_symbol, '实际下单资金'] = symbol_info.loc[nan_symbol, '实际下单量'] * symbol_last_price[nan_symbol]
    print('实际下单量：\n', symbol_info, '\n')

    # 使用twap算法拆分订单
    twap_symbol_info_list = get_twap_symbol_info_list(symbol_info, Max_one_order_amount)

    for i in range(len(twap_symbol_info_list)):

        # =====获取币种的最新价格
        symbol_last_price = fetch_binance_ticker_price(exchange, symbol_info)

        # =====逐批下单
        place_order(twap_symbol_info_list[i], symbol_last_price, min_qty, price_precision)

        if i < len(twap_symbol_info_list) - 1:
            print(f'Twap {Twap_interval} s 等待')
            time.sleep(Twap_interval)


# 下单
def place_order(exchange, symbol_info, symbol_last_price, min_qty, price_precision):

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
        open_order = robust(exchange.fapiPrivate_post_order, params)
        print('下单完成，下单信息：', open_order, '\n')


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
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内


def robust(actual_do, *args, **keyargs):

    result = run_function_till_success(function=lambda: actual_do(*args, **keyargs), tryTimes=TRY_TIMES, sleepTimes=SLEEP_TIMES)
    if result:
        return result[0]
    else:
        os._exit(0)


# ===下单专用容错函数（更快，一个币种下单失败不影响其他币种继续下单）
# 不发送每次尝试的报错，只在尝试tryTimes次失败后发送一次总的报错信息
def run_function_till_success_order(function, tryTimes=5, sleepTimes=60):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    reason = ''
    while True:
        if retry > tryTimes:
            return [False, reason]
        try:
            result = function()
            return [result, retry]
        except (Exception) as e:
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内


# 尝试出错后，不是直接退出程序，而是返回False以供处理
# 下单尝试的频率加快，Try5，sleep10
def robust_order(actual_do, *args, **keyargs):
    tryTimes = 5
    sleepTimes = 10
    result = run_function_till_success_order(function=lambda: actual_do(
        *args, **keyargs), tryTimes=tryTimes, sleepTimes=sleepTimes)
    # 如果运行成功，result = [result, retry]
    # 如果运行不成功，result = [False,reason]
    if not result[0]:
        # 报错内容发送给钉钉
        tg_notify_robot.send_msg(str(result[1]) + '\n' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数')
    return result


def get_asset(ttl):
    ttl_msg = '账户统计\n'
    ttl_value = 0

    for i in ttl:
        BINANCE_CONFIG = {
            'apiKey': i.get('apiKey'),
            'secret': i.get('secret'),
            'timeout': 3000,
            'rateLimit': 10,
            # 'hostname': 'binancezh.com',  # 无法fq的时候启用
            'enableRateLimit': False
        }
        exchange = ccxt.binance(BINANCE_CONFIG)

        def get_u_balance():
            # u本位合约u_balance
            u_balance = exchange.fapiPrivateV2_get_balance()
            u_balance_usdt = 0
            for item in u_balance:
                if float(item.get('balance', 0)) > 0:
                    if item['asset'] == 'USDT':
                        u_balance_usdt = float(item['balance']) + float(item['crossUnPnl'])
            return u_balance_usdt

        def get_bi_balance_usdt():
            # 币本位合约
            bi_balance = exchange.dapiPrivate_get_balance()
            bi_one = {}
            for item in bi_balance:
                if float(item.get('balance', 0)) > 0:
                    bi_one[item['asset']] = float(item['balance']) + float(item['crossUnPnl'])
            bi_balance_usdt = 0
            for k, v in bi_one.items():
                delivery_symbol = '{}{}'.format(k, 'USD_PERP')
                params = {'symbol': delivery_symbol, 'limit': 5}
                delivery_buy1_price = float(exchange.dapiPublicGetDepth(params=params)['bids'][0][0])
                one_bi_balance_usdt = float(delivery_buy1_price) * float(v)
                bi_balance_usdt += one_bi_balance_usdt
            return bi_balance_usdt

        def get_spot_balance_usdt():
            # 现货
            balance = exchange.privateGetAccount()
            balance_list = balance['balances']
            res_list = []
            spot_balance_usdt = 0
            for item in balance_list:
                if float(item['free']) > 0:
                    res_list.append({item['asset']: float(item['free'])})
                    if item['asset'] == 'USDT':
                        spot_balance_usdt += float(item['free'])
                    else:
                        if item['asset'] == 'LDBNB':
                            item['asset'] = 'BNB'
                        spot_sell1_price = exchange.fetch_ticker('{}/USDT'.format(item['asset']))
                        spot_balance_usdt += float(item['free']) * float(spot_sell1_price['close'])
            return spot_balance_usdt

        u_balance_usdt = get_u_balance()
        bi_balance_usdt = get_bi_balance_usdt()
        spot_balance_usdt = get_spot_balance_usdt()

        value = float(u_balance_usdt) + float(bi_balance_usdt) + float(spot_balance_usdt)
        msg = '账户名:' + i.get('account_name', '') + '\n'
        msg += '合计余额:' + str(value) + '\n'
        msg += '现货余额USDT:' + str(spot_balance_usdt) + '\n'
        msg += '币本位合约余额USDT:' + str(bi_balance_usdt) + '\n'
        msg += 'U本位合约余额USDT:' + str(u_balance_usdt) + '\n'
        ttl_msg += msg
        ttl_value += value

    ttl_msg += '合计余额USDT' + str(ttl_value) + '\n'
    ttl_msg += '记录时间' + str(datetime.now()) + '\n'
    print(ttl_msg)
    return ttl_msg
