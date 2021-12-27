"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
import ccxt
import pandas as pd
import time
import os
import json
import requests
import time
import hmac
import hashlib
import base64
from urllib import parse
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
import numpy as np

from xbx.coin_alpha.program.trading.Signals import *
from xbx.coin_alpha.program.trading.Config import *


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
    time.sleep(1)
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
    columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']
    df = df[columns]

    # 删除runtime那行的数据，如果有的话
    df = df[df['candle_begin_time'] != run_time]
    print('结束获取k线数据：', symbol, datetime.now())

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

    # 多进程获取数据
    s_time = time.time()
    with Pool(processes=len(arg_list)) as pl:
        # 利用starmap启用多进程信息
        result = pl.starmap(fetch_binance_swap_candle_data, arg_list)

    df = dict(result)
    print('获取所有币种K线数据完成，花费时间：', time.time() - s_time)
    return df


# 获取币安永续合约账户的当前净值
def fetch_binance_swap_equity(exchange):
    """
    获取币安永续合约账户的当前净值
    """
    # 获取当前账户净值
    balance = exchange.fapiPrivate_get_balance()  # 获取账户净值
    balance = pd.DataFrame(balance)
    equity = float(balance[balance['asset'] == 'USDT']['balance'])
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
    tickers = binance.fapiPublic_get_ticker_24hr()
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
        para = strategy['para']
        factor = strategy['factor']
        if_reverse = strategy['if_reverse']
        hold_period = strategy['hold_period']
        selected_coin_num = strategy['selected_coin_num']

        # ===逐个遍历每一个币种，计算其因子，并且转化周期
        period_df_list = []
        for symbol in symbol_candle_data.keys():
            # =获取相应币种1h的k线，深度拷贝
            df = symbol_candle_data[symbol].copy()

            # =计算因子
            df = eval(f'signal_{factor}')(df, int(para))  # 计算信号

            # =将数据转化为需要的周期
            df['s_time'] = df['candle_begin_time']
            df['e_time'] = df['candle_begin_time']
            df.set_index('candle_begin_time', inplace=True)

            agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'close': 'last', factor: 'first' if if_reverse else 'last'}

            # 转换生成每个策略所有offset的因子
            for offset in range(int(hold_period[:-1])):
                # 转换周期
                period_df = df.resample(hold_period, base=offset).agg(agg_dict)
                period_df['offset'] = offset
                # 保存策略信息到结果当中
                period_df['key'] = f'{factor}_{para}_{hold_period}_{offset}H'  # 创建主键值

                n = 34  # 指标的时间窗口参数
                period_df['median'] = period_df['close'].rolling(window=n).mean()  # 计算中轨
                period_df['std'] = period_df['close'].rolling(n, min_periods=1).std(ddof=0)  # 计算标准差
                period_df['m'] = abs(period_df['close'] - period_df['median']) / period_df['std']  # 计算自适应m
                period_df['up'] = period_df['m'].rolling(window=n).max().shift(1)  # 计算z_score 上限
                period_df['dn'] = period_df['m'].rolling(window=n).min().shift(1)  # 计算z_score 下限
                period_df['upper'] = period_df['median'] + period_df['std'] * period_df['up']  # 计算布林上轨
                period_df['lower'] = period_df['median'] - period_df['std'] * period_df['up']  # 计算布林下轨
                period_df['condition_long'] = (period_df['close'] >= period_df['lower'])  # 允许做多的条件：破下轨，不做多
                period_df['condition_short'] = (period_df['close'] <= period_df['upper'])  # 允许做空的条件：破上轨，不做空

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

        # ===选币数据整理完成，接下来开始选币
        # 多空双向rank
        df['币总数'] = df.groupby(df.index).size()
        # ranks assigned in order they appear in the array
        df['rank'] = df.groupby('s_time')[factor].rank(method='first')
        # 删除不要的币
        df['方向'] = 0

        df.loc[(df['rank'] <= selected_coin_num) & (df['condition_long'] == True), '方向'] = 1
        df.loc[((df['币总数'] - df['rank']) < selected_coin_num) & (df['condition_short'] == True), '方向'] = -1

        df = df[df['方向'] != 0]
        # ===将每个币种的数据保存到dict中
        # 删除不需要的列
        df.drop([factor, '币总数', 'rank'], axis=1, inplace=True)
        df.reset_index(inplace=True)
        select_coin_list.append(df)

    select_coin = pd.concat(select_coin_list)
    print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)
    # debug
    print(select_coin)
    # exit()
    return select_coin


# 计算旧的和新的策略分配资金
def cal_old_and_new_trade_usdt():
    """
    每隔一段时间修改一下trade_usdt
    """
    # =====计算每个策略分配的交易资金
    # 当本地存在trade_usdt_history.txt文件
    if os.path.isfile(File_Name):
        # 读取本地文件记录的trade_usdt
        local_trade_usdt = read_trade_usdt_history(File_Name)

        # 满足更新条件，需要更新trade_usdt
        if (datetime.now() - datetime(2000, 1, 1)).days % Update_Day == 0 and datetime.now().hour == Update_Hour:
            trade_usdt_new = fetch_binance_swap_equity(exchange)  # 最新的账户净值
            trade_usdt_new *= Percent  # 乘以一定的比例，用以最终的交易
            trade_usdt_old = local_trade_usdt  # 本地记录的trade_usdt
        # 不满足更新条件，使用本地的trade_usdt
        else:
            trade_usdt_new = local_trade_usdt
            trade_usdt_old = local_trade_usdt

    # 本地不存在trade_usdt_history.txt文件
    else:
        trade_usdt_new = fetch_binance_swap_equity(exchange)  # 读取币安账户最新的trade_usdt
        trade_usdt_old = trade_usdt_new

    print('trade_usdt_old：', trade_usdt_old, 'trade_usdt_new：', trade_usdt_new, '\n')
    return trade_usdt_old, trade_usdt_new


# 计算每个策略分配的资金
def cal_strategy_trade_usdt(stratagy_list, trade_usdt_new, trade_usdt_old):
    """
    计算每个策略分配的资金
    """
    df = pd.DataFrame()
    # 策略的个数
    strategy_num = len(stratagy_list)
    # 遍历策略
    for strategy in stratagy_list:
        para = strategy['para']
        factor = strategy['factor']
        hold_period = strategy['hold_period']
        selected_coin_num = strategy['selected_coin_num']

        offset_num = int(hold_period[:-1])
        for offset in range(offset_num):
            df.loc[f'{factor}_{para}_{hold_period}_{offset}H', '策略分配资金_旧'] = trade_usdt_old / strategy_num / 2 / \
                offset_num / selected_coin_num
            df.loc[f'{factor}_{para}_{hold_period}_{offset}H', '策略分配资金_新'] = trade_usdt_new / strategy_num / 2 / \
                offset_num / selected_coin_num

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'key'}, inplace=True)

    return df


# 计算实际下单量
def cal_order_amount(symbol_info, select_coin, strategy_trade_usdt, run_time):
    """
    计算每个币种的实际下单量，并且聚会汇总，放到symbol_info变量中
    """
    # 合并每个策略分配的资金
    select_coin = pd.merge(left=select_coin, right=strategy_trade_usdt, how='left')

    # 将策略选币时间end_time与当天的凌晨比较，越过凌晨时刻则用本周期的资金，否则用上周期资金
    select_coin['策略分配资金'] = np.where(select_coin['e_time'] >= run_time.replace(
        hour=Update_Hour), select_coin['策略分配资金_新'], select_coin['策略分配资金_旧'])
    # 计算下单量
    select_coin['目标下单量'] = select_coin['策略分配资金'] / select_coin['close'] * select_coin['方向']
    print(select_coin[['key', 's_time', 'symbol', '方向', '策略分配资金_旧', '策略分配资金_新', '策略分配资金']], '\n')

    # 对下单量进行汇总
    symbol_info['目标下单量'] = select_coin.groupby('symbol')[['目标下单量']].sum()
    symbol_info['目标下单量'].fillna(value=0, inplace=True)
    symbol_info['目标下单份数'] = select_coin.groupby('symbol')[['方向']].sum()
    symbol_info['实际下单量'] = symbol_info['目标下单量'] - symbol_info['当前持仓量']

    # 删除实际下单量为0的币种
    symbol_info = symbol_info[symbol_info['实际下单量'] != 0]
    return symbol_info


# 下单
def place_order(symbol_info, symbol_last_price):
    """
    下单
    """
    for symbol, row in symbol_info.dropna(subset=['实际下单量']).iterrows():

        # 计算下单量：按照最小下单量向下取整
        quantity = row['实际下单量']
        quantity = float(f'{quantity:.{min_qty[symbol]}f}')
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

        # 下单参数
        params = {'symbol': symbol, 'side': side, 'type': 'LIMIT', 'price': price, 'quantity': quantity,
                  'clientOrderId': str(time.time()), 'timeInForce': 'GTC'}
        # 下单
        print('下单参数：', params)
        open_order, _ = retry_wrapper(exchange.fapiPrivate_post_order, params, sleep_seconds=5)
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


# 重试机制
def retry_wrapper(func, params, sleep_seconds=5, retry_times=5):
    """
    需要不断重试的函数，可以使用本函数调用。
    func: 需要重试的函数名
    """
    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result, params
        except Exception as e:
            print(func.__name__, '函数报错，报错内容：', str(e), '程序暂停，sleep时间：', sleep_seconds)
            time.sleep(sleep_seconds)
        else:
            pass
        finally:
            pass
    else:
        raise ValueError(func.__name__, '函数报错重试次数过多，程序退出。')


def store_trade_usdt_history(equity, run_time, equity_file_name):
    """
    将数据保存到本地
    """
    equity_str = f'{run_time} {equity}\n'

    # 把更新的时间及净值信息存储到文件的最后一行
    with open(equity_file_name, 'a+') as f:
        f.write(equity_str)


def read_trade_usdt_history(equity_file_name):
    # 从文件的最后一行获取上个周期的净值信息
    """
    2020-08-02 00:00:00 1251.01426
    2020-08-03 00:00:00 1451.04726
    2020-08-04 00:00:00 1351.016
    """
    with open(equity_file_name, 'r') as f:
        end_line = f.readlines()[-1]  # 获取最后一行数据
        data = float(end_line.split()[-1])  # 获取按空格分割字符后的最后一个值
    return data


def send_dingding_msg_every_loop(equity, select_coin, symbol_info, symbol_amount, symbol_last_price):
    """
    发送钉钉
    """
    # 获取多头仓位
    long_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] > 0]['当前持仓量']).dropna()
    # 获取空头仓位
    short_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] < 0]['当前持仓量']).dropna()

    dingding_msg = f'账户净值： {equity:8.2f}\n'
    dingding_msg += f'多仓净值: {sum(long_position_equity):8.2f}\n'
    dingding_msg += f'空仓净值: {sum(short_position_equity):8.2f}\n'
    dingding_msg += '策略持仓\n\n'
    dingding_msg += select_coin[['key', 'symbol', '方向']].to_string(index=False)
    dingding_msg += '\n下单信息\n'
    dingding_msg += symbol_amount.to_string(index=False)

    send_dingding_msg(dingding_msg, dingding_id, dingding_secret)
    print('发送钉钉成功')


def build_message(equity, select_coin, symbol_info, symbol_amount, symbol_last_price):
    """
    发送钉钉
    """
    # 获取多头仓位
    long_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] > 0]['当前持仓量']).dropna()
    # 获取空头仓位
    short_position_equity = (symbol_last_price * symbol_info[symbol_info['当前持仓量'] < 0]['当前持仓量']).dropna()

    dingding_msg = f'账户净值： {equity:8.2f}\n'
    dingding_msg += f'多仓净值: {sum(long_position_equity):8.2f}\n'
    dingding_msg += f'空仓净值: {sum(short_position_equity):8.2f}\n'
    dingding_msg += '策略持仓\n\n'
    dingding_msg += select_coin[['key', 'symbol', '方向']].to_string(index=False)
    dingding_msg += '\n下单信息\n'
    dingding_msg += symbol_amount.to_string(index=False)
    return dingding_msg


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
# 计算钉钉时间戳


def cal_timestamp_sign(secret):
    # 根据钉钉开发文档，修改推送消息的安全设置https://ding-doc.dingtalk.com/doc#/serverapi2/qf2nxq
    # 也就是根据这个方法，不只是要有robot_id，还要有secret
    # 当前时间戳，单位是毫秒，与请求调用时间误差不能超过1小时
    # python3用int取整
    timestamp = int(round(time.time() * 1000))
    # 密钥，机器人安全设置页面，加签一栏下面显示的SEC开头的字符串
    secret_enc = bytes(secret.encode('utf-8'))
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = bytes(string_to_sign.encode('utf-8'))
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    # 得到最终的签名值
    sign = parse.quote_plus(base64.b64encode(hmac_code))
    return str(timestamp), str(sign)


# 发送钉钉消息
def send_dingding_msg(content, robot_id='29b9dbba2ec06550ae33173974639b8cf573a700290bc857d550fdf72640ebc6',
                      secret='SEC22f3b5e4f6e7d2a50f24f18cde9c411b18239ab035032413c00eab87251bed3e'):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。例如如下地址：https://oapi.dingtalk.com/robot/
    send?access_token=81a0e96814b4c8c3132445f529fbffd4bcce66
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """
    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}
        headers = {"Content-Type": "application/json;charset=utf-8"}
        # https://oapi.dingtalk.com/robot/send?access_token=XXXXXX&timestamp=XXX&sign=XXX
        timestamp, sign_str = cal_timestamp_sign(secret)
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id + \
              '&timestamp=' + timestamp + '&sign=' + sign_str
        body = json.dumps(msg)
        requests.post(url, data=body, headers=headers, timeout=10)
        print('成功发送钉钉')
    except Exception as e:
        print("发送钉钉失败:", e)
