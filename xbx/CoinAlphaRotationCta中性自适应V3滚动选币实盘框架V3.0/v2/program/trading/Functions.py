import pandas as pd
import numpy as np
import time
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
from Signals import *
from Config import *
from Utility import robust
import configparser
config = configparser.ConfigParser()


# =====获取数据
# 获取单个币种的1小时数据
# @robust
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
    # 获取数据
    # kline = exchange.fapiPublic_get_klines({'symbol': symbol, 'interval': '1h', 'limit': limit})
    kline = robust(exchange.fapiPublic_get_klines,{'symbol': symbol, 'interval': '1h', 'limit': limit})

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
 
    # 多进程获取数据
    with Pool(processes=4) as pl:
        # 利用starmap启用多进程信息
        result = pl.starmap(fetch_binance_swap_candle_data, arg_list)
                    

    df = dict(result)
    print('获取所有币种K线数据完成，花费时间：', time.time() - s_time, '\n')
    return df


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
    config.read('config.ini')

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
                print('no data',symbol)
                if symbol not in no_enough_data_symbol:
                    no_enough_data_symbol.append(symbol)                
                continue
                
            if len(df) < 1500-1:
                print('no enough data',symbol)
                if symbol not in no_enough_data_symbol:
                    no_enough_data_symbol.append(symbol)
                continue

            df[c_factor] = 0

            for factor_dict in factors:

                factor          = factor_dict['factor']
                para            = factor_dict['para']
                if_reverse      = factor_dict['if_reverse']
                tmp_df = df.copy()
                tmp_df = eval(f'signal_{factor}')(tmp_df, int(para))  # 计算信号

                # 初始化
                df[factor + '_因子'] = np.nan 

                # =空计算
                if np.isnan(tmp_df.iloc[-1][factor]):
                    continue

                # if if_reverse:              
                #     df[factor + '_因子'] = - df[factor]  
                # else:
                df[factor + '_因子'] = tmp_df[factor] 



            # =将数据转化为需要的周期
            df['s_time'] = df['candle_begin_time']
            df['e_time'] = df['candle_begin_time']
            df.set_index('candle_begin_time', inplace=True)

            agg_dict = {'symbol': 'first', 's_time': 'first', 'e_time': 'last', 'close': 'last', c_factor: 'last'}

            for factor_dict in factors:
                factor          = factor_dict['factor']
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
            factor  = factor_dict['factor']
            weight  = factor_dict['weight']
            df[factor + '_排名'] = df.groupby('s_time')[factor + '_因子'].rank()
            df[c_factor] += df[factor + '_排名']*weight


        # ===选币数据整理完成，接下来开始选币
        # 多空双向rank
        df['币总数'] = df.groupby(df.index).size()
        df['rank'] = df.groupby('s_time')[c_factor].rank(ascending=False, method='first')
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

    return select_coin


# =====辅助功能函数
# ===下次运行时间，和课程里面讲的函数是一样的
def next_run_time(time_interval, ahead_seconds=5, cheat_seconds=100):
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
def sleep_until_run_time(time_interval, ahead_time=1, if_sleep=True, cheat_seconds=120):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param if_sleep:
    :param time_interval:
    :param ahead_time:
    :return:
    """
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time, cheat_seconds)
    # sleep
    if if_sleep:
        time.sleep(max(0, (run_time - datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if datetime.now() > run_time:
                break
    return run_time


def transfer_future_to_spot(exchange, asset, amount):
    info = robust(exchange.sapiPostFuturesTransfer, {
        'type': 2,  # 1：现货至u本位合约；2：u本位合约至现货
        'asset': asset,
        'amount': amount,
    })
    print(f'从U本位合约至现货账户划转成功：{info}，划转数量：{amount} {asset}，时间：{datetime.datetime.now()}')


def transfer_spot_to_future(exchange, asset, amount):
    info = robust(exchange.sapiPostFuturesTransfer, {
        'type': 1,  # 1：现货至u本位合约；2：u本位合约至现货
        'asset': asset,
        'amount': amount,
    })
    print(f'从现货至U本位合约账户划转{amount} {asset}成功：{info}，时间：{datetime.datetime.now()}')


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
    amount_bnb = float(balance[balance['asset'] == 'BNB']['walletBalance'].iloc[0])
    print(f"当前账户剩余{amount_bnb} BNB")
    if amount_bnb < 0.001:
        spot_bnb_amount = get_spot_balance(exchange, 'BNB')
        print(f"当前现货账户持有{spot_bnb_amount} BNB")
        if spot_bnb_amount > 0.001:
            transfer_spot_to_future(exchange, 'BNB', spot_bnb_amount)
            print(f"把现货账户中已有{spot_bnb_amount} BNB 转入U本位合约账户")
        else:
            print("从现货市场买入10 USDT等值BNB并转入合约账户")
            spot_usdt_amount = get_spot_balance(exchange, 'USDT')
            if spot_usdt_amount < 10.2:
                transfer_future_to_spot(exchange, 'USDT', 10.21 - spot_usdt_amount)
            spot_buy_quote(exchange, 'BNBUSDT', 10.2)
            # 等待5秒后重新获取BNB现货账户余额。在市场行情剧烈波动的情况下可能会获取不到持仓情况，则等待下一个周期处理
            time.sleep(5)
            spot_bnb_amount = get_spot_balance(exchange, 'BNB')
            if spot_bnb_amount > 0:
                transfer_spot_to_future(exchange, 'BNB', spot_bnb_amount)
                message = f"成功买入{spot_bnb_amount} BNB并转入U本位合约账户"
                print(message)
            else:
                print(f"未查询到现货账户BNB资产，等待下一周期处理")

if __name__ == '__main__':
   account = exchange.f_account()
   assets = pd.DataFrame(account['assets'], dtype=float)  # 帐户的总信息
   replenish_bnb(exchange, assets)                

