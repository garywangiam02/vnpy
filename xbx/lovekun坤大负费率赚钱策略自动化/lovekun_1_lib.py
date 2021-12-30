import time
from datetime import date, datetime, timedelta
import traceback
import os
import pandas as pd
import math
import json
import requests
import hmac
import hashlib
import base64
from urllib import parse
from config import *


'''
这里是一些通用的类、库、工具方法
'''

class TimeCost():
    """
    用于统计两行代码之间的耗时
    """
    time_dict = {}

    def start(self, key = 'default'):
        self.time_dict[key] = datetime.now()

    def end(self, key = 'default'):
        print(f'[{key}] 耗时: {datetime.now() - self.time_dict[key]}')
        del self.time_dict[key]


class StrategyBase:
    """
    策略基类
    """
    
    name = "StrategyBase"
    sleep_sec_every_loop = 3     # 每轮执行end后sleep的时间间隔（秒）
    run_once = False             # 是否只执行一轮
    config = Config()

    def prepare_once(self):
        """
        策略启动之后首先执行且只执行一次的准备过程
        """
        pass

    def prepare_loop(self):
        """
        策略启动之后每轮执行前的准备过程
        """
        pass

    def execute(self):
        """
        每轮执行的策略核心逻辑
        """
        pass

    def end(self):
        """
        每轮执行策略后的收尾工作
        """
        pass

class StrategyRunner:
    """
    策略运行器
    """

    run_once = False

    def run(self, strategy : StrategyBase):
        """
        启动策略

        :param strategy: 策略实例
        :param prepare_params: 准备参数
        """

        print('\n===== [StrategyRunner][%s] 启动() at %s\n' % (strategy.name, datetime.now()))
        tc = TimeCost()

        # 准备阶段（只运行一次）
        try:
            print('\n===== [StrategyRunner][%s] 启动准备()\n' % (strategy.name))
            tc.start('prepare_once')
            strategy.prepare_once()
            tc.end('prepare_once')
        except Exception as e:
            print('===== [StrategyRunner][ERROR.准备阶段]', str(e))
            print(traceback.format_exc())
            print('===== 程序退出')
            exit()

        error_times = 0

        while True:
            # 循环开始
            print('\n' + '-' * 15 + ' [StrategyRunner] loop '  + '-' * 15 + '\n')

            try:
                # 准备阶段（每次循环）
                tc.start('prepare_loop')
                print('\n===== [StrategyRunner][%s] 循环 - 准备\n' % (strategy.name))
                strategy.prepare_loop()
                tc.end('prepare_loop')

                # 执行阶段
                tc.start('execute')
                print('\n===== [StrategyRunner][%s] 循环 - 执行 \n' % (strategy.name))
                strategy.execute()
                tc.end('execute')

                # 收尾阶段
                tc.start('end')
                print('\n===== [StrategyRunner][%s] 循环 - 收尾 \n' % (strategy.name))
                strategy.end()
                tc.end('end')

                # 如果只运行一次，则结束
                if self.run_once:
                    exit()

                # sleep至下一轮
                error_times = 0
                time.sleep(strategy.sleep_sec_every_loop)
            except Exception as e:
                print('===== [StrategyRunner][ERROR]', str(e))
                print(traceback.format_exc())
                
                error_times += 1
                max_err_times = 30  # 最大错误次数

                if error_times < max_err_times:
                    print(f'===== [StrategyRunner][Retry {error_times}] sleep 10s 后再次尝试运行')
                    send_dingding_msg(traceback.format_exc() + f'\n\n[StrategyRunner][Retry {error_times}] sleep 10s 后再次尝试运行', strategy.config.dd_root_id, strategy.config.dd_secret)
                    time.sleep(10)
                else:
                    print(f'===== [StrategyRunner] 失败次数超过{max_err_times}次，程序退出')
                    send_dingding_msg(traceback.format_exc() + f'\n\n[StrategyRunner][{strategy.name}] 失败次数超过{max_err_times}次，程序退出', strategy.config.dd_root_id, strategy.config.dd_secret)
                    os._exit(1)

#====================
#   common utils
#====================

def now_str_short():
    """
    获取当前时间的格式化字符串（短）

    :return:
    """
    return time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))

# 重试机制
def retry_wrapper(func, params={}, act_name='', sleep_seconds=3, retry_times=5):
    """
    需要在出错时不断重试的函数，例如和交易所交互，可以使用本函数调用。

    :param func: 需要重试的函数名
    :param params: func的参数
    :param act_name: 本次动作的名称
    :param sleep_seconds: 报错后的sleep时间
    :param retry_times: 为最大的出错重试次数
    :return:
    """

    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result
        except Exception as e:
            print(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            print(traceback.format_exc())
            time.sleep(sleep_seconds)
    else:
        # send_dingding_and_raise_error(output_info)
        raise ValueError(act_name, '报错重试次数超过上限，程序退出。')

def sleep_if_in_special_hours():
    """当前小时如果是资金费率结算周期，则等待20s"""
    # 避免资金费率结算，多收手续费
    # 参考文档：https://www.binancezh.co/zh-CN/support/faq/360033525031
    # TODO 如果运行环境timezone不是utc+8的倍数，会有问题
    now = datetime.now()
    if (now.minute == 0 and now.hour % 8 == 0):
        print('* 当前为资金费率结算周期 ', datetime.now())
        if now.second > 20:
            print('* 当前时间已过20s，无需等待')
        else:
            sleep_sec = max(20 - now.second, 0.1)
            print('* 等待 %s 秒' % sleep_sec)
            time.sleep(sleep_sec) 
        print('* 等待结束，当前时间: ', datetime.now(), '\n\n')

def sleep_by_time_interval(time_interval, skip_sleep = False, delta_seconds = 0):
    """
    根据当前时间和时间间隔，sleep至下个整点（分钟、小时、天）

    Args:
        time_interval: 1m, 2h, 3d, ...
        skip_sleep: True / False
        delta_seconds: 差值（秒），比如要提早30s结束sleep，则delta_seconds=-30
    """
    now_time = datetime.now()
    run_time = None

    # 分钟级周期
    if 'm' in time_interval:
        run_time = now_time.replace(minute=0, second=0, microsecond=0)
        time_step = timedelta(minutes=int(time_interval.replace('m', '')))

    # 小时级周期
    elif 'h' in time_interval:
        run_time = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
        time_step = timedelta(hours=int(time_interval.replace('h', '')))
    
    # 天级周期
    elif 'd' in time_interval:
        run_time = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
        time_step = timedelta(days=int(time_interval.replace('d', '')))

    # 非法周期
    else:
        raise ValueError('[common_utils.sleep_by_time_interval()] INVALID time_interval : %s' % time_interval)

    while run_time < now_time:
        run_time = run_time + time_step

    real_sleep_target =  run_time + timedelta(seconds=delta_seconds)
    sleep_to(real_sleep_target, skip_sleep)

    prev_run_time = run_time - time_step
    
    return run_time, prev_run_time

def sleep_to(run_time, skip_sleep = False, delta_seconds = 0, desc = ""):
    now = datetime.now()
    target = run_time + timedelta(seconds=delta_seconds)
    time_left = target - now
    if delta_seconds < 0:
        print(f'sleep目标: {run_time} ({delta_seconds}s) = {target}, 当前时间: {now}, 预计等待: {time_left} {desc}')
    elif delta_seconds > 0:
        print(f'sleep目标: {run_time} (+{delta_seconds}s) = {target}, 当前时间: {now}, 预计等待: {time_left} {desc}')
    else:
        print(f'sleep目标: {run_time}, 当前时间: {now}, 预计等待: {time_left} {desc}')

    if datetime.now() >= target:
        print('当前时间已过，跳过等待')
        return

    # sleep至目标时间
    if skip_sleep:
        time.sleep(0.5)
    else:
        if time_left.days >= 0:
            sleep_sec = max(0.01, time_left.seconds)
            # print('sleep_sec ', sleep_sec)
            time.sleep(sleep_sec)
            # 由于sleep不太精准，最后一段时间通过以下方式等待时间到达目标时间
            while True:
                if datetime.now() >= target:
                    break
    
    print('恢复sleep: %s (当前时间)' % datetime.now())


#====================
#   binance utils
#====================

def binance_u_furture_get_exchangeinfo(exchange):
    """
    获取U本位合约交易规则和交易对
    """
    exchange_info = retry_wrapper(exchange.fapiPublic_get_exchangeinfo, act_name='获取U本位合约交易规则和交易对')

    return exchange_info

def binance_u_furture_get_trade_rules_from_exchangeinfo(exchange_info):
    """
    从交易规则信息中获取币种最小交易量和币种下单精度
    """

    # 整理币种最小交易量
    min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}
    # 案例：{'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0}
    # print('\n min_qty: \n', min_qty)

    # 整理币种下单精度
    price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in exchange_info['symbols']}
    # 案例：{'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3}
    # print('\n price_precision: \n', min_qty)
    
    return min_qty, price_precision

def binance_u_furture_get_position(exchange, symbol_list):
    """
    获取U本位合约用户持仓

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
    position_risk = retry_wrapper(exchange.fapiPrivate_get_positionrisk, act_name='获取U本位合约用户持仓')

    # 将原始数据转化为dataframe
    position_risk = pd.DataFrame(position_risk, dtype='float')

    # 整理数据
    position_risk.rename(columns={'positionAmt': '当前持仓量', 'entryPrice': '当前持仓均价', 'markPrice': '当前标记价格'}, inplace=True)
    position_risk = position_risk[position_risk['当前持仓量'] != 0]  # 只保留有仓位的币种
    position_risk.set_index('symbol', inplace=True)  # 将symbol设置为index

    # 创建symbol_info
    symbol_info = pd.DataFrame(index=symbol_list, columns=['当前持仓量', '当前持仓均价', '当前标记价格', '当前持仓价值'])
    symbol_info['当前持仓量'] = position_risk['当前持仓量']
    symbol_info['当前标记价格'] = position_risk['当前标记价格']
    symbol_info['当前持仓均价'] = position_risk['当前持仓均价']
    symbol_info['当前持仓价值'] = abs(symbol_info['当前持仓量'].astype(float)) * symbol_info['当前标记价格'].astype(float)
    symbol_info['当前持仓量'].fillna(value=0, inplace=True)
    symbol_info['当前持仓均价'].fillna(value=0, inplace=True)
    symbol_info['当前标记价格'].fillna(value=0, inplace=True)
    symbol_info['当前持仓价值'].fillna(value=0, inplace=True)

    return symbol_info

# U本位下单
def binance_u_future_place_order(exchange, params):
    """
    U本位合约下单

    参考文档:
        - 下单：https://binance-docs.github.io/apidocs/futures/cn/#trade-3
        - 测试下单：https://binance-docs.github.io/apidocs/futures/cn/#trade-4

    Params:
        params: dict 根据API文档填写参数对象 
    """
    return retry_wrapper(exchange.fapiPrivate_post_order, params=params, act_name='U本位合约下单')

# 最新价格
def binance_u_furture_fetch_ticker_price(exchange, symbol_info):
    """
    使用ccxt的接口fapiPublic_get_ticker_24hr()获取ticker数据

    https://binance-docs.github.io/apidocs/futures/cn/#8ff46b58de

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
        ticker = retry_wrapper(exchange.fapiPublic_get_ticker_price, params={ 'symbol': symbol }, act_name='获取U本位最新价格')
        ticker_df_list.append(ticker)

    tickers = pd.DataFrame(ticker_df_list, dtype=float)
    tickers.set_index('symbol', inplace=True)
    return tickers['price']

def binance_u_furture_fetch_all_swap_candle_data(exchange, symbol_list, interval, run_time, limit = 1000, use_thread = True):
    """
    并行获取所有币种永续合约数据的K线数据

    :param exchange:
    :param symbol_list:
    :param interval: 参考币安文档，https://binance-docs.github.io/apidocs/futures/cn/#api，K线间隔
    :param run_time: 
    :param limit:
    :param use_thread: True：使用线程并发、False使用进程并发
    :return:
    """

    print(f'开始并行获取所有币种(x{len(symbol_list)})永续合约数据的K线数据({interval}, x{limit}), 使用线程={use_thread}')

    # 创建参数列表
    exchange_def = {
        'apiKey': exchange.apiKey,
        'secret': exchange.secret,
        'timeout': exchange.timeout,  # ms
        'rateLimit': exchange.rateLimit, # ms
        'verbose': exchange.verbose,
        'hostname': exchange.hostname,
        'enableRateLimit': exchange.enableRateLimit,
    }
    
    s_time = datetime.now()
    result = []

    if use_thread:
        # 多线程获取数据
        from multiprocessing.dummy import Pool as PoolThread

        arg_list = [(exchange_def, symbol, interval, run_time, limit) for symbol in symbol_list]

        with PoolThread() as pl:
            result = pl.starmap(_u_furture_fetch_swap_candle_data_for_multi_thread, arg_list)
    else:
        # 多进程 + 多线程获取数据 
        from multiprocessing import Pool as PoolProcess, cpu_count

        length = len(symbol_list)
        n = cpu_count()
        arg_list = []

        for i in range(n):
            one_process_list = symbol_list[math.floor(i / n * length):math.floor((i + 1) / n * length)]
            arg_list.append((exchange_def, one_process_list, interval, run_time, limit))

        with PoolProcess() as pl:
            result_lists = pl.starmap(_u_furture_fetch_swap_candle_data_for_multi_process, arg_list)
            for r in result_lists:
                for rr in r:
                    result.append(rr)

    df = dict(result)
    print('获取所有币种(x%s)K线数据(%s)完成，花费时间：%s \n' % (len(symbol_list), interval, datetime.now() - s_time))
    return df

def _u_furture_fetch_swap_candle_data_for_multi_process(exchange_def, symbol_list, interval, run_time, limit=1000):
    """
    多线程专用，重新创建exchange对象
    """
    # 多线程获取数据
    from multiprocessing.dummy import  Pool as PoolThread

    arg_list = [(exchange_def, symbol, interval, run_time, limit) for symbol in symbol_list]
    result = []

    with PoolThread() as pl:
        result = pl.starmap(_u_furture_fetch_swap_candle_data_for_multi_thread, arg_list)

    return result

def _u_furture_fetch_swap_candle_data_for_multi_thread(exchange_def, symbol, interval, run_time, limit=1000):
    """
    多线程专用，重新创建exchange对象
    """
    import ccxt
    exchange = ccxt.binance(exchange_def)
    symbol, df = u_furture_fetch_swap_candle_data(exchange, symbol, interval, run_time, limit)

    return symbol, df

def u_furture_fetch_swap_candle_data(exchange, symbol, interval, run_time, limit=1000):
    """
    通过ccxt的接口fapiPublic_get_klines，获取永续合约k线数据

    获取单个币种的1小时数据
    :param exchange:
    :param symbol:
    :param limit: 
    :param interval: 参考币安文档，https://binance-docs.github.io/apidocs/futures/cn/#api，K线间隔
    :param run_time: datetime
    :return:
    """

    # 获取数据
    # 参考文档：https://binance-docs.github.io/apidocs/futures/cn/#k
    max_num = 999
    df_list = []
    kline_left = limit
    end_time = int(run_time.timestamp()) * 1000

    while kline_left > 0:
        curr_limit = None
        if kline_left > max_num:
            curr_limit = max_num
        else:
            curr_limit = kline_left
        kline_left -= curr_limit

        params = {'symbol': symbol, 'interval': interval, 'endTime': end_time,  'limit': curr_limit}
        kline = retry_wrapper(exchange.fapiPublic_get_klines, params=params, act_name='获取U本位合约K线')

        # 将数据转换为DataFrame
        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(kline, columns=columns, dtype='float')

        # 整理数据
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=8)  # 时间转化为东八区
        df['symbol'] = symbol  # 添加symbol列
        columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
                
        df = df[columns]
        df_list.append(df)

        if df.shape[0] > 0:
            end_time = int((pd.to_datetime(df.iloc[0]['candle_begin_time'], unit='ms') - pd.Timedelta(hours=8)).timestamp()) * 1000 - 1 # 不减1会重复一条，少取
        else:
            break

    # 合并数据
    df = pd.concat(df_list)
    df.sort_values(by=['candle_begin_time'], inplace=True)
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    df = df[df['candle_begin_time'] != run_time]  # 删除runtime那行的数据，如果有的话

    if df.shape[0] > limit:
        df = df.tail(limit)
    
    return symbol, df

#====================
#   dingding utils
#====================

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
def send_dingding_msg(content, robot_id, secret):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。例如如下地址：https://oapi.dingtalk.com/robot/
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """
    
    try:
        msg = {
            "msgtype": "text",
            "text": {
                "content": content + '\n\n[❤LoveKun❤] ' + datetime.now().strftime("%m-%d %H:%M:%S")
            }
        }
        headers = {"Content-Type": "application/json;charset=utf-8"}
        # https://oapi.dingtalk.com/robot/send?access_token=XXXXXX&timestamp=XXX&sign=XXX
        timestamp, sign_str = cal_timestamp_sign(secret)
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id + '&timestamp=' + timestamp + '&sign=' + sign_str
        body = json.dumps(msg)
        requests.post(url, data=body, headers=headers, timeout=10)
        print('\n成功发送钉钉： "', content[0:min(10, len(content))].replace('\n', ' '), '" ...\n')
    except Exception as e:
        print("\n发送钉钉失败： ", e)

#====================
#    order utils
#====================

def place_order(run_time, exchange, signals, user_uf_position, user_virtual_position, min_qty, price_precision, balance_total, max_leverage = 3, is_real_place_order = False):
    """
    下单

    :param exchange: ccxt交易所对象
    :param signals: [{'symbol':'BTCUSDT', 'signal': 1, 'order_usdt': 2000.0, 'one_order_usdt': 1000.0, 'slippage': 0.02}, ...]
    :param user_uf_position: 当前持仓信息
    :param min_qty: 最小交易量信息
    :param price_precision: 下单精度信息
    :param max_leverage: 最大杠杆数
    :param is_real_place_order 是否真的提交下单
    """

    print('\n下单程序开始，待下单信号:')
    print(signals, '\n')

    place_order_min_usdt = 8    # 最小下单金额，最好比币安规定的5u多一点

    orders = []             # 进行下单的订单数组
    temp_orders = []        # 临时下单的订单数组，存放会增加仓位的订单
    failed_orders = []      # 计算过程中失败的订单数组（不下单）

    # 统计当前持仓杠杆
    curr_pos_usdt = 0  # 当前总持仓价值  

    print(user_uf_position[user_uf_position['当前持仓量'] != 0])

    for symbol, row in user_uf_position.iterrows():
        curr_pos_usdt += row['当前持仓价值']

    curr_pos_leverage = 0

    if curr_pos_usdt > 0 and balance_total > 0:
        curr_pos_leverage = round(curr_pos_usdt / balance_total, 2)

    print(f'当前持仓总价值: ${round(curr_pos_usdt)}，当前杠杆: {curr_pos_leverage}')

    for s in signals:
        symbol = s['symbol']
        signal = s['signal']
        trade_usdt = s['order_usdt']
        one_order_usdt = s['one_order_usdt']
        slippage = s['slippage']

        if signal not in [0, -1, 1]:
            print(f'{symbol} 未产生信号')
            continue
        
        order = {}
        order['symbol'] = symbol
        order['方向'] = signal
        order['每单最大金额'] = one_order_usdt

        # 如果在虚拟持仓中，则最新信号为平仓或者信号相反，则清理虚拟仓位
        if symbol in user_virtual_position.index and (signal == 0 or signal * user_virtual_position.loc[symbol]['pos'] == -1):
            clear_virtual_pos(symbol)

        if symbol in user_uf_position.index:
            order['当前持仓量'] = user_uf_position.loc[symbol]['当前持仓量']
        else:
            print(f'未获取到 {symbol} 当前持仓，跳过下单')
            continue
        
        # 如果当前持仓方向与信号一致，则跳过下单   * 这里与中性不一样，中性会更新持仓比例
        if (order['当前持仓量'] > 0 and signal == 1) or (order['当前持仓量'] < 0 and signal == -1) or (order['当前持仓量'] == 0 and signal == 0):
            print(f'{symbol} 信号({signal})与持仓方向相同，跳过下单')
            continue
        
        symbol_last_price = binance_u_furture_fetch_ticker_price(exchange, pd.DataFrame({}, index=[symbol]))
        symbol_last_price = symbol_last_price.loc[symbol]
        order['目标下单量'] = trade_usdt / symbol_last_price * order['方向'] 
        order['实际下单量'] = order['目标下单量'] - order['当前持仓量']

        if order['实际下单量'] > 0 or order['实际下单量'] < 0:
            order['最新价格'] = symbol_last_price
            order['滑点'] = slippage
            price = symbol_last_price   # 下单价格要计算滑点

            if order['实际下单量'] > 0:
                order['side'] = 'BUY'
                price = price * (1 + slippage)
            else:
                order['side'] = 'SELL'
                price = price * (1 - slippage)
            
            price = float(f'{price:.{price_precision[symbol]}f}')
            order['下单价格'] = price
            order['实际下单资金'] = order['实际下单量'] * price

            if abs(order['实际下单资金']) < place_order_min_usdt:
                order['失败原因'] = f"下单资金 < {place_order_min_usdt}U (${round(abs(order['实际下单资金']), 2)})"
                # failed_orders.append(order)
                continue
            
            # 下单后的总杠杆不能超过最大杠杆限制
            # 先把减仓的加入订单列表，并减少总仓位金额，加仓的放入临时订单列表
            # 最后检查临时订单列表，一个个添加到订单列表，直到达到仓位上限
            is_reduce_only = order['方向'] == 0 or (order['实际下单量'] * order['目标下单量'] <  0) # 是否建仓：平仓或相反方向操作

            if is_reduce_only:
                print(f"{symbol} {order['side']} ${round(abs(order['实际下单资金']))} -- 减仓，可以下单")
                curr_pos_usdt -= abs(order['实际下单资金'])
                orders.append(order)
            else:
                temp_orders.append(order)
        else:
            order['失败原因'] = '实际下单量=0，不生成订单'
            failed_orders.append(order)
            continue

    for order in temp_orders:
        symbol = order['symbol']
        if round((curr_pos_usdt + abs(order['实际下单资金'])) / balance_total, 2) > max_leverage:
            order['失败原因'] = f"下单后，将超过总杠杆{max_leverage}，取消下单"
            print(f"{symbol} {order['side']} ${round(abs(order['实际下单资金']))} : {order['失败原因']}")
            failed_orders.append(order)
            save_virtual_pos(run_time, symbol, order['方向'], order['下单价格'])   # 因最大杠杆限制取消的订单，保存到虚拟仓位
            continue
        else:
            orders.append(order)
            curr_pos_usdt += abs(order['实际下单资金'])
            print(f"{symbol} {order['side']} ${round(abs(order['实际下单资金']))} 可以下单，总杠杆 => {round(curr_pos_usdt / balance_total, 2)}/{max_leverage}")

    print('\n\n--------- orders ---------')
    if len(orders) > 0:
        order_df = pd.DataFrame(orders)
        print(order_df[['symbol', '方向', '实际下单资金']])
    else:
        print('无订单')

    batch_orders = [[]]         # 按批次组装的订单信息

    # 开始拆单（按金额）
    print('\n\n--------- 开始拆单 ---------\n')
    if len(orders) > 0:
        for o in orders:
            symbol = o['symbol']
            symbol_min_qty = min_qty[symbol]            # 这里是一个整数，代表最小下单量是10x负一的几次方，即小数点后面几位
            min_quantity = 10 ** (-1 * symbol_min_qty)  # 算出最小下单量
            single_piece = math.ceil(o['每单最大金额'] / o['下单价格'] / min_quantity)   # 拆分订单，每个分订单的份数（1份=1最小下单量）
            order_usdt = abs(o['实际下单资金'])
            order_piece = round(abs(o['实际下单量']) / min_quantity)

            # print(f'\n [{symbol}] 开始拆单，每单最大金额: ', o['每单最大金额'], '    拆分订单最大份数: ', single_piece, '   拆分前份数: ', order_piece, '    单份金额: $', single_piece * min_quantity * o['下单价格'])

            if order_piece <= single_piece:
                if order_usdt < place_order_min_usdt:
                    o['失败原因'] = f'下单资金 < 5U (${order_usdt})'
                    # failed_orders.append(o)
                    continue
                elif abs(o['实际下单量'])  < min_quantity:
                    o['失败原因'] = f'下单量 < {min_quantity} {symbol}'
                    failed_orders.append(o)
                    continue
                else:
                    o['实际下单量'] = order_piece * min_quantity

                    if o['side'] == 'SELL':
                        o['实际下单量'] = order_piece * min_quantity * -1
                    else:
                        o['实际下单量'] = order_piece * min_quantity

                    o['实际下单资金'] = o['实际下单量'] * o['下单价格']
                    batch_orders[0].append(o)
            else:
                print(f'开始拆单 {symbol}，总价值 ${round(order_usdt, 2)}，共{order_piece}份，下单最小量(min_qty={symbol_min_qty}): {min_quantity}，单订单最小份数: {single_piece}')
                i = 0
                left_piece = order_piece
                last_chip_order = None
                while left_piece > 0:
                    chip_piece = min(single_piece, left_piece)
                    left_piece -= chip_piece
                    chip_usdt = chip_piece * min_quantity * o['下单价格']
                    chip_amount = None

                    if o['side'] == 'BUY':
                        chip_amount = chip_piece * min_quantity
                    elif o['side'] == 'SELL':
                        chip_amount = chip_piece * min_quantity * -1
                    else:
                        print(f'  - {symbol} 订单side数值异常，跳过')
                        continue

                    # 当下单量（币数量）小于最小下单量，或者下单金额小于5美金时，添加到上一个小订单中（如果有）
                    if chip_usdt < place_order_min_usdt:
                        if last_chip_order:
                            last_chip_order['实际下单量'] = last_chip_order['实际下单量'] + chip_amount
                            last_chip_order['实际下单资金'] = last_chip_order['实际下单量'] * last_chip_order['下单价格']
                            print(f'  - {symbol} 剩余订单过小，添加到上一个订单，添加金额 ${round(chip_usdt, 2)}，份数 {chip_piece}')
                        else:
                            print(f'  - {symbol} 订单金额或订单量过小，跳过')
                            continue
                    else:
                        o_chip = o.copy()
                        o_chip['实际下单量'] = chip_amount
                        o_chip['实际下单资金'] = chip_usdt

                        if len(batch_orders) < (i + 1): # 创建一个新的订单批次
                            batch_orders.append([])
                        batch_orders[i].append(o_chip)
                        last_chip_order = o_chip
                        i += 1
                        print(f"  - 拆出小单 ${round(abs(o_chip['实际下单资金']), 2)}, {round(o_chip['实际下单量'], 4)} {symbol}，份数 {chip_piece}")
    else:
        print('本轮无需下单.')

    # 按批次下单
    batch_num = 1
    batch_total = len(batch_orders)
    if len(batch_orders) > 0 and len(batch_orders[0]) > 0:
        print(f'\n--- 最终分 {batch_total} 批下单 ---')
        for bo in batch_orders:
            for o in bo:
                symbol = o['symbol']
                quantity = o['实际下单量']
                quantity = float(f'{quantity:.{min_qty[symbol]}f}')
                o['批次'] = f'{batch_num}-{batch_total}'
                o['quantity'] = abs(quantity)
                o['实际下单量'] = quantity
                o['实际下单资金'] = quantity * o['下单价格']
                o['reduce_only'] = o['方向'] == 0 or (o['实际下单量'] * o['目标下单量'] <  0)   # 平仓或者相反方向操作
                o['下单参数'] = {'symbol': symbol, 'side': o['side'], 'type': 'LIMIT', 'price': o['下单价格'], 'quantity': o['quantity'], 'timeInForce': 'GTC', 'reduceOnly': o['reduce_only']}
                
                print(f"下单参数: {o['下单参数']}")

                # 下单
                place_order_start = datetime.now()
                if is_real_place_order:
                    order_rtn = binance_u_future_place_order(exchange, o['下单参数'])
                    print(f"[{o['批次']}] {symbol} 耗时：{(datetime.now() - place_order_start)}")
                    o['返回结果'] = order_rtn
                else:
                    o['返回结果'] = {'info': '已关闭下单功能'}
            print(f'\n== 批次 {batch_num} 完成 ==\n')
            batch_num += 1
            time.sleep(1)   # 每批订单等1秒

    if len(failed_orders) > 0:
        print('\n--- 最终失败订单 ---')
        for fo in failed_orders:
            print('[FAILED_ORDER] ', fo['失败原因'], '  :  ', fo)

    return batch_orders, failed_orders

VIRTUAL_POS_FILE_PATH = 'lovekun_virtual_pos.json'

def save_virtual_pos(run_time: datetime, symbol:str, pos:int, price):
    '''保存虚拟仓位'''
    df = get_all_virtual_pos()
    df = df[df['symbol'] != symbol].copy() # 删除币种原来的仓位 
    df = df.append({ 'run_time': run_time, 'symbol': symbol, 'pos': pos, 'price': price }, ignore_index=True)
    df.to_json(VIRTUAL_POS_FILE_PATH)
    print(f'保存虚拟仓位: {symbol} {pos} {run_time}')

def get_all_virtual_pos():
    '''获取所有虚拟仓位'''
    if os.path.exists(VIRTUAL_POS_FILE_PATH):
        return pd.read_json(VIRTUAL_POS_FILE_PATH)
    else:
        return pd.DataFrame(columns=['run_time', 'symbol', 'pos', 'price'])

def clear_virtual_pos(symbol:str):
    '''清除虚拟仓位'''
    df = get_all_virtual_pos()
    df = df[df['symbol'] != symbol].copy() # 删除币种原来的仓位 
    df.to_json(VIRTUAL_POS_FILE_PATH)
    print(f'清除拟仓位: {symbol}')

