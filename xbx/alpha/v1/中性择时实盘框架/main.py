"""
《邢不行-2020新版|Python数字货币量化投资课程》
无需编程基础，助教答疑服务，专属策略网站，一旦加入，永续更新。
课程详细介绍：https://quantclass.cn/crypto/class
邢不行微信: xbx9025
本程序作者: 邢不行

# 课程内容
币安u本位择时策略实盘框架
"""
import pandas as pd
import ccxt
from Function import *
from Config import *
import sys
import json
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)

# 输出日志到txt中
class Logger(object):

    def __init__(self, fileN='default.log'):
        self.terminal = sys.stdout
        self.log = open(fileN, 'a')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.flush()  # 每次写入后刷新到文件中，防止程序意外结束

    def flush(self):
        self.log.flush()

sys.stdout = Logger('log' + '.txt')  # 输出日志到txt中

import configparser
config = configparser.ConfigParser()
config.read('config.ini')

# debug
debug = int(config['default']['debug'])


# ==========配置运行相关参数==========
# =k线周期
time_interval = '5m'  # 目前支持5m，15m，30m，1h，2h等。得交易所支持的K线才行。最好不要低于5m

# =交易所配置
BINANCE_CONFIG = {
    'apiKey': config['binance_api']['apiKey'],
    'secret': config['binance_api']['secret'],
    'timeout': exchange_timeout,
    'rateLimit': 10,
    'verbose': False,
    'hostname': 'fapi.binance.com',
    'enableRateLimit': False,
    'options': {
            'adjustForTimeDifference': True,  # ←---- resolves the timestamp
            'recvWindow': 10000,
        },
}
exchange = ccxt.binance(BINANCE_CONFIG)  # 交易所api



def main():

    # 从csv文件读取策略配置信息
    symbol_config_df = pd.read_csv('symbol_config_df.csv')
    symbol_config_df['symbol_1'] = symbol_config_df['symbol'].apply(lambda x: x.split('-')[0] + 'USDT')
    symbol_config_df['symbol_2'] = symbol_config_df['symbol'].apply(lambda x: x.split('-')[1] + 'USDT')

    symbol_list = get_symbol_list(symbol_config_df)
    # {'BTCUSDT', 'LTCUSDT', 'UNIUSDT', 'ETHUSDT', 'DOGEUSDT'}

    # =判断是否单向持仓，若不是程序退出
    if_oneway_mode(exchange)

    # ==========获取需要交易币种的历史数据==========
    # 获取数据
    max_candle_num = 10  # 获取的历史K线数量，获取了最新数据后合成的k线数据最终也仅保留max_candle_num根
    symbol_candle_data = get_binance_history_candle_data(exchange, symbol_list, time_interval, max_candle_num, if_print=True)

    # =进入每次的循环
    while True:
        # ==========获取持仓数据==========
        # 初始化symbol_info，在每次循环开始时都初始化，防止上次循环的内容污染本次循环的内容。
        symbol_info_columns = ['账户权益', '持仓方向', '持仓量', '持仓收益', '持仓均价', '当前价格']
        symbol_info = pd.DataFrame(index=symbol_list, columns=symbol_info_columns)  # 转化为dataframe

        # 更新账户信息symbol_info
        symbol_info = binance_update_account(exchange, symbol_list, symbol_info)
        # 获取精度信息
        symbol_info = update_precision(exchange, symbol_info)
        print('持仓信息\n', symbol_info)

        # ==========根据当前时间，获取策略下次执行时间，例如16:15。并sleep至该时间==========
        if debug == 1:
            run_time = sleep_until_run_time(time_interval, if_sleep=False)
        else:
            run_time = sleep_until_run_time(time_interval, if_sleep=True)

        # ==========获取最新的k线数据==========
        exchange.timeout = 1000  # 即将获取最新数据，临时将timeout设置为1s，加快获取数据速度
        # 获取新数据
        recent_candle_num = 5
        recent_candle_data = single_threading_get_binance_candle_data(exchange, symbol_list, symbol_info,
                                                                      time_interval, run_time, recent_candle_num)
        # 将最近的数据打印出
        for symbol in symbol_list:
            print('\n' + '-'*20, symbol, '-'*20)
            print(recent_candle_data[symbol].tail(min(2, recent_candle_num)))

        # 将symbol_candle_data和最新获取的recent_candle_data数据合并
        # 每个币种仅保留max_candle_num根k线
        symbol_candle_data = symbol_candle_data_append_recent_candle_data(symbol_candle_data, recent_candle_data,
                                                                          symbol_list, max_candle_num)

        # 将ETHUSDT和BTCUSDT的数据转化为ETH-BTC数据
        complex_symbol_data = transfer_symbol_data(symbol_config_df, symbol_candle_data)
        # 将合并的数据打印出
        for complex_symbol in complex_symbol_data.keys():
            print('\n' + '-' * 20, complex_symbol, '-' * 20)
            print(complex_symbol_data[complex_symbol].tail(min(2, recent_candle_num)))


        # ==========计算每个币种的交易信号==========
        symbol_config_df = calculate_signal(symbol_config_df, complex_symbol_data)
        # print(symbol_config_df)

        # 合并各策略信号，计算出每个币种实际下单量
        order_info_df = cal_order_info_df(symbol_config_df, symbol_info)
        # exit()

        # ==========下单==========
        exchange.timeout = exchange_timeout  # 下单时需要增加timeout的时间，将timout恢复正常
        # 计算下单信息，根据order_info_df计算出下单参数symbol_order_params
        symbol_order_params = cal_all_order_info(order_info_df)
        # print('\n订单参数\n', symbol_order_params)

        # 开始批量下单
        all_order_info = place_binance_batch_order(exchange, symbol_order_params)

        # 检查下单情况，有无漏单
        check_palce_order(symbol_order_params, all_order_info)

        # 下单完成后，更新symbol_config_df，保存好last_pos与last_open_price数据
        symbol_config_df = update_symbol_config_df(symbol_config_df)

        # 下单结束，保存净值到本地文件，方便日后统计资金曲线
        # 注意这里保存的是下单前获取的账户权益，下单后账户权益会有微小波动，需要准确数据的话需要再调用一次binance_update_account()
        store_equity_history(symbol_info['账户权益'].iloc[0], run_time, equity_history_file)


        if debug == 1:
            print('\n当前为debug模式，直接退出程序，不执行下一个循环。')
            exit()

        # 本次循环结束
        print('\n', '-' * 40, '本次循环结束，%d秒后进入下一次循环' % long_sleep_time, '-' * 40, '\n\n')
        time.sleep(long_sleep_time)


if __name__ == '__main__':

    main()

    # while True:
    #     try:
    #         main()
    #     except Exception as e:
    #         print('系统出错，10s之后重新运行，出错原因：' + str(e))
    #         time.sleep(long_sleep_time)
