'''
1_整理数据.py
这个py主要完成两件事：
1.将csv数据转换为pickle，每个币种一个pickle
2.用BTC-USDT和ETH-USDT合成ETH-BTC
'''
import os
from datetime import date
from Function import *
import glob
import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置命令行输出时的列对齐功能
pd.set_option('display.unicode.east_asian_width', True)


# 从币安原始的数据中，整理出每个币种一个文件的格式
def transfer_bn_raw_data_2_pkl_data(time_interval='5m', spot_or_swap='spot'):
    """
    从币安原始的数据中，整理出每个币种一个文件的格式
    :param time_interval:
    :return:
    """

    # ===从币安的原始数据汇总，获取所有csv文件的文件路径
    path_list = glob.glob(root_path + f'/data/raw_history_k_data/binance/{spot_or_swap}*/*/*.csv')  # python自带的库，或者某文件夹中所有csv文件的路径
    # 只取需要的time_interval的csv文件
    path_list = list(filter(lambda x: time_interval in x, path_list))
    # print(path_list)
    # exit()

    # ===获取包含的所有币种的名称
    symbol_list = [os.path.splitext(os.path.basename(x))[0] for x in path_list if time_interval in x]
    # '/coin_alpha/data/backtest/binance/spot/2017-11-07/ETH-USDT_5m.csv'
    # os.path.basename(x) 后得到ETH-USDT_5m.csv，而os.path.splitext('BTC-USDT_5m.csv')[0]后得到 'BTC-USDT_5m'
    symbol_list = set(symbol_list)
    # print(symbol_list)
    # exit()

    # ===整理每个币种对应的csv文件路径，存储到dict中
    symbol_csv_data_dict = {symbol: [] for symbol in symbol_list}
    for path in path_list:
        symbol = os.path.splitext(os.path.basename(path))[0]
        if symbol in symbol_csv_data_dict:
            symbol_csv_data_dict[symbol].append(path)

    # ===分别将每个币种对应的所有csv文件一一读入，汇总到一张表内
    for symbol in symbol_csv_data_dict:
        # print(symbol)

        # 遍历数据并且合并
        df_list = []
        for path in sorted(symbol_csv_data_dict[symbol]):
            # print(path)
            df = pd.read_csv(path, encoding="GBK", parse_dates=['candle_begin_time'])
            df_list.append(df)
        data = pd.concat(df_list, ignore_index=True)

        # # 增加两列数据
        # data['symbol'] = symbol.split('_')[0].lower()  # symbol
        # data['avg_price'] = data['quote_volume'] / data['volume']  # 均价

        # 排序并重新索引
        data.sort_values(by='candle_begin_time', inplace=False)
        data.reset_index(drop=True, inplace=True)

        # 导出完整数据
        # 若目录不存在，先创建目录
        output_path = root_path + f'/data/pickle_data/{spot_or_swap}/'
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        # 删除当天数据，仅保留当天之前的数据
        data = data[data['candle_begin_time'] < pd.to_datetime(date.today())]
        # 导出数据
        data.to_pickle(f'{output_path}{symbol}.pkl')
        print(f'成功输出 {symbol}.pkl 至 {output_path}')








# 可以用BTC-USDT和ETH-USDT合成ETH-BTC
def create_trade_pair(symbol_1, symbol_2, spot_or_swap='spot'):
    df_symbol_1 = pd.read_pickle(root_path + f'/data/pickle_data/{spot_or_swap}/{symbol_1}-USDT_5m.pkl')
    df_symbol_2 = pd.read_pickle(root_path + f'/data/pickle_data/{spot_or_swap}/{symbol_2}-USDT_5m.pkl')

    # 仅保留开高收低信息，成交量信息无法合成
    column_list = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']
    df = pd.merge(df_symbol_1[column_list], df_symbol_2[column_list], how='inner', on='candle_begin_time')

    df['open'] = df['open_x'] / df['open_y']
    df['high'] = df['high_x'] / df['low_y']  # 注意是high/low
    df['low'] = df['low_x'] / df['high_y']  # 注意是low/high
    df['close'] = df['close_x'] / df['close_y']
    df['volume'] = df['volume_y'] # 这里有问题，但volume字段策略用不上
    df = df[column_list]

    df.to_pickle(root_path + f'/data/pickle_data/{spot_or_swap}/{symbol_1}-{symbol_2}_5m.pkl')
    print(f'成功输出 {symbol_1}-{symbol_2}_5m.pkl 至 {root_path}/data/pickle_data/{spot_or_swap}/')

    return df





# csv -> pkl 保存在/data/pickle_data/
transfer_bn_raw_data_2_pkl_data(time_interval='5m', spot_or_swap='spot')
transfer_bn_raw_data_2_pkl_data(time_interval='5m', spot_or_swap='swap')




# 用BTC-USDT和ETH-USDT合成ETH-BTC，保存在/data/pickle_data/
df_complex = create_trade_pair('ETH', 'BTC', spot_or_swap='spot')
df_complex = create_trade_pair('ETH', 'BTC', spot_or_swap='swap')


# 用BTC-USDT和ETH-USDT合成的ETH-BTC
# 与直接从交易所抓取的真实的交易对ETH-BTC基本一致
# 用这种方法可以合成出不同的交易对
# 即使交易所没有这个真实的交易对存在
# 我们也可以通过做多ETH，做空BTC的方式，做多ETH/BTC
# 相当于变相地增加了趋势策略的可交易币种
