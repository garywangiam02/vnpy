"""
邢不行2020策略分享会
0705数字货币多空选币中性策略
邢不行微信：xbx9025
"""
import pandas as pd
import glob
import os
from xbx.coin_alpha.program.backtest.Function import *
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


# 从币安原始的数据中，整理出每个币种一个文件的格式
def transfer_bn_raw_data_2_pkl_data(time_interval='5m'):
    """
    从币安原始的数据中，整理出每个币种一个文件的格式
    :param time_interval:
    :return:
    """

    # ===获取项目根目录
    _ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
    root_path = os.path.abspath(os.path.join(_, '../..'))  # 返回根目录文件夹

    # ===从币安的原始数据汇总，获取所有csv文件的文件路径
    path_list = glob.glob(root_path + '/data/backtest/*/*/*.csv')  # python自带的库，或者某文件夹中所有csv文件的路径
    # 只取需要的time_interval的csv文件
    path_list = list(filter(lambda x: time_interval in x, path_list))

    # ===获取包含的所有币种的名称
    symbol_list = [os.path.splitext(os.path.basename(x))[0] for x in path_list if time_interval in x]
    # '/coin_alpha/data/backtest/binance/spot/2017-11-07/ETH-USDT_5m.csv'
    # os.path.basename(x) 后得到ETH-USDT_5m.csv，而os.path.splitext('BTC-USDT_5m.csv')[0]后得到 'BTC-USDT_5m'
    symbol_list = set(symbol_list)

    # ===整理每个币种对应的csv文件路径，存储到dict中
    symbol_csv_data_dict = {symbol: [] for symbol in symbol_list}
    for path in path_list:
        symbol = os.path.splitext(os.path.basename(path))[0]
        if symbol in symbol_csv_data_dict:
            symbol_csv_data_dict[symbol].append(path)

    # ===分别将每个币种对应的所有csv文件一一读入，汇总到一张表内
    for symbol in symbol_csv_data_dict:
        print(symbol)

        # 遍历数据并且合并
        df_list = []
        for path in sorted(symbol_csv_data_dict[symbol]):
            print(path)
            df = pd.read_csv(path, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
            df_list.append(df)
        data = pd.concat(df_list, ignore_index=True)

        # 增加两列数据
        data['symbol'] = symbol.split('_')[0].lower()  # symbol
        data['avg_price'] = data['quote_volume'] / data['volume']  # 均价

        # 排序并重新索引
        data.sort_values(by='candle_begin_time', inplace=False)
        data.reset_index(drop=True, inplace=True)

        # 导出完整数据
        data.to_pickle(root_path + '/data/backtest/output/pickle_data/%s.pkl' % symbol)


# ===整理出所有币种的5分钟数据
# transfer_bn_raw_data_2_pkl_data(time_interval='5m')

# ===整理出所有币种的1分钟数据
transfer_bn_raw_data_2_pkl_data(time_interval='1m')

# ===遍历所有币种5分钟数据，将数据转换为1小时周期
pkl_data_list = glob.glob(root_path + '/data/backtest/output/pickle_data*/*5m.pkl')
for path in pkl_data_list:
    print(path)
    # =读取数据
    df = pd.read_pickle(path)

    # =将数据转换为1小时周期
    df.set_index('candle_begin_time', inplace=True)
    df['avg_price_5m'] = df['avg_price']
    agg_dict = {
        'symbol': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
        'avg_price_5m': 'first'
    }
    df = df.resample(rule='1H').agg(agg_dict)

    # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
    df['symbol'].fillna(method='ffill', inplace=True)
    # 对开、高、收、低、价格进行补全处理
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(value=df['close'], inplace=True)
    df['high'].fillna(value=df['close'], inplace=True)
    df['low'].fillna(value=df['close'], inplace=True)
    # 将停盘时间的某些列，数据填补为0
    fill_0_list = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

    # =补全1分钟数据中的均价
    path_1m = path.replace('_5m.pkl', '_1m.pkl')
    # 文件存在
    if os.path.isfile(path_1m):
        df_1m = pd.read_pickle(path_1m)
        df_1m.set_index('candle_begin_time', inplace=True)
        df['avg_price_1m'] = df_1m['avg_price']

    # =计算最终的均价
    df['avg_price'] = df['avg_price_1m']  # 默认使用1分钟均价
    df['avg_price'].fillna(value=df['avg_price_5m'], inplace=True)  # 没有1分钟均价就使用5分钟均价
    df['avg_price'].fillna(value=df['open'], inplace=True)  # 没有5分钟均价就使用开盘价
    del df['avg_price_5m']
    del df['avg_price_1m']

    # =输出数据
    df.reset_index(inplace=True)
    df.to_pickle(path.replace('_5m.pkl', '.pkl'))
