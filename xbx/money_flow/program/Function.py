"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
12 事件策略 案例：中户资金流
"""
import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
from decimal import Decimal, ROUND_HALF_UP
from tqdm import tqdm

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


# region 文件交互
def get_code_list_in_one_dir(path, end_with='csv'):
    """
    从指定文件夹下，导入所有csv文件的文件名
    :param path:
    :param end_with:
    :return:
    """
    stock_list = []

    # 系统自带函数os.walk，用于遍历文件夹中的所有文件
    for root, dirs, files in os.walk(path):
        if files:  # 当files不为空的时候
            for f in files:
                if f.endswith('.' + end_with):
                    index = f.find('.' + end_with)
                    stock_list.append(f[:index])

    return sorted(stock_list)


# 导入指数
def import_index_data(path, back_trader_start=None, back_trader_end=None):
    """
    从指定位置读入指数数据。指数数据来自于：program/构建自己的股票数据库/案例_获取股票最近日K线数据.py
    :param back_trader_end: 回测结束时间
    :param back_trader_start: 回测开始时间
    :param path:
    :return:
    """
    # 导入指数数据
    df_index = pd.read_csv(path, parse_dates=['candle_end_time'])
    df_index['指数涨跌幅'] = df_index['close'].pct_change()
    df_index = df_index[['candle_end_time', '指数涨跌幅']]
    df_index.dropna(subset=['指数涨跌幅'], inplace=True)
    df_index.rename(columns={'candle_end_time': '交易日期'}, inplace=True)

    if back_trader_start:
        df_index = df_index[df_index['交易日期'] >= pd.to_datetime(back_trader_start)]
    if back_trader_end:
        df_index = df_index[df_index['交易日期'] <= pd.to_datetime(back_trader_end)]

    df_index.sort_values(by=['交易日期'], inplace=True)
    df_index.reset_index(inplace=True, drop=True)

    return df_index


# endregion

# region 股票数据处理

def cal_fuquan_price(df, fuquan_type='前复权'):
    """
    用于计算复权价格
    :param df: 必须包含的字段：收盘价，前收盘价，开盘价，最高价，最低价
    :param fuquan_type: ‘前复权’或者‘后复权’
    :return: 最终输出的df中，新增字段：收盘价_复权，开盘价_复权，最高价_复权，最低价_复权
    """

    # 计算复权因子
    df['复权因子'] = (df['收盘价'] / df['前收盘价']).cumprod()

    # 计算前复权、后复权收盘价
    if fuquan_type == '后复权':
        df['收盘价_复权'] = df['复权因子'] * (df.iloc[0]['收盘价'] / df.iloc[0]['复权因子'])
    elif fuquan_type == '前复权':
        df['收盘价_复权'] = df['复权因子'] * (df.iloc[-1]['收盘价'] / df.iloc[-1]['复权因子'])
    else:
        raise ValueError('计算复权价时，出现未知的复权类型：%s' % fuquan_type)

    # 计算复权
    df['开盘价_复权'] = df['开盘价'] / df['收盘价'] * df['收盘价_复权']
    df['最高价_复权'] = df['最高价'] / df['收盘价'] * df['收盘价_复权']
    df['最低价_复权'] = df['最低价'] / df['收盘价'] * df['收盘价_复权']
    del df['复权因子']
    return df


# 将股票数据和指数数据合并
def merge_with_index_data(df, index_data):
    """
    原始股票数据在不交易的时候没有数据。
    将原始股票数据和指数数据合并，可以补全原始股票数据没有交易的日期。
    :param df: 股票数据
    :param index_data: 指数数据
    :return:
    """

    # ===将股票数据和上证指数合并，结果已经排序
    df = pd.merge(left=df, right=index_data, on='交易日期', how='right', sort=True,
                  indicator=True)

    # ===对开、高、收、低、前收盘价价格进行补全处理
    # 用前一天的收盘价，补全收盘价的空值
    df['收盘价'].fillna(method='ffill', inplace=True)
    # 用收盘价补全开盘价、最高价、最低价的空值
    df['开盘价'].fillna(value=df['收盘价'], inplace=True)
    df['最高价'].fillna(value=df['收盘价'], inplace=True)
    df['最低价'].fillna(value=df['收盘价'], inplace=True)
    # 补全前收盘价
    df['前收盘价'].fillna(value=df['收盘价'].shift(), inplace=True)

    # ===将停盘时间的某些列，数据填补为0
    fill_0_list = ['成交量', '成交额', '涨跌幅', '开盘买入涨跌幅']
    df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

    # ===用前一天的数据，补全其余空值
    df.fillna(method='ffill', inplace=True)

    # ===去除上市之前的数据
    df = df[df['股票代码'].notnull()]

    # ===判断计算当天是否交易
    df['是否交易'] = 1
    df.loc[df['_merge'] == 'right_only', '是否交易'] = 0
    del df['_merge']

    df.reset_index(drop=True, inplace=True)

    return df


# 计算是涨停
def cal_if_zhangting_with_st(df):
    """
    计算股票当天的涨跌停价格。在计算涨跌停价格的时候，按照严格的四舍五入。
    包含st股，但是不包含新股

    :param df: 必须得是日线数据。必须包含的字段：前收盘价，开盘价，最高价，最低价
    :return:
    """

    # 计算涨停价格
    df['涨停价'] = df['前收盘价'] * 1.1

    # 针对st进行修改
    df.loc[df['股票名称'].str.contains('ST'), '涨停价'] = df['前收盘价'] * 1.05
    df.loc[df['股票名称'].str.contains('S'), '涨停价'] = df['前收盘价'] * 1.05

    # 四舍五入
    df['涨停价'] = df['涨停价'].apply(lambda x: float(Decimal(x * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 100))

    # 判断是否一字涨停
    df['一字涨停'] = False
    df.loc[df['最低价'] >= df['涨停价'], '一字涨停'] = True

    # 判断是否开盘涨停
    df['开盘涨停'] = False
    df.loc[df['开盘价'] >= df['涨停价'], '开盘涨停'] = True

    return df


def read_event_data(path, event_list):
    """
    从文件中读取event数据，目前只支持pkl文件
    :param path:
    :param event_list:
    :return:
    """
    # 遍历读取所有的event文件
    temp = []
    for event in event_list:
        event = pd.read_pickle(path + '/' + event + '.pkl')
        temp.append(event)
    # 纵向合并多个event文件
    event_df = pd.concat(temp, ignore_index=True)

    # 某个股票在某日可能触发多个事件，将其合并到一行
    event_df = event_df.groupby(['事件日期', '股票代码'])[event_list].sum()
    event_df.sort_values(by=['事件日期', '股票代码'], inplace=True)
    event_df.reset_index(inplace=True)

    return event_df


# endregion


def cal_today_stock_cap_line(data, hold_period):
    """
    当天买入若干支股票，计算买入这些股票之后的资金曲线
    :param data:
    :param hold_period:
    :return:
    """
    # 将涨跌幅数据转换成numpy的array格式
    array = np.array(data.tolist())
    # 获取持仓天数
    future_days = len(array[0])
    hold_period = min(hold_period, future_days)
    # 截取涨跌幅数据
    array = array[:, :hold_period]  # 行全选，列只选取前hold_period列
    # 计算每个股票的资金曲线
    array = array + 1
    array = np.cumprod(array, axis=1)
    # 计算整体资金曲线
    array = array.mean(axis=0)

    return list(array)


def update_df(today_event, start_index, cap_num, cap):
    """
    :param today_event:  当天的事件数据
    :param start_index:  开始的index，为update使用
    :param cap_num:  使用哪一份资金进行投资
    :param cap:  投资金额是多少
    :return:
    """

    # 获取持仓每日净值
    value_list = today_event.iloc[0]['持仓每日净值']

    # 创建用来更新的df
    index = range(start_index, start_index + len(value_list))
    df = pd.DataFrame(index=index)

    # 给更新的df赋值
    df['%s_资金' % cap_num] = value_list
    df['%s_资金' % cap_num] *= cap

    df['%s_股票' % cap_num] = today_event.iloc[0]['买入股票代码']
    df['%s_余数' % cap_num] = range(len(value_list), 0, -1)

    return df
