"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
12 事件策略 案例：中户资金流
"""
from datetime import datetime
from multiprocessing import Pool, cpu_count
import pandas as pd
from program.Config import *
from program.Function import *

# 确认股票数据路径
stock_data_path = os.path.join(root_path, 'data/trade_data/stock/')
event_list = ['event_资金流_1', 'event_资金流_2']


def cal_money_flow_event_each_stock(code):
    print(code, )

    # 读入股票数据
    path = stock_data_path + '%s.csv' % code
    df = pd.read_csv(path, encoding='gbk', skiprows=1, parse_dates=['交易日期'])
    df = df[['股票代码', '股票名称', '交易日期', '成交额', '中户资金买入额', '收盘价', '是否跌停']]
    df.sort_values('交易日期', inplace=True)

    # 计算上市天数，并删除上市天数不足一年的股票
    df['上市至今交易天数'] = df.index + 1
    df = df[df['上市至今交易天数'] > 250]

    # 删除'中户资金买入额', '成交额'字段为空的行
    df.dropna(subset=['中户资金买入额', '成交额'], how='any', inplace=True, axis=0)
    if df.empty:
        return pd.DataFrame()
    # 计算事件
    df['中户资金买入额'] *= 10000
    df['factor'] = df['中户资金买入额'] / df['成交额']

    df['event_资金流_1'] = None
    df['event_资金流_2'] = None
    df.loc[df['factor'] > 0.4, 'event_资金流_1'] = 1
    df.loc[df['factor'] > 0.45, 'event_资金流_2'] = 1

    '''
    逻辑：最近发生跌停表示股票可能存在重大风险，需要筛除。
    代码流程：
    1. 计算跌停价格
    2. 计算最近5日是否发生跌停
    3. 将最近发生跌停的事件取消（event从1赋值为0）
    '''
    # 计算涨停价格
    df['跌停价'] = df['前收盘价'] * 0.9

    # 针对st进行修改
    df.loc[df['股票名称'].str.contains('ST'), '跌停价'] = df['前收盘价'] * 0.95
    df.loc[df['股票名称'].str.contains('S'), '跌停价'] = df['前收盘价'] * 0.95

    # 四舍五入
    df['跌停价'] = df['跌停价'].apply(lambda x: float(Decimal(x * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 100))

    # 判断是否一字涨停
    df.loc[df['最低价'] <= df['跌停价'], '一字跌停'] = 1
    df['一字跌停'].fillna(value=0, inplace=True)

    # # 判断是否开盘涨停
    df.loc[df['开盘价'] <= df['跌停价'], '开盘跌停'] = 1
    df['开盘跌停'].fillna(value=0, inplace=True)

    df.loc[(df['一字跌停'] + df['开盘跌停']) >= 1, '是否跌停'] = 1
    df.drop(['一字跌停', '开盘跌停', '跌停价'], axis=1, inplace=True)
    # 把空值补全为0
    df['是否跌停'].fillna(value=0, inplace=True)

    # 计算最近5天是否跌停
    df['近期是否跌停'] = df['是否跌停'].rolling(5).sum()
    # 合并资金流条件与跌停条件
    con = df['近期是否跌停'] >= 1
    df.loc[con, 'event_资金流_1'] = 0
    df.loc[con, 'event_资金流_2'] = 0

    # 删除没有事件的日期
    df.dropna(subset=event_list, how='all', inplace=True, axis=0)

    # 输出
    df['事件日期'] = df['交易日期']  # 直接新建一列，补rename
    df = df[['股票代码', '股票名称', '事件日期'] + event_list]

    return df


if __name__ == '__main__':

    # 测试
    cal_money_flow_event_each_stock('sz300479')

    # 标记开始时间
    start_time = datetime.now()
    # 获取所有股票代码
    stock_list = get_code_list_in_one_dir(stock_data_path)

    # 并行处理
    multiply_process = True
    if multiply_process:
        with Pool(max(cpu_count() - 2, 1)) as pool:
            df_list = pool.map(cal_money_flow_event_each_stock, sorted(stock_list))
    # 传行处理
    else:
        df_list = []
        for stock_code in stock_list:
            data = cal_money_flow_event_each_stock(stock_code)
            df_list.append(data)
    print('读入完成, 开始合并，消耗事件', datetime.now() - start_time)

    # 输出各个事件数据
    all_stock_data = pd.concat(df_list, ignore_index=True)
    for event in event_list:
        df = all_stock_data[all_stock_data[event] == 1]
        df.sort_values(['事件日期', '股票代码'], inplace=True)
        df = df[['股票代码', '股票名称', '事件日期', event]]
        df.reset_index(inplace=True, drop=True)
        print(df)
        df.to_pickle(root_path + '/data/事件策略event合集/%s.pkl' % event)

    print('运行完成:', datetime.now() - start_time)
