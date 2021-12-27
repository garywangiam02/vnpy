"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
事件小组基础代码
"""
import talib
from datetime import datetime
from multiprocessing import Pool, cpu_count
import pandas as pd
from xbx.money_flow.program.Config import *
from xbx.money_flow.program.Function import *

# 确认股票数据路径
stock_data_path = os.path.join(root_path, 'data/trade_data/stock/')


def cal_money_flow_event_each_stock(code):
    print(code, )

    # 读入股票数据
    path = stock_data_path + '%s.csv' % code
    df = pd.read_csv(path, encoding='gbk', skiprows=1, parse_dates=['交易日期'])
    df = df[['股票代码', '股票名称', '交易日期', '成交额', '总市值', '收盘价', '前收盘价', '机构资金买入额', '机构资金卖出额', '大户资金买入额', '大户资金卖出额',
             '中户资金买入额', '中户资金卖出额', '散户资金买入额', '散户资金卖出额']]
    df.sort_values('交易日期', inplace=True)

    # 计算上市天数，并删除上市天数不足一年的股票
    df['上市至今交易天数'] = df.index + 1
    df = df[df['上市至今交易天数'] > 250]

    if df.empty:
        return pd.DataFrame()

    # 删除'中户资金买入额', '成交额'字段为空的行
    df.dropna(subset=['中户资金买入额', '成交额'], how='any', inplace=True, axis=0)
    df[['机构资金买入额', '机构资金卖出额', '大户资金买入额', '大户资金卖出额',
        '中户资金买入额', '中户资金卖出额', '散户资金买入额', '散户资金卖出额']] *= 10000
    # 事件生成
    df['散户资金卖出占比'] = df['散户资金卖出额'] / df['成交额']  # 散户资金卖出额占比
    ratio_s = df['散户资金卖出占比'] > 0.2
    df['中户资金买入占比'] = df['中户资金买入额'] / df['成交额']  # 中户资金买入额占比
    ratio_m = df['中户资金买入占比'] > 0.5
    ratio_l = df['大户资金买入额'] > 0

    # 筛选事件  修改因子
    df['选股因子'] = None
    df.loc[ratio_s & ratio_m & ratio_l, '选股因子'] = 1

    # 删除没有事件的日期
    df.dropna(subset=['选股因子'], how='all', inplace=True, axis=0)

    # 输出
    df['事件日期'] = df['交易日期']  # 直接新建一列，补rename
    df = df[['股票代码', '股票名称', '事件日期', '选股因子']]

    return df


if __name__ == '__main__':

    # 测试
    cal_money_flow_event_each_stock('sz300479')

    # 标记开始时间
    start_time = datetime.now()
    # 获取所有股票代码
    stock_list = get_code_list_in_one_dir(stock_data_path)

    # 并行处理
    multiply_process = False
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
    event_list = ['选股因子']
    for event in event_list:
        df = all_stock_data[all_stock_data[event] == 1]
        df.sort_values(['事件日期', '股票代码'], inplace=True)
        df = df[['股票代码', '股票名称', '事件日期', event]]
        df.reset_index(inplace=True, drop=True)
        print(df)
        df.to_pickle(root_path + '/data/事件策略event合集/%s.pkl' % event)

    print('运行完成:', datetime.now() - start_time)
