import time
import os
import pymysql
import numpy as np
from utility import *
import pandas as pd
from config import *
from datetime import datetime, timedelta
from icecream import ic

def exchange_config(api,secret):
    BINANCE_CONFIG = {
        'apiKey': api,
        'secret': secret,
        'timeout': 30000,
        'rateLimit': 10,
        'hostname': 'binancezh.com',  # 无法fq的时候启用
        'enableRateLimit': False,
        'options': {
            'adjustForTimeDifference': True,  # ←---- resolves the timestamp
            'recvWindow': 10000,
        },
    }
    return BINANCE_CONFIG




def table_create_acc(table_name):
    try:
        with conn.cursor() as cursor:
            create_table_sql = f"""CREATE TABLE {table_name} (

                                 `id` int(0) NOT NULL AUTO_INCREMENT,
                                  `time` datetime NOT NULL,
                                  `account` varchar(255) NULL,
                                  `capital` float NULL,
                                  `balance` float NULL,
                                  `unRealizedProfit` float NULL,                                
                                  `pnl` float NULL,
                                  `pnl_p` float NULL,
                                  `pnl_today` float NULL,
                                  `pnl_today_p` float NULL,
                                  `ytd_equity` float NULL,
                                  `annual_return` float NULL,
                                  `init_date` datetime NOT NULL,
                                  `running_days` int(255) NULL,
                                  `deposit` float NULL,
                                  `deposit_sum` float NULL,
                                  `spread` float NULL,
                                  `transfer_id` varchar(255) NULL,
                                  `balance_restoration` float NULL,
                                  `equity_change` float NULL,
                                  `equity` float NULL,
                                  `strategy_name` varchar(255) NULL,
                                  PRIMARY KEY (`id`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4
                                        """
            cursor.execute(create_table_sql)
    except pymysql.err.InternalError as err:
        ic(f'创建数据库失败{err}')


def table_create_trading(table_name):
    try:
        with conn.cursor() as cursor:
            create_table_sql = f"""CREATE TABLE {table_name} (

                                 `id` int(0) NOT NULL AUTO_INCREMENT,
                                  `time` datetime NOT NULL,
                                  `account` varchar(255) NULL,
                                  `symbol` varchar(255) NULL,
                                  `notional` float NULL,
                                  `positionAmt` float NULL,
                                  `direction` int(255) NULL,
                                  `unRealizedProfit` float NULL,
                                  `unRealizedProfit_p` float NULL,
                                  `pnl` float NULL,
                                  `entryprice` float NULL,
                                  `margin` float NULL,
                                  `leverage` int(255) NULL,
                                  `liquidationPrice` float NULL,
                                  `markprice` float NULL,
                                  `strategy_name` varchar(255) NULL,
                                  PRIMARY KEY (`id`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4
                                        """
            cursor.execute(create_table_sql)
    except pymysql.err.InternalError as err:
        ic(f'创建数据库失败{err}')


def table_create_overview(table_name):
    try:
        with conn.cursor() as cursor:
            create_table_sql = f"""CREATE TABLE {table_name} (

                                 `id` int(0) NOT NULL AUTO_INCREMENT,
                                  `time` datetime NOT NULL,
                                  `capital` float NULL,
                                  `balance` float NULL,
                                  `unRealizedProfit` float NULL,                                
                                  `pnl` float NULL,
                                  `pnl_p` float NULL,
                                  `pnl_today` float NULL,
                                  `pnl_today_p` float NULL,
                                  `ytd_equity` float NULL,
                                  `annual_return` float NULL,
                                  `running_days` int(255) NULL,
                                  `deposit` float NULL,
                                  `deposit_sum` float NULL,
                                  `spread` float NULL,
                                  `balance_restoration` float NULL,
                                  `equity_change` float NULL,
                                  `equity` float NULL,
                                  `max_draw_down` float NULL,
                                  PRIMARY KEY (`id`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4
                                        """
            cursor.execute(create_table_sql)
    except pymysql.err.InternalError as err:
        ic(f'创建数据库失败{err}')

def table_create_history_trade(table_name):
    try:
        with conn.cursor() as cursor:
            create_table_sql = f"""CREATE TABLE {table_name} (

                                 `id` int(0) NOT NULL AUTO_INCREMENT,
                                  `time` datetime NOT NULL,
                                  `account` varchar(255) NULL,
                                  `strategy_name` varchar(255) NULL,
                                  `symbol` varchar(255) NULL,
                                  `orderId` varchar(255) NULL,
                                  `side` varchar(255) NULL,
                                  `price` float NULL,
                                  `qty` float NULL,
                                  `realizedPnl` float NULL,
                                  `marginAsset` varchar(255) NULL,
                                  `quoteQty` float NULL,
                                  `commission` float NULL,
                                  `commissionAsset` varchar(255) NULL,
                                  `positionSide` varchar(255) NULL,
                                  `buyer` float NULL,
                                  `maker` float NULL,
                                  PRIMARY KEY (`id`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4
                                        """
            cursor.execute(create_table_sql)
    except pymysql.err.InternalError as err:
        ic(f'创建数据库失败{err}')

def table_create_trade_analysis(table_name):
    try:
        with conn.cursor() as cursor:
            create_table_sql = f"""CREATE TABLE {table_name} (

                                 `id` int(0) NOT NULL AUTO_INCREMENT,
                                  `time` datetime NOT NULL,
                                  `account` varchar(255) NULL,
                                  `strategy_name` varchar(255) NULL,
                                  `trade_num` int(255) NULL,
                                  `max_profit` float NULL, 
                                  `max_loss` float NULL,
                                  `profit_loss_all` float NULL,
                                  `win_loss_ratio` float NULL,
                                  `long_loss_num` int(255) NULL,
                                  `short_loss_num` int(255) NULL,
                                  `long_win_num` int(255) NULL,
                                  `short_win_num` int(255) NULL,
                                  `win_rate` float NULL,
                                  `long_win_rate` float NULL,
                                  `short_win_rate` float NULL,
                                  `long_profit` float NULL,
                                  `long_loss` float NULL,
                                  `short_profit` float NULL,
                                  `short_loss` float NULL,
                                  PRIMARY KEY (`id`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4
                                        """
            cursor.execute(create_table_sql)
    except pymysql.err.InternalError as err:
        ic(f'创建数据库失败{err}')

def get_data_from_mysql(table_name,account=None, limit=10):
    cursor = conn.cursor()    # 创建cursor
    if account and limit:
        sql = f"SELECT * FROM {table_name} where account='{account}' order by time desc limit {limit};"
    elif account and limit is None:
        sql = f"SELECT * FROM {table_name} where account='{account}';"
    elif account is None and limit:
        sql = f"SELECT * FROM {table_name} order by time desc limit {limit};"
    else:
        sql = f"SELECT * FROM {table_name};"
    cursor.execute(sql)
    col = cursor.description    # 获取数据库列表信息
    re = cursor.fetchall()    # 获取全部查询信息
    columns = pd.DataFrame(list(col)) # 获取的信息默认为tuple类型，将columns转换成DataFrame类型
    df = pd.DataFrame(list(re), columns=columns[0])    # 将数据转换成DataFrame类型，并匹配columns
    df.sort_values(by='time', ascending=True, inplace=True)
    return df


def table_judge(db_name,table_name,table_type):
    """

    :param db_name:
    :param table_name:
    :param table_type: 1. 账户信息 2.持仓信息 3.全局信息
    :return:
    """
    try:
        with conn.cursor() as cursor:
            sql = 'show tables;'
            cursor.execute(sql)
            tables = cursor.fetchall()
            table_lib = [table[f'Tables_in_{db_name}'] for table in tables]
            if table_name not in table_lib:
                if table_type == 1:
                    table_create_acc(table_name)
                elif table_type == 2:
                    table_create_trading(table_name)
                elif table_type == 3:
                    table_create_overview(table_name)
                elif table_type == 4:
                    table_create_history_trade(table_name)
                elif table_type == 5:
                    table_create_trade_analysis(table_name)
    except pymysql.err.InternalError as err:
        ic(f'判断数据库是否存在失败{err}')


# 计算账户平均滑点，返回值为万分之x
def cal_spread_acc(exchange):
    spread = robust(exchange.fapiPrivate_get_allorders)
    orders_df = pd.DataFrame(spread)
    if orders_df.empty:
        spread_mean = 0
    else:
        orders_df = orders_df[orders_df['cumQuote'].astype(float) > 0]
        orders_df = orders_df[orders_df['type'] == 'LIMIT']
        for col in ['price', 'avgPrice']:
            orders_df[col] = pd.to_numeric(orders_df[col])

        orders_df['signalPrice'] = orders_df['price'] / np.where(orders_df['side'] == 'SELL', 0.98, 1.02)
        orders_df['spread'] = orders_df['avgPrice'] / orders_df['signalPrice'] - 1
        orders_df['spread'] = orders_df['spread'] * np.where(orders_df['side'] == 'BUY', -1, 1)  # 可自行修改，用BUY则是正滑点为佳，用SELL则是负滑点为佳
        spread_mean = round(orders_df['spread'].mean() * 10000, 6)

    return spread_mean


# 获取future账户资金进出信息
def get_binance_future_transfer_history(exchange, mins, tz_server_offset):
    # mins = 24*20*60 # for test
    s_time = (datetime.now() - tz_server_offset - timedelta(minutes=mins)).strftime('%Y-%m-%d %H:%M:%S')
    start_time_since = exchange.parse8601(s_time)
    timestamp = int(round(time.time() * 1000))
    params = {
        "asset": 'USDT',
        "startTime": start_time_since,
        "timestamp": timestamp
    }
    account_info = exchange.sapi_get_futures_transfer(params=params)

    if account_info['total'] == 0 or account_info['total'] == '0' or float(account_info['rows'][-1]['amount']) <= bnb_filter or account_info['rows'][-1]['status'] != 'CONFIRMED':
        deposit = 0
        tranId = '0'
    else:
        df = pd.DataFrame(account_info['rows'])
        # df['direction'] = df['type'].apply(lambda x: 1 if x == '1' else(-1 if x == '2' else 0)) # 入金为1，出金为-1
        df['direction'] = df['type'].apply(
            lambda x: 1 if (x == 1 or x == '1') else (-1 if (x == 2 or x == '2') else 0))  # 入金为1，出金为-1
        df['deposit'] = df['direction'] * df['amount'].astype('float')
        deposit = df['deposit'].sum()
        if abs(deposit) < bnb_filter:
            deposit = 0
        # deposit = df.iloc[-1]['deposit'] # 出入金额
        tranId = str(df.iloc[-1]['tranId'])
    return deposit,tranId


# 计算 acc_table
def get_acc_info(exchange, table_acc, account, run_time_utc, tz_server_offset, tz_local_offset, strategy_name):

    df_overview_exist = True
    df_acc_exist = True
    # 检查overview table是否存在
    try:
        get_data_from_mysql('overview', account, 5)
    except:
        df_overview_exist = False
    # 检查该acc table是否存在
    try:
        get_data_from_mysql(table_acc, account, 5)
    except:
        df_acc_exist = False
    # 创建新账户时，overview_table会出现偏差。这里创建new_acc_capital变量，以便计算overview table的复权偏差
    new_acc_capital = 0

    # === 检查table是否存在，如不存在则创建
    table_judge(db_name, table_acc, 1)  # 检查账table_acc是否存在

    # 获取账户信息
    account_info = robust(exchange.fapiPrivateGetAccount)
    # local runtime
    run_time_local = run_time_utc + tz_local_offset
    # 常见指标
    df_acc = pd.DataFrame()
    df_acc['time'] = [run_time_utc]
    df_acc['account'] = [account]
    df_acc['strategy_name'] = [strategy_name]
    df_acc['balance'] = [round(float(account_info['totalMarginBalance']), 3)]
    df_acc['capital'] = df_acc['balance']   # 若数据库为空，则本金等于余额  余额
    df_acc['unRealizedProfit'] = [round(float(account_info['totalUnrealizedProfit']), 3)] # 未实现的盈利

    # 获取mysql中table_acc 最近200条数据。200条数据时按照15分钟抓取一次数据的频率计算的。如果频率更高的话，需要调高这里的数字。
    df_sql = get_data_from_mysql(table_acc, account, 200)
    # 获取昨日df，用于计算今日指标
    ytd_filter = (run_time_utc + tz_local_offset).replace(hour=0, minute=0, second=0, microsecond=0) - tz_local_offset
    ytd_df = df_sql[df_sql['time'] < ytd_filter]
    # === 根据转账记录计算复权资金(balance_restoration)和涨跌幅(price_change)
    # 初始数值
    # df_acc['id'] = 0
    df_acc['deposit'] = 0 # 划转金额
    df_acc['deposit_sum'] = 0  # 划转总数
    df_acc['transfer_id'] = 0  # 划转ID
    df_acc['balance_restoration'] = df_acc['balance']
    df_acc['equity_change'] = 1
    df_acc['equity'] = 1
    df_acc['init_date'] = run_time_utc

    if df_acc_exist == False and df_overview_exist == True:  # 如果该table不存在，且overview table存在。
        new_acc_capital = df_acc.iloc[-1]['capital']

    if not df_sql.empty:
        # df_acc['id'] = df_sql.iloc[-1]['id'] + 1
        df_acc['capital'] = df_sql.iloc[-1]['capital']  # 数据库不为空时，修正本金数额  本金

        # 如果deposit数值不为空，且transfer_id 不同,则获取deposit和transfer_id的值
        mins = int(time_interval.strip('m')) + 1
        transfer = get_binance_future_transfer_history(exchange, mins, tz_server_offset)

        if transfer[0] != 0 and transfer[1] != df_sql.iloc[-1]['transfer_id']:
            df_acc['deposit'] = transfer[0]
            df_acc['transfer_id'] = transfer[1]

        # 截取上周期的转账记录
        if run_time_utc.strftime('%M') == '00':
            hour_ago = run_time_utc - timedelta(hours=1)
            s_time = pd.to_datetime(hour_ago.strftime('%Y-%m-%d %H:00:00'))
        else:
            s_time = run_time_utc.strftime('%Y-%m-%d %H:00:00')

        # if int(run_time_utc.strftime('%M')) == int(time_interval.strip('m')):  # 第一个周期
        #     period_deposit = 0
        # else:
        period_deposit = df_sql[df_sql['time'] > s_time]['deposit'].sum()
        df_acc['balance_restoration'] = df_acc['balance'] - period_deposit - df_acc['deposit'] # 记录实际参与策略的资金
        df_acc['deposit_sum'] = df_sql.iloc[-1]['deposit_sum'] + df_acc['deposit']

        # 计算涨跌幅，复权资金，资金曲线，年化收益
        # if int(run_time_utc.strftime('%M')) == int(time_interval.strip('m')):  # 第一个周期
        #     if df_sql.iloc[-1]['balance'] == 0:
        #
        #     df_acc['equity_change'] = df_acc['balance_restoration'] / df_sql.iloc[-1]['balance']
        # else:
        if df_sql.iloc[-1]['balance_restoration'] == 0:
            df_acc['equity_change'] = 1
        else:
            df_acc['equity_change'] = df_acc['balance_restoration'] / df_sql.iloc[-1]['balance_restoration']
        
        df_acc['init_date'] = df_sql.iloc[-1]['init_date']
        if df_sql.iloc[-1]['equity'] is None or pd.isnull(df_sql.iloc[-1]['equity']):
            df_acc['equity'] = df_acc['equity_change'] * 1
        else:
            df_acc['equity'] = df_sql.iloc[-1]['equity'] * df_acc['equity_change']

    # 计算其他常见指标
    df_acc['pnl'] = df_acc['balance'] - df_acc['capital'] - df_acc['deposit_sum']
    df_acc['pnl_p'] = (df_acc['equity'] - 1) * 100.00

    df_acc['running_days'] = (df_acc.iloc[-1]['time'] - df_acc.iloc[-1]['init_date']).days
    df_acc['running_days'] = df_acc['running_days'].apply(lambda x:1 if x <= 0 else x)  # 修正为0的情况
    
    df_acc['annual_return'] = round((df_acc['equity'] - 1) / df_acc['running_days'] * 365 * 100.00, 2) # 年化收益率
    df_acc['spread'] = cal_spread_acc(exchange)

    # 计算今日收益指标
    if ytd_df.empty:
        ic(f'===> {table_acc} need more data, skip calculate ytd_equity <===')
        df_acc['ytd_equity'] = 0
        df_acc['pnl_today'] = df_acc['pnl']
        df_acc['pnl_today_p'] = df_acc['pnl_p']
    else:
        try:
            td_deposit = df_sql[df_sql['time'] >= ytd_filter]['deposit'].sum()  # 获取数据库里从今日0点到目前出入金总额
        except:
            td_deposit = 0
        df_acc['ytd_equity'] = ytd_df.iloc[-1]['balance']
        if df_acc['ytd_equity'].iloc[-1] == 0:
            df_acc['pnl_today'] = df_acc['pnl']
            df_acc['pnl_today_p'] = df_acc['pnl_p']
        else:
            df_acc['pnl_today'] = df_acc['balance'] - td_deposit - df_acc['deposit'] - df_acc['ytd_equity']
            df_acc['pnl_today_p'] = df_acc['pnl_today'] / df_acc['ytd_equity'] * 100.00

    # 整理df_acc
    df_acc = df_acc[
        ['time', 'account', 'capital', 'balance', 'unRealizedProfit', 'pnl', 'pnl_p', 'pnl_today', 'pnl_today_p',
         'ytd_equity','init_date','running_days', 'annual_return','deposit', 'deposit_sum','spread','transfer_id', 'balance_restoration',
         'equity_change','equity', 'strategy_name']]

    return df_acc, new_acc_capital


# 计算 trading_table
def get_trading_info(exchange,table_trading,account,strategy_name):

    table_judge(db_name, table_trading, 2)  # 检查table_trading是否存在
    # 获取原始数据
    position_risk = robust(exchange.fapiPrivate_get_positionrisk)

    # 将原始数据转化为dataframe
    position_risk = pd.DataFrame(position_risk, dtype='float')

    # 整理数据
    position_risk = position_risk[position_risk['positionAmt'] != 0]  # 只保留有仓位的币种
    position_risk.reset_index(inplace=True)
    position_risk.drop(
        columns=['index', 'maxNotionalValue', 'marginType', 'isolatedMargin', 'isAutoAddMargin', 'positionSide',
                 'isolatedWallet'], inplace=True)

    position_risk['account'] = account # 账户
    position_risk['strategy_name'] = strategy_name # 策略名称
    # 判断多空方向
    position_risk['direction'] = -1 # 仓位方向
    position_risk.loc[position_risk['positionAmt'] > 0, 'direction'] = 1
    position_risk['notional'] = abs(position_risk['notional'])  # 新增 消耗的USDT
    position_risk['positionAmt'] = abs(position_risk['positionAmt'])  # 新增 仓位
    position_risk['unRealizedProfit_p'] = round(
        position_risk['unRealizedProfit'] / abs(position_risk['entryPrice'] * position_risk['positionAmt']) * 100, 2)  # 收益率

    position_risk['margin'] = abs(position_risk['entryPrice'] * position_risk['positionAmt'] / 20)  # 保证金
    position_risk['time'] = datetime.now()

    # 重新排序
    position_risk = position_risk[
        ['account', 'symbol', 'unRealizedProfit', 'unRealizedProfit_p', 'direction', 'notional',
         'positionAmt', 'entryPrice', 'markPrice', 'liquidationPrice', 'leverage', 'margin', 'strategy_name', 'time']]
    return position_risk


# 计算 overview_table
def get_overview_info(df_all,run_time_utc,new_acc_capital):
    # 检查overview_table是否存在
    table_judge(db_name, 'overview', 3)  # 检查overview table 是否存在
    df_sum = pd.DataFrame()

    df_sum['time'] = [run_time_utc]
    df_sum['capital'] = [round(df_all['capital'].sum(), 4)]
    df_sum['balance'] = [round(df_all['balance'].sum(), 4)]
    df_sum['unRealizedProfit'] = [round(df_all['unRealizedProfit'].sum(), 4)]
    df_sum['pnl'] = [round(df_all['pnl'].sum(), 4)]
    df_sum['pnl_today'] = [round(df_all['pnl_today'].sum(), 4)]
    df_sum['ytd_equity'] = [round(df_all['ytd_equity'].sum(), 4)]
    df_sum['running_days'] = [df_all['running_days'].max()]
    df_sum['deposit'] = [df_all['deposit'].sum()]
    df_sum['balance_restoration'] = df_all['balance_restoration'].sum()
    df_sum['deposit_sum'] = [df_all['deposit_sum'].sum()]
    df_sum['spread'] = [df_all['spread'].mean()]

    # === 根据转账记录计算涨跌幅，资金曲线 ，年化收益
    df_sql_overview = get_data_from_mysql(table_name='overview',limit=None)
    ic(df_sql_overview)

    # 初始数值
    # df_sum['id'] = 0
    df_sum['equity_change'] = 1
    df_sum['equity'] = 1
    df_sum['annual_return'] = 0

    if not df_sql_overview.empty:
        # df_sum['id'] = [df_sql_overview.iloc[-1]['id'] + 1]
        # ic(df_sum)
        if int(run_time_utc.strftime('%M')) == int(time_interval.strip('m')):  # 第一个周期
            df_sum['equity_change'] = (df_sum['balance_restoration'] - new_acc_capital)/ df_sql_overview.iloc[-1]['balance']
        else:
            df_sum['equity_change'] = (df_sum['balance_restoration'] - new_acc_capital) / df_sql_overview.iloc[-1]['balance_restoration']
        df_sum['equity'] = df_sql_overview.iloc[-1]['equity'] * df_sum['equity_change']
    df_sum['annual_return'] = round((df_sum['equity'] - 1) / df_sum['running_days'] * 365 * 100.00, 2)
    df_sum['pnl_p'] = (df_sum['equity'] - 1) * 100.00
    # 计算今日收益指标
    if df_sum.iloc[-1]['ytd_equity'] == 0:
        ic(f'===> table_overview need more data, skip calculate ytd_equity <===')
        df_sum['pnl_today_p'] = [df_sum['pnl_p'].sum()]
    else:
        df_sum['pnl_today_p'] = [round(df_sum['pnl_today'].sum() / df_sum['ytd_equity'].sum() * 100.00, 2)]
    if df_sql_overview.empty:
        max_draw_down = 0
    else:
        # 计算最大回撤
        df_sql_overview['max2here'] = df_sql_overview['equity'].expanding().max()
        df_sql_overview['dd2here'] = df_sql_overview['equity'] / df_sql_overview['max2here'] - 1
        end_date, max_draw_down = tuple(df_sql_overview.sort_values(by=['dd2here']).iloc[0][['time', 'dd2here']])
    df_sum['max_draw_down'] = max_draw_down # 最大回撤
    # 整理df_sum
    df_sum = df_sum[['time', 'capital', 'balance', 'unRealizedProfit', 'pnl', 'pnl_p', 'pnl_today',
                     'pnl_today_p', 'ytd_equity','running_days', 'annual_return',
                     'deposit', 'deposit_sum','spread','balance_restoration', 'equity_change', 'equity', 'max_draw_down']]

    return df_sum


def get_history_trade(exchange, table, account, strategy_name):
    """
    获取账户的成交信息进行分析
    """
    try:
        table_judge(db_name, table, 4) # 检查历史成交信息表是否存在

        # 获取数据
        history_trade = robust(exchange.fapiPrivateGetUserTrades)

        history_trade_df = pd.DataFrame(history_trade, dtype='float')

        history_trade_df['account'] = account
        history_trade_df['strategy_name'] = strategy_name
        history_trade_df['time'] = pd.to_datetime(history_trade_df['time'], unit='ms')
        history_trade_df['time'] = history_trade_df['time'].astype(str)
        history_trade_df = history_trade_df[history_trade_df['realizedPnl'] != 0] # 过滤未盈亏的

        keys = ', '.join(list(history_trade_df.columns))
        values = ', '.join(['%s'] * len(history_trade_df.columns))
        # 将成交信息存入数据库中
        # for index, row in history_trade_df.iterrows():
        cursor = conn.cursor()
        try:
            # 记录在表中不存在则进行插入，如果存在则进行更新
            sql = f"INSERT INTO {table} ({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE symbol=VALUES(symbol)," \
                  f"id=VALUES(id), orderId=VALUES(orderId), side=VALUES(side), price=VALUES(price), qty=VALUES(qty)," \
                  f"realizedPnl=VALUES(realizedPnl), marginAsset=VALUES(marginAsset), quoteQty=VALUES(quoteQty), commission=VALUES(commission)," \
                  f"commissionAsset=VALUES(commissionAsset),time=VALUES(time), positionSide=VALUES(positionSide),buyer=VALUES(buyer),maker=VALUES(maker)," \
                  f"account=VALUES(account), strategy_name=VALUES(strategy_name);"

            # 批量插入使用executement
            cursor.executemany(sql, history_trade_df.values.tolist())
            conn.commit()
        except Exception as e:
            ic(e)
            # 错误回滚
            conn.rollback()

        return history_trade_df
    except Exception as e:
        msg = '获取历史记录失败，没有历史成交信息:', str(e)
        ic(msg)

def cal_trade_analysis(trade_df, account, strategy_name, run_time_utc=None):
    """
    根据成交信息进行分析
    """
    trade_df['realizedPnl'] = trade_df['realizedPnl'].astype("float")
    # trade_df['commission_usdt'] = trade_df['commission_usdt'].astype("float")
    # fee_sum = round(trade_df['commission_usdt'].sum(), 3)
    trade_df = trade_df[trade_df['realizedPnl'] != 0]
    trade_num = len(trade_df)  # 交易次数
    max_profit = round(trade_df['realizedPnl'].max(), 3)  # 最大盈利
    max_loss = round(trade_df['realizedPnl'].min(), 3)  # 最大亏损
    mean_profit = round(trade_df['realizedPnl'].mean(), 3)  # 盈亏均值
    trade_df['盈利'] = trade_df.loc[trade_df['realizedPnl'] > 0, 'realizedPnl']
    trade_df['亏损'] = trade_df.loc[trade_df['realizedPnl'] < 0, 'realizedPnl']

    profit_all = trade_df['盈利'].sum()  # 盈利金额
    loss_all = trade_df['亏损'].sum()  # 亏损金额
    profit_loss_all = profit_all + loss_all  # 最后金额
    # actual_profit = profit_loss_all - fee_sum  # 实际盈利为  盈利部分减去手续费总和

    # ======================亏损统计=============================
    loss_df = trade_df[trade_df['亏损'] < 0]
    # 开多亏损次数
    loss_buy_df = loss_df[loss_df['side'] == 'SELL']  # 平仓为卖 说明开仓时开多
    long_loss_num = len(loss_buy_df)
    # 开空亏损次数
    loss_sell_df = loss_df[loss_df['side'] == 'BUY']  # 平仓为买 说明开仓时开空
    short_loss_num = len(loss_sell_df)

    # =========================盈利统计===============================
    win_df = trade_df[trade_df['盈利'] > 0]
    # 开多盈利次数
    win_buy_df = win_df[win_df['side'] == 'SELL']  # 平仓为卖 说明开仓时开多
    long_win_num = len(win_buy_df)
    # 开空盈利次数
    win_sell_df = win_df[win_df['side'] == 'BUY']  # 平仓为买 说明开仓时开空
    short_win_num = len(win_sell_df)

    # 计算盈亏比   单笔盈亏 除以 单笔亏损
    win_mean = profit_all / len(win_df)
    loss_mean = loss_all / len(loss_df)
    win_loss_ratio = abs(win_mean / loss_mean)

    win_rate = round(len(win_df) / (len(win_df) + len(loss_df)), 3)
    long_win_rate = round(long_win_num / (long_loss_num + long_win_num), 3)
    short_win_rate = round(short_win_num / (short_win_num + short_loss_num), 3)

    # 开多分析
    long_df = trade_df[trade_df['side'] == 'SELL']  # 平仓为卖 说明开仓时开多
    long_profit = long_df['盈利'].sum()
    long_loss = long_df['亏损'].sum()

    # 开空分析
    short_df = trade_df[trade_df['side'] == 'BUY']  # 平仓为买 说明开仓时开空
    short_profit = short_df['盈利'].sum()
    short_loss = short_df['亏损'].sum()

    # 分析前10盈利币种以及亏损币种
    group_df = trade_df.groupby('symbol').agg({'realizedPnl': 'sum'})
    top_win = group_df.sort_values('realizedPnl', ascending=False).iloc[:10].to_dict()
    top_loss = group_df.sort_values('realizedPnl', ascending=True).iloc[:10].to_dict()

    statistics_analysis = {
        "account": account,
        "strategy_name": strategy_name,
        "trade_num" : trade_num, # 交易次数
        "max_profit": max_profit, # 最大盈利
        "max_loss": max_loss, # 最大亏损
        # "fee": fee_sum, # 手续费综合
        "profit_loss_all": profit_loss_all, # 盈亏总金额
        # "actual_profit": actual_profit, # 实际盈利金额
        "win_loss_ratio": win_loss_ratio,
        "long_loss_num": long_loss_num, # 多头亏损次数
        "short_loss_num": short_loss_num, # 空头亏损次数
        "long_win_num": long_win_num, # 多头盈利次数
        "short_win_num": short_win_num, # 空头盈利次数
        "win_rate":win_rate, # 胜率
        "long_win_rate":long_win_rate, # 多头胜率
        "short_win_rate": short_win_rate, # 空头胜率
        "long_profit":long_profit, # 多头盈利金额
        "long_loss": long_loss, # 多头亏损金额
        "short_profit": short_profit, # 空头盈利金额
        "short_loss": short_loss, # 空头亏损金额
        # "top_win": top_win['realizedPnl'], # 盈利前10币种
        # "top_loss": top_loss['realizedPnl'], # 亏损前10币种
        # "time": datetime.now()
        "time": run_time_utc
    }
    return statistics_analysis

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
        ic('time_interval格式不符合规范。程序exit')
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
    ic('程序下次运行的时间：', target_time)
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