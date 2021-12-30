import ccxt
import pandas as pd
import math
import configparser
import warnings
import time as t
import os
from config import *
from function import *
from sqlalchemy import create_engine
import pymysql
from icecream import ic

def Timestamp():
    return '%s |> ' % time.strftime("%Y-%m-%d %T")

# 定制输出格式
ic.configureOutput(prefix=Timestamp)
if debug !=1:
    ic.disable()
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 6000)  # 最多显示数据的行数


def main():
    while True:
        # 更新时差
        tz_server_offset = tz_server.utcoffset(datetime.utcnow())
        tz_local_offset = tz_local.utcoffset(datetime.utcnow())

        # =====sleep直到下一个15分钟
        if debug == 1:
            # 调试模式，直接进入下一个循环
            run_time_utc = sleep_until_run_time(time_interval, if_sleep=False, cheat_seconds=0) - tz_server_offset
        else:
            run_time_utc = sleep_until_run_time(time_interval, if_sleep=True, cheat_seconds=0) - tz_server_offset
        df_all = pd.DataFrame()

        # 用于存储新账户资金
        new_acc_capital_total = 0

        table_position = 'binance_position' # 用于记录仓位信息
        table_balance = 'binance_balance' # 用于记录每个账户的资金曲线
        table_history_trade = 'binance_history_trade' # 用于记录订单信息，
        table_trade_analysis = 'binance_trade_analysis' # 分析盈亏信息
        # 读取config数据，获得账户列表,循环每个账户
        for account, config in all_api.items():
            # 获取账户设置信息
            apiKey = config['apiKey']
            secret = config['secret']
            strategy_name = config['strategy']
            # 设置mysql的table名称
            # table_acc = account[:20] + '_acc'
            # table_trading = account[:20] + '_trading'
            # 交易所设置初始化
            exchange = ccxt.binance(exchange_config(apiKey, secret))

            # ================计算账户持仓指标 df_acc================
            # acc_info = get_acc_info(exchange, table_acc, account, run_time_utc, tz_server_offset, tz_local_offset)
            acc_info = get_acc_info(exchange, table_balance, account, run_time_utc, tz_server_offset, tz_local_offset,strategy_name )
            df_acc = acc_info[0]
            df_acc = df_acc.replace([np.inf, -np.inf], np.nan)
            df_acc.fillna(value=0, inplace=True)
            ic(f'{account[:4]}_df_acc:', df_acc)

            # 新账户的资金额存入new_acc_capital_total
            new_acc_capital_total += acc_info[1]

            # df_acc添加到df_all,方便计算总资金曲线
            df_all = df_all.append(df_acc, ignore_index=True)

            cursor = conn.cursor()
            try:

                sql = f"delete from {table_position} where account='{account}';"
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                ic(e)
                # 错误回滚
                conn.rollback()
            # 写入数据库资金曲线
            with engine.begin() as con:
                df_acc.to_sql(name=table_balance, con=con, if_exists='append', index=False)

            # ================获取账户的实际持仓信息df_trading ================
            # df_trading = get_trading_info(exchange,table_trading,account)
            df_trading = get_trading_info(exchange,table_position,account, strategy_name)
            ic(f'{account[:4]}_trading_info:',df_trading)

            # 写入数据库trading_table
            with engine.begin() as con:
                df_trading.to_sql(name=table_position, con=con, if_exists='append', index=False)

            try:
                # ==================获取策略的成交信息进行分析 ===========================
                # 获取最新的历史成交信息
                df_history_trade = get_history_trade(exchange, table_history_trade, account, strategy_name)
                ic(df_history_trade)

                # 添加分析信息
                # 获取成交所有成交信息
                df_sql_histoty_trade = get_data_from_mysql(table_name=table_history_trade, account=account, limit=None)

                statistics_analysis = cal_trade_analysis(df_sql_histoty_trade, account, strategy_name, run_time_utc)
                ic(statistics_analysis)

                df_trade_analysis = pd.DataFrame()
                df_trade_analysis = df_trade_analysis.append(statistics_analysis, ignore_index=True)

                # 判断是否有数据库
                table_judge(db_name, table_trade_analysis, 5)
                # 写入数据库table_trade_analysis
                with engine.begin() as con:
                    df_trade_analysis.to_sql(name=table_trade_analysis, con=con, if_exists='append', index=False)
            except Exception as e:
                msg = f'{account}账户不存在历史成交信息' + str(e)
                ic(msg)

        # =====统计所有持仓指标，计算综合资金曲线 df_sum ==============
        df_sum = get_overview_info(df_all, run_time_utc,new_acc_capital_total)
        ic('df_sum:',df_sum)
        ic('new_acc_capital_total:',new_acc_capital_total)
        # 写入数据库overview_table
        with engine.begin() as con:
            df_sum.to_sql(name='overview', con=con, if_exists='append', index=False)

        # 结束循环前打印结束时间
        ic(datetime.now())
        if debug == 1:
            exit()


if __name__ == '__main__':
    main()
