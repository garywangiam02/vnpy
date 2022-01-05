from trading.functions import *
from trading.utility import notify_sender
from trading.multi_trade import trade
from apscheduler.schedulers.background import BackgroundScheduler
import os
import pickle
from datetime import datetime

from config.trade_config import *
from config.config import settings

pd.options.mode.chained_assignment = None
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def select_coin_apaptbollv3():
    """
    轮动选币
    """

    run_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    # =====选币数据整理 & 选币
    if os.path.exists(strategy_config_file_path):
        with open(strategy_config_file_path, "rb") as file:
              expected_strategy_config = pickle.load(file)
    else:
        # =====并行获取所有币种的1小时K线
        symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time)
        print(symbol_candle_data['BTCUSDT'].tail(2), '\n')
        select_coin = cal_factor_and_select_coin(stratagy_list, symbol_candle_data, run_time)
        select_coin = select_coin[select_coin['方向'] == 1]
        select_coin_file_path = f'{path_root_out}/select_coin_final.csv'
        select_coin.to_csv(select_coin_file_path)
        # 计算V3策略配置信息
        expected_strategy_config = cal_strategy_config(select_coin)

    # 重置CTA策略配置
    trade_father.adjust_strategys('unset', expected_strategy_config)


def reset_coin_config():
    """
    rename 策略配置，使得可以正常选出下一次币
    """
    if os.path.exists(strategy_config_file_path):
        file_rename = strategy_config_file_path + '.bak'
        os.rename(strategy_config_file_path, file_rename)


def cal_strategy_config(select_coin):
    """
    梳理策略配置
    """
    select_coin['final_score'] = select_coin['c12']
    select_coin = select_coin.sort_values(by='final_score', ascending=False)
    select_coin = select_coin.drop_duplicates(subset=['symbol'], keep='first')  # 同样的币种,只保留得分最高记录
    print(select_coin.tail(5))
    strategy_config = {}
    for i, r in select_coin.iterrows():
        coin_config = {}
        coin_config['symbol'] = r['symbol']
        coin_config['strategy_name'] = 'adaptboll_v3_with_stoploss'
        coin_config['para'] = [12]
        coin_config['data_num'] = 1200  # 策略函数需要多少根k线
        coin_config['time_interval'] = '1h'  # 策略的时间周期
        coin_config['leverage'] = 1  # 策略基础杠杆
        coin_config['weight'] = round(MAX_LEVERAGE / selected_coin_num, 2)  # 策略分配的资金权重
        config_symbol = coin_config['symbol'] if (coin_config['symbol'])[
                                                  0:1].isalpha() else "_" + coin_config['symbol']  # 兼容1000SHIBUSDT
        strategy_name = "_".join([config_symbol, coin_config['strategy_name'], coin_config['time_interval']])
        strategy_config[strategy_name] = coin_config
    print(strategy_config)
    with open(strategy_config_file_path, "wb") as file:
        pickle.dump(strategy_config, file)  # 保存策略配置
    return strategy_config


if __name__ == '__main__':
    with open(strategy_config_file_path, "rb") as file:
            expected_strategy_config = pickle.load(file)
            aa = 1
#    cta_config = {
#       'ETHUSDT_adaptboll_v3_with_stoploss_1h': {  # 该策略配置的名称随意，可以自己取名
#           'symbol': 'ETHUSDT',  # 交易标的
#           'strategy_name': 'adaptboll_v3_with_stoploss',  # 你的策略函数名称
#           'para': [12],  # 参数
#           'data_num': 1200,  # 策略函数需要多少根k线
#           'time_interval': '1h',  # 策略的时间周期
#           'leverage': 1,  # 策略基础杠杆
#           'weight': 0.15,  # 策略分配的资金权重
#        },
#    }
#    trade_father = trade(apiKey = settings.BINANCE_API_KEY, secret = settings.BINANCE_SECRET, config = cta_config, notify_sender = notify_sender, posInfer=False, proxies=False, timeout=1)
#    select_coin_apaptbollv3()
#    scheduler = BackgroundScheduler()
#    scheduler.add_job(reset_coin_config,'cron',month = '1-12',day ='1', hour='9',minute = '43',id = 'reset_coin_config')  # 每月1号执行一次
#    scheduler.add_job(select_coin_apaptbollv3,'cron',month = '1-12',day ='1', hour='9',minute = '45',id = 'select_coin_apaptbollv3')  # 每月1号执行一次
#    #scheduler.add_job(reset_coin_config,'cron',day_of_week = 'sun', hour='9',minute = '43',id = 'reset_coin_config') #每周运行一次
#    #scheduler.add_job(select_coin_apaptbollv3,'cron',day_of_week = 'sun', hour='9',minute = '45',id = 'select_coin_apaptbollv3') #每周运行一次
#    #scheduler.add_job(reset_coin_config,'cron',minute = '*/3',id = 'reset_coin_config')
#    #scheduler.add_job(select_coin_apaptbollv3,'cron',minute='*/6',id = 'select_coin_apaptbollv3')


#    scheduler.start()

#    while(True):
#         time.sleep(60*30)
