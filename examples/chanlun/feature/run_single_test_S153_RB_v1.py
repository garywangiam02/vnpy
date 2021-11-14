# flake8: noqa

import os
import sys
from copy import copy

# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..','..', ))
if VNPY_ROOT not in sys.path:
    sys.path.append(VNPY_ROOT)
    print(f'append {VNPY_ROOT} into sys.path')

from datetime import datetime
from vnpy.trader.object import Exchange
from vnpy.app.cta_strategy_pro.portfolio_testing import single_test
from vnpy.trader.utility import load_json


def get_symbol_configs(json_file_name, bar_file_format):
    """
    根据文件获取合约的配置
    :param json_file_name:
    :param bar_file_format:
    :return: dict
    """
    config_dict = load_json(json_file_name)
    for underlying_symbol in list(config_dict.keys()):
        config = config_dict.pop(underlying_symbol, {})
        config.pop('mi_symbol', None)
        config.pop('full_symbol', None)
        config.update({
            'product': "期货",
            'commission_rate': 0.0001 if underlying_symbol not in ['T', 'TF'] else 5,
            'bar_file': bar_file_format.format(underlying_symbol)})
        config_dict.update({f'{underlying_symbol}99': config})

    return config_dict


# 回测引擎参数
test_setting = {}

test_setting['name'] = 'S153_ChanThree_{}'.format(datetime.now().strftime('%m%d_%H%M'))

# 测试时间段, 从开始日期计算，过了init_days天，才全部激活交易
test_setting['start_date'] = '20210501'
test_setting['init_days'] = 10
test_setting['end_date'] = '20211030'

# 测试资金相关, 资金最大仓位， 期初资金
test_setting['percent_limit'] = 100
test_setting['init_capital'] = 30000

# 测试日志相关， Ture，开始详细日志， False, 只记录简单日志
test_setting['debug'] = True
test_setting['mode'] = 'bar'

# 配置是当前运行目录的相对路径
test_setting['data_path'] = 'data'
test_setting['logs_path'] = 'log'

# 测试数据文件相关(可以从test_symbols加载，或者自定义）
test_setting['bar_interval_seconds'] = 60  # 回测数据的秒周期

# 从配置文件中获取回测合约的基本信息，并更新bar的路径
symbol_datas = get_symbol_configs(
    json_file_name=os.path.abspath(os.path.join(VNPY_ROOT, 'vnpy', 'data', 'tdx', 'future_contracts.json')),
    bar_file_format=VNPY_ROOT + '/bar_data/tdx/{}99_20170101_1m.csv'
)

test_setting['symbol_datas'] = symbol_datas

"""
# 这里是读取账号的cta strategy pro json文件，作为一个组合
# 读取组合回测策略的参数列表
strategy_setting = load_json('cta_strategy_pro_setting.json')


task_id = str(uuid1())
print(f'添加celery 任务：{task_id}')
execute.apply_async(kwargs={'func': 'vnpy.app.cta_strategy_pro.portfolio_testing.single_test',
                                'test_setting': test_setting,
                                'strategy_setting': strategy_setting},
                                task_id=task_id)

"""

# 创建回测任务
count = 0

# 区别于一般得指数合约J99，前面需要加上future_renko,bar回测引擎才能识别
# symbol = 'J99'
#
# symbol_info = symbol_datas.get(symbol)
# underlying_symbol = symbol_info.get('underlying_symbol')
# symbol_info.update({'bar_file': VNPY_ROOT + f'/bar_data/tdx/{symbol}_20160101_1m.csv'})
# symbol_datas.update({symbol: symbol_info})

# 更新测试名称
# test_setting.update({'name': test_setting['name'] + f"_{symbol}"})

strategy_setting = {}

# selected_symbol = #['IF', 'IC', 'IH', 'AP', 'J', 'SC', 'AG', 'TA', 'RU', 'SF', 'MA', 'SM', 'PB', 'V', 'ZC', 'ZN', 'RB', 'HC', 'CS', 'AU', 'C', 'I', 'A', 'M', 'SR', 'JD', 'NI', 'P', 'CY', 'AL', 'CU', 'FG', 'SP', 'Y', 'FU', 'OI', 'CJ', 'BU', 'CF', 'L', 'PP']
selected_symbol = {"M": 40, "RM": 40,
                   "SA": 10,
                   "CS": 40, "C": 40,
                   "CF": 10,
                   "JD": 24,
                   "SR": 30,
                   "A": 40, "B": 40,
                   "MA": 12, "TA": 12, "PP": 10, "V": 15,
                   "RU": 9,
                   "J": 5, "JM": 6, "ZC": 8,
                   "I": 6, "RB": 8, "HC": 8, "SS": 5,
                   "AG": 5, "CU": 3, "AU": 4,
                   "NI": 3,
                   "AL": 15, "PB": 4, "ZN": 4, "SN": 5,
                   "FU": 8, "SC": 2, "BU": 12,
                   "P": 20, "Y": 20, "OI": 20,
                   "FG": 8,
                   "T": 5, "TF": 5,
                   "EB": 6, "EG": 6,
                   "L": 10,
                   "SP": 20,
                   "SM": 12, "SF": 8,
                   "AP": 21,
                   "CJ": 20,
                   # "IC":1,"IF":1,"IH":1
                   }

max_invest_rate = 0.5 # round(1 / len(selected_symbol),2)

for symbol, symbol_info in symbol_datas.items():
    exchange = symbol_info.get('exchange', 'LOCAL')
    if exchange not in [Exchange.CZCE.value, Exchange.DCE.value, Exchange.SHFE.value, Exchange.INE.value,
                        Exchange.CFFEX.value]:
        continue

    underlying_symbol = symbol_info.get('underlying_symbol')
    if underlying_symbol not in ['SP']: #selected_symbol: #
        continue

    max_invest_pos = 0 #selected_symbol[underlying_symbol]

    vt_symbol = symbol + '.' + exchange

    strategy_setting.update({
        f"S153_ChanThree_{symbol}": {
            "class_name": "Strategy153_Chan_Three_V1",
            "vt_symbol": f"{vt_symbol}",
            "auto_init": True,
            "setting": {
                "backtesting": True,
                "max_invest_pos": max_invest_pos,
                "single_lost_rate":0.1,
                "max_invest_rate": max_invest_rate,
                  "bar_names": "M5-M30"
            }
        }
    })



single_test(test_setting=test_setting, strategy_setting=strategy_setting)
