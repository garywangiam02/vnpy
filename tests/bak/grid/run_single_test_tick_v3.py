# flake8: noqa

import os
import sys
from copy import copy

VNPY_ROOT='/Users/gary/workspace/quant/vnpy/'
# 将repostory的目录，作为根目录，添加到系统环境中。
# VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', ))
# if VNPY_ROOT not in sys.path:
#     sys.path.append(VNPY_ROOT)
#     print(f'append {VNPY_ROOT} into sys.path')

from datetime import datetime
from vnpy.app.cta_strategy_pro.portfolio_testing import single_test
from vnpy.trader.utility import load_json, get_underlying_symbol


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

test_setting['name'] = 'FutureGrid_v3_{}'.format(datetime.now().strftime('%m%d_%H%M'))

# 测试时间段, 从开始日期计算，过了init_days天，才全部激活交易
test_setting['start_date'] = '20210501'
test_setting['init_days'] = 5
test_setting['end_date'] = '20210701'

# 测试资金相关, 资金最大仓位， 期初资金
test_setting['percent_limit'] = 20
test_setting['init_capital'] = 2000000

# 测试日志相关， Ture，开始详细日志， False, 只记录简单日志
test_setting['debug'] = True
test_setting['mode'] = 'bar'
test_setting['tick_path'] = os.path.abspath(os.path.join(VNPY_ROOT, 'bar_data'))
test_setting['use_tq'] = True  # 使用天勤下载tick数据
test_setting['using_99_contract'] = False  # 不强制转换使用99合约符号

# 配置是当前运行目录的相对路径
test_setting['data_path'] = 'data'
test_setting['logs_path'] = 'log'

# 测试数据文件相关(可以从test_symbols加载，或者自定义）
test_setting['bar_interval_seconds'] = 60  # 回测数据的秒周期

# 从配置文件中获取回测合约的基本信息，并更新bar的路径
symbol_datas = get_symbol_configs(
    json_file_name=os.path.abspath(os.path.join(VNPY_ROOT, 'vnpy', 'data', 'tdx', 'future_contracts.json')),
    bar_file_format=""
)
test_setting['symbol_datas'] = symbol_datas

# 创建回测任务
count = 0

# 回测的真实合约
symbol = 'ag2112'
# 回测的短合约
underlying_symbol = get_underlying_symbol(symbol).upper()
# 只回测一个合约的话，就更新下，不用太多合约参数
symbol_info = symbol_datas.get(f'{underlying_symbol}99')
symbol_datas.update({f'{underlying_symbol}99': symbol_info})

# 更新回测实例名称
test_setting.update({'name': test_setting['name'] + f"_{symbol}"})

# 合约+交易所
vt_symbol = symbol + '.' + symbol_info.get('exchange')

# 回测策略的设置 {回测策略实例名1: 策略参数}， {策略实例名2: 策略参数},,,
strategy_setting = {
    f"Future_grid_tick_{symbol}": {
        "class_name": "StrategyGridTradeFutureV3",
        "vt_symbol": f"{vt_symbol}",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "class_name": "StrategyGridTradeFutureV3",
            "max_invest_percent": 10,
            "grid_repeats": 3,
            "x_minute": 1,
            "grid_height_percent": 1,
            "active_short_grid": True
        }
    }
}

# 调用组合回测, 输入回测参数，策略参数
single_test(test_setting=test_setting, strategy_setting=strategy_setting)
