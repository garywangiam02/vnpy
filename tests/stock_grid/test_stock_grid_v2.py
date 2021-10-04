# flake8: noqa

import os
import sys
from copy import copy

# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', ))
sys.path.append(VNPY_ROOT)
print(f'append {VNPY_ROOT} into sys.path')
os.environ["VNPY_TESTING"] = "1"

from datetime import datetime
from vnpy.app.cta_stock.portfolio_testing import single_test
from vnpy.trader.utility import load_json

from vnpy.data.stock.stock_base import get_stock_base
from vnpy.data.tdx.tdx_stock_data import TdxStockData

from datetime import datetime
from vnpy.app.cta_stock.portfolio_testing import single_test
from vnpy.trader.utility import load_json
from vnpy.data.stock.stock_base import get_stock_base

def get_symbol_configs(bar_file_format):
    """
    根据文件获取合约的配置
    :param bar_file_format:
    :return: dict
    """
    # 从vnpy.data.stock目录下，获取股票得基本信息缓存
    config_dict = get_stock_base()
    for vt_symbol, config in config_dict.items():
        config.update({
            'product': "股票",
            'price_tick': 0.01,
            'symbol_size': 100,
            'min_volume':100,
            'margin_rate': 1,
            'commission_rate': 0.001,
            'bar_file': bar_file_format.format(config.get('exchange'), config.get('code'))})

    return config_dict

# 回测引擎参数
test_setting = {}

test_setting['name'] = 'stock_grid_{}'.format(datetime.now().strftime('%m%d_%H%M'))

# 测试时间段, 从开始日期计算，过了init_days天，才全部激活交易
test_setting['start_date'] = '20200101'
test_setting['init_days'] = 30
test_setting['end_date'] = '20211231'

# 测试资金相关, 资金最大仓位， 期初资金
test_setting['percent_limit'] = 20
test_setting['init_capital'] = 1000000     # 100万 现金

# 测试日志相关， Ture，开始详细日志， False, 只记录简单日志
test_setting['debug'] = False

# 配置是当前运行目录的相对路径
test_setting['data_path'] = 'data'
test_setting['logs_path'] = 'log'

# 测试数据文件相关(可以从test_symbols加载，或者自定义）
test_setting['bar_interval_seconds'] = 60   # 回测数据的秒周期

# 从配置文件中获取回测合约的基本信息，并更新bar的路径
symbol_datas = get_symbol_configs(
    bar_file_format=VNPY_ROOT + '/bar_data/{}/{}_1m.csv'
)

test_setting['symbol_datas'] = symbol_datas

"""
# 这里是读取账号的cta strategy pro json文件，作为一个组合
# 读取组合回测策略的参数列表
strategy_setting = load_json('cta_stock_setting.json')


task_id = str(uuid1())
print(f'添加celery 任务：{task_id}')
execute.apply_async(kwargs={'func': 'vnpy.app.cta_stock.portfolio_testing.single_test',
                                'test_setting': test_setting,
                                'strategy_setting': strategy_setting},
                                task_id=task_id)

"""

# 创建回测任务
count = 0

vt_symbol = '600410.SSE'

# 单回测，只保留该合约的数据，删除其他数据
symbol_info = symbol_datas.get(vt_symbol)
symbol_datas = {vt_symbol: symbol_info}
test_setting['symbol_datas'] = symbol_datas

# 更新测试名称
test_setting.update({'name': test_setting['name'] + f"_{vt_symbol}"})
#
strategy_setting = {
    f"stock_grid_{vt_symbol}": {
        "class_name": "StrategyStockGridTradeV2",
        "vt_symbols": [vt_symbol],
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "max_invest_rate": 0.2,
            "max_single_margin": 12000,
            "grid_height_percent": 5,
            "x_minute": 5,
            "grid_lots": 16
        }
    }
}

single_test(test_setting=test_setting, strategy_setting=strategy_setting)


