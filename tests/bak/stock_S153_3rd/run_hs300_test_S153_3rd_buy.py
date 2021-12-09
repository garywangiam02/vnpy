# flake8: noqa

import baostock as bs
from vnpy.data.stock.stock_base import get_stock_base
from vnpy.trader.utility import load_json
from vnpy.app.cta_stock.portfolio_testing import single_test
from datetime import datetime
import os
import sys
from copy import copy

# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', ))
if VNPY_ROOT not in sys.path:
    sys.path.append(VNPY_ROOT)
    print(f'append {VNPY_ROOT} into sys.path')


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
            'min_volume': 100 if config.get('type') != 'stock_cb' else 10,
            'margin_rate': 1,
            'commission_rate': 0.001 if config.get('type') != 'stock_cb' else 0.00005,
            'bar_file': bar_file_format.format(config.get('exchange'), config.get('code'))})

    return config_dict


# 回测引擎参数
test_setting = {}

test_setting['name'] = 'stock_3rd_buy_{}'.format(datetime.now().strftime('%m%d_%H%M'))

# 测试时间段, 从开始日期计算，过了init_days天，才全部激活交易
test_setting['start_date'] = '20180101'
test_setting['init_days'] = 30
test_setting['end_date'] = '20211231'

# 测试资金相关, 资金最大仓位， 期初资金
test_setting['percent_limit'] = 100
test_setting['init_capital'] = 10000000     # 1000万 现金

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

lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 获取沪深300成分股
rs = bs.query_hs300_stocks()
print('query_hs300 error_code:' + rs.error_code)
print('query_hs300  error_msg:' + rs.error_msg)

# 打印结果集
hs300_stocks = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data = rs.get_row_data()

    ex, code = data[1].split('.')
    if ex == 'sh':
        vt_symbol = f'{code}.SSE'
    else:
        vt_symbol = f'{code}.SZSE'
    hs300_stocks.append(vt_symbol)
    # if len(hs300_stocks) >=100:
    #     break

hs300_stocks = hs300_stocks[-100:]

# hs300_stocks = [ '000651.SZSE','600036.SSE']
# 更新测试名称
test_setting.update({'name': test_setting['name'] + f"_hs300"})
#
strategy_setting = {
    f"stock_S153_hs300": {
        "class_name": "StrategyStock3rdBuyGroupV1",
        "vt_symbols": hs300_stocks,
        "auto_init": True,
        "setting": {
            "backtesting": True,

            "class_name": "StrategyStock3rdBuyGroupV1",
            "max_invest_rate": 1,
            "share_symbol_count": 20

        }
    }
}

single_test(test_setting=test_setting, strategy_setting=strategy_setting)
