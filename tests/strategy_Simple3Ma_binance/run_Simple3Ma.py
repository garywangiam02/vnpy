# flake8: noqa

from vnpy.trader.utility import load_json
from vnpy.app.cta_crypto.portfolio_testing import single_test
from datetime import datetime
import os
import sys
from copy import copy

# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', ))
if VNPY_ROOT not in sys.path:
    sys.path.append(VNPY_ROOT)
    print(f'append {VNPY_ROOT} into sys.path')


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
        config_dict.update({f'{underlying_symbol}': config})

    return config_dict


# 回测引擎参数
test_setting = {}

test_setting['name'] = 'S_StrategyMacdChannelGroup_v1_6_{}'.format(datetime.now().strftime('%m%d_%H%M'))

# 测试时间段, 从开始日期计算，过了init_days天，才全部激活交易
test_setting['start_date'] = '20201001'
test_setting['init_days'] = 10
test_setting['end_date'] = '20210205'

# 测试资金相关, 资金最大仓位， 期初资金
test_setting['percent_limit'] = 50
test_setting['init_capital'] = 20000

# 测试日志相关， Ture，开始详细日志， False, 只记录简单日志
test_setting['debug'] = True
test_setting['mode'] = 'bar'

# 配置是当前运行目录的相对路径
test_setting['data_path'] = 'data'
test_setting['logs_path'] = 'log'

# 测试数据文件相关(可以从test_symbols加载，或者自定义）
test_setting['bar_interval_seconds'] = 60 * 15  # 回测数据的秒周期

# 从配置文件中获取回测合约的基本信息，并更新bar的路径
symbol_datas = get_symbol_configs(
    json_file_name=os.path.abspath(os.path.join(VNPY_ROOT, 'vnpy', 'data', 'binance', 'future_contracts.json')),
    bar_file_format=""
)
test_setting['symbol_datas'] = symbol_datas

# 输出每日净值曲线
test_setting['is_plot_daily'] = True

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
symbol = 'BTCUSDT'
vtSymbol = 'BTCUSDT.BINANCE'


x_minute = 60  # K线分钟数
x_ma1_len = 17
x_ma2_len = 24
x_ma3_len = 78

symbol_info = symbol_datas.get(vtSymbol)
# underlying_symbol = symbol_info.get('underlying_symbol')
symbol_info.update({'bar_file': VNPY_ROOT.replace('\\', r'/') + f'/bar_data/binance/{symbol}_20170101_1m.csv'})
symbol_datas.update({symbol: symbol_info})

# 更新测试名称
test_setting.update({'name': test_setting['name'] + f"_{symbol}"})
test_setting.update({'using_99_contract': False})

# vt_symbol = symbol + '.' + symbol_info.get('exchange')
vt_symbol = symbol

bar_name = f'f{x_minute}_s{x_ma1_len}_ema{x_ma2_len}_M{x_ma3_len}'

strategy_setting = {
    f"S_Triple_MA_{symbol}": {
        "class_name": "StrategyMacdChannelGroup_v1_6",
        "vt_symbol": f"BTCUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "class_name": "StrategyMacdChannelGroup_v1_6",
            "max_invest_rate": 0.01,
            "bar_names": [
                "f12_s26_n9_M120"
            ]
        }
    }
}

single_test(test_setting=test_setting, strategy_setting=strategy_setting)
