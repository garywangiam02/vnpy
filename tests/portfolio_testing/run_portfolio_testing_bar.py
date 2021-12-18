# flake8: noqa

import os
import sys
from copy import copy

# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', ))
if VNPY_ROOT not in sys.path:
    sys.path.append(VNPY_ROOT)
    print(f'append {VNPY_ROOT} into sys.path ...')

os.environ["VNPY_TESTING"] = "1"

from datetime import datetime
from vnpy.app.cta_crypto.portfolio_testing import single_test  # portfolio_testing
from vnpy.trader.utility import load_json
from vnpy.data.binance.binance_future_data import BinanceFutureData

def get_symbol_configs(bar_file_format):
    """
    根据文件获取合约的配置
    :param bar_file_format:
    :return: dict
    """
    config_dict = BinanceFutureData.load_contracts()
    for vt_symbol in list(config_dict.keys()):
        config = config_dict.pop(vt_symbol, {})
        config.update({
            'commission_rate': 0.0002,
            'bar_file': bar_file_format.format(config.get('symbol'))})
        config_dict.update({config.get('symbol'): config})
    return config_dict


# 回测引擎参数
test_setting = {}

test_setting['name'] = 'FutureGrid_cta_crypto_portfolio_testing_{}'.format(datetime.now().strftime('%m%d_%H%M'))

# 测试时间段, 从开始日期计算，过了init_days天，才全部激活交易
test_setting['start_date'] = '20190101'
test_setting['init_days'] = 10
test_setting['end_date'] = '20211231'

# 测试资金相关, 资金最大仓位， 期初资金
test_setting['percent_limit'] = 100
test_setting['init_capital'] = 10000

# 测试日志相关， Ture，开始详细日志， False, 只记录简单日志
test_setting['debug'] = False

# 配置是当前运行目录的相对路径
test_setting['data_path'] = 'data'
test_setting['logs_path'] = 'log'

# 测试数据文件相关(可以从test_symbols加载，或者自定义）
test_setting['bar_interval_seconds'] = 60  # 回测数据的秒周期

# 创建回测任务
count = 0

# 从配置文件中获取回测合约的基本信息，并更新bar的路径
symbol_datas = get_symbol_configs(
    bar_file_format=VNPY_ROOT + '/../bar_data/binance/{}_20170101_1m.csv'
)

test_setting['symbol_datas'] = symbol_datas

# 更新测试名称
# test_setting.update({'name': test_setting['name'] + f"_{symbol}"})

strategy_setting = {
    f"S144_BTCUSDT_M30_n6_v1.7": {
        "class_name": "StrategyMacdChannelGroup_v1_7",
        "vt_symbol": "BTCUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "max_invest_rate": 0.01,
            "bar_names": ['f12_s26_n6_M30']
        }
    },
    f"S144_BTCUSDT_M120": {
        "class_name": "StrategyMacdChannelGroup_v1_7",
        "vt_symbol": "BTCUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "max_invest_rate": 0.01,
            "bar_names": ['f12_s26_n9_M120']
        }
    },
    f"Future_grid_tick_BTCUSDT": {
        "class_name": "StrategyGridTradeFuture_v2",
        "vt_symbol": "BTCUSDT.BINANCE",
        "setting": {
            "activate_market": False,
            "backtesting": True,
            "class_name": "StrategyGridTradeFuture_v2",
            "max_invest_rate": 0.01,
            "grid_height_percent": 7.5,
            "grid_lots": 8,
            "x_minute": 15
        }
    },

    f"S144_ETHUSDT_M30_n6_v1.7": {
        "class_name": "StrategyMacdChannelGroup_v1_7",
        "vt_symbol": "ETHUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "activate_market": False,
            "backtesting": True,
            "max_invest_rate": 0.01,
            "bar_names": ['f12_s26_n6_M30']
        }
    },
    f"S144_ETHUSDT_M120": {
        "class_name": "StrategyMacdChannelGroup_v1_7",
        "vt_symbol": "ETHUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "max_invest_rate": 0.01,
            "bar_names": ['f12_s26_n9_M120']
        }
    },
    f"Future_grid_tick_ETHUSDT": {
        "class_name": "StrategyGridTradeFuture_v2",
        "vt_symbol": "ETHUSDT.BINANCE",
        "setting": {
            "backtesting": True,
            "class_name": "StrategyGridTradeFuture_v2",
            "max_invest_rate": 0.0,
            "grid_height_percent": 7.5,
            "grid_lots": 8,
            "x_minute": 15
        }
    },

    f"S144_BNBUSDT_M30_n6_v1.7": {
        "class_name": "StrategyMacdChannelGroup_v1_7",
        "vt_symbol": "BNBUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "max_invest_rate": 0.01,
            "bar_names": ['f12_s26_n6_M30']
        }
    },
    f"S144_BNBUSDT_M120": {
        "class_name": "StrategyMacdChannelGroup_v1_7",
        "vt_symbol": "BNBUSDT.BINANCE",
        "auto_init": True,
        "setting": {
            "backtesting": True,
            "max_invest_rate": 0.01,
            "bar_names": ['f12_s26_n9_M120']
        }
    },
    f"Future_grid_tick_BNBUSDT": {
        "class_name": "StrategyGridTradeFuture_v2",
        "vt_symbol": "BNBUSDT.BINANCE",
        "setting": {
            "activate_market": False,
            "backtesting": True,
            "class_name": "StrategyGridTradeFuture_v2",
            "max_invest_rate": 0.01,
            "grid_height_percent": 7.5,
            "grid_lots": 8,
            "x_minute": 15
        }
    },

    # f"S144_VETUSDT_M30_n6_v1.7": {
    #     "class_name": "StrategyMacdChannelGroup_v1_7",
    #     "vt_symbol": "VETUSDT.BINANCE",
    #     "auto_init": True,
    #     "setting": {
    #         "backtesting": True,
    #         "max_invest_rate": 0.01,
    #         "bar_names": ['f12_s26_n6_M30']
    #     }
    # },
    # f"S144_VETUSDT_M120": {
    #     "class_name": "StrategyMacdChannelGroup_v1_7",
    #     "vt_symbol": "VETUSDT.BINANCE",
    #     "auto_init": True,
    #     "setting": {
    #         "backtesting": True,
    #         "max_invest_rate": 0.01,
    #         "bar_names": ['f12_s26_n9_M120']
    #     }
    # },
    # f"Future_grid_tick_VETUSDT": {
    #     "class_name": "StrategyGridTradeFuture_v2",
    #     "vt_symbol": "VETUSDT.BINANCE",
    #     "setting": {
    #         "activate_market": False,
    #         "backtesting": True,
    #         "class_name": "StrategyGridTradeFuture_v2",
    #         "max_invest_rate": 0.01,
    #         "grid_height_percent": 7.5,
    #         "grid_lots": 8,
    #         "x_minute": 15
    #     }
    # },

    # f"S144_LINKUSDT_M30_n6_v1.7": {
    #     "class_name": "StrategyMacdChannelGroup_v1_7",
    #     "vt_symbol": "LINKUSDT.BINANCE",
    #     "auto_init": True,
    #     "setting": {
    #         "backtesting": True,
    #         "max_invest_rate": 0.01,
    #         "bar_names": ['f12_s26_n6_M30']
    #     }
    # },
    # f"S144_LINKUSDT_M120": {
    #     "class_name": "StrategyMacdChannelGroup_v1_7",
    #     "vt_symbol": "LINKUSDT.BINANCE",
    #     "auto_init": True,
    #     "setting": {
    #         "backtesting": True,
    #         "max_invest_rate": 0.01,
    #         "bar_names": ['f12_s26_n9_M120']
    #     }
    # },
    # f"Future_grid_tick_LINKUSDT": {
    #     "class_name": "StrategyGridTradeFuture_v2",
    #     "vt_symbol": "LINKUSDT.BINANCE",
    #     "setting": {
    #         "activate_market": False,
    #         "backtesting": True,
    #         "class_name": "StrategyGridTradeFuture_v2",
    #         "max_invest_rate": 0.01,
    #         "grid_height_percent": 7.5,
    #         "grid_lots": 8,
    #         "x_minute": 15
    #     }
    # },

    # f"S144_NEOUSDT_M30_n6_v1.7": {
    #     "class_name": "StrategyMacdChannelGroup_v1_7",
    #     "vt_symbol": "NEOUSDT.BINANCE",
    #     "auto_init": True,
    #     "setting": {
    #         "backtesting": True,
    #         "max_invest_rate": 0.01,
    #         "bar_names": ['f12_s26_n6_M30']
    #     }
    # },
    # f"S144_NEOUSDT_M120": {
    #     "class_name": "StrategyMacdChannelGroup_v1_7",
    #     "vt_symbol": "NEOUSDT.BINANCE",
    #     "auto_init": True,
    #     "setting": {
    #         "backtesting": True,
    #         "max_invest_rate": 0.01,
    #         "bar_names": ['f12_s26_n9_M120']
    #     }
    # },
    # f"Future_grid_tick_NEOUSDT": {
    #     "class_name": "StrategyGridTradeFuture_v2",
    #     "vt_symbol": "NEOUSDT.BINANCE",
    #     "setting": {
    #         "activate_market": False,
    #         "backtesting": True,
    #         "class_name": "StrategyGridTradeFuture_v2",
    #         "max_invest_rate": 0.01,
    #         "grid_height_percent": 7.5,
    #         "grid_lots": 8,
    #         "x_minute": 15
    #     }
    # }
}

single_test(test_setting=test_setting, strategy_setting=strategy_setting)

print(f"all done... {datetime.now()}")

'''

FutureGrid_cta_crypto_portfolio_testing_0531_1212	------------------------------
FutureGrid_cta_crypto_portfolio_testing_0531_1212	第一笔交易：	2019-09-15 07:32:00
FutureGrid_cta_crypto_portfolio_testing_0531_1212	最后一笔交易：	2021-05-28 23:11:00
FutureGrid_cta_crypto_portfolio_testing_0531_1212	总交易次数：	2,524
FutureGrid_cta_crypto_portfolio_testing_0531_1212	期初资金：	10,000
FutureGrid_cta_crypto_portfolio_testing_0531_1212	总盈亏：	11,219.12
FutureGrid_cta_crypto_portfolio_testing_0531_1212	资金最高净值：	38,621.38
FutureGrid_cta_crypto_portfolio_testing_0531_1212	资金最高净值时间：	2021/05/23
FutureGrid_cta_crypto_portfolio_testing_0531_1212	每笔最大盈利：	3,326.2
FutureGrid_cta_crypto_portfolio_testing_0531_1212	每笔最大亏损：	-1,533.41
FutureGrid_cta_crypto_portfolio_testing_0531_1212	净值最大回撤: 	-16,796.1
FutureGrid_cta_crypto_portfolio_testing_0531_1212	净值最大回撤率: 	44.94
FutureGrid_cta_crypto_portfolio_testing_0531_1212	净值最大回撤时间：	2021/05/28
FutureGrid_cta_crypto_portfolio_testing_0531_1212	胜率：	22.86
FutureGrid_cta_crypto_portfolio_testing_0531_1212	盈利交易平均值	302.77
FutureGrid_cta_crypto_portfolio_testing_0531_1212	亏损交易平均值	-83.97
FutureGrid_cta_crypto_portfolio_testing_0531_1212	盈亏比：	3.61
FutureGrid_cta_crypto_portfolio_testing_0531_1212	最大资金占比：	7.38
FutureGrid_cta_crypto_portfolio_testing_0531_1212	平均每笔盈利：	4.44
FutureGrid_cta_crypto_portfolio_testing_0531_1212	平均每笔滑点成本：	0.0
FutureGrid_cta_crypto_portfolio_testing_0531_1212	平均每笔佣金：	2.87
FutureGrid_cta_crypto_portfolio_testing_0531_1212	Sharpe Ratio：	0.52
测试结束
all done... 2021-05-31 13:16:38.434508

Process finished with exit code 0


FutureGrid_cta_crypto_portfolio_testing_0531_1040	------------------------------
FutureGrid_cta_crypto_portfolio_testing_0531_1040	第一笔交易：	2019-09-16 00:02:00
FutureGrid_cta_crypto_portfolio_testing_0531_1040	最后一笔交易：	2021-05-29 11:32:00
FutureGrid_cta_crypto_portfolio_testing_0531_1040	总交易次数：	3,372
FutureGrid_cta_crypto_portfolio_testing_0531_1040	期初资金：	10,000
FutureGrid_cta_crypto_portfolio_testing_0531_1040	总盈亏：	16,968.69
FutureGrid_cta_crypto_portfolio_testing_0531_1040	资金最高净值：	30,595.24
FutureGrid_cta_crypto_portfolio_testing_0531_1040	资金最高净值时间：	2021/05/26
FutureGrid_cta_crypto_portfolio_testing_0531_1040	每笔最大盈利：	1,792.64
FutureGrid_cta_crypto_portfolio_testing_0531_1040	每笔最大亏损：	-831.06
FutureGrid_cta_crypto_portfolio_testing_0531_1040	净值最大回撤: 	-8,410.26
FutureGrid_cta_crypto_portfolio_testing_0531_1040	净值最大回撤率: 	47.7
FutureGrid_cta_crypto_portfolio_testing_0531_1040	净值最大回撤时间：	2021/01/31
FutureGrid_cta_crypto_portfolio_testing_0531_1040	胜率：	35.71
FutureGrid_cta_crypto_portfolio_testing_0531_1040	盈利交易平均值	137.06
FutureGrid_cta_crypto_portfolio_testing_0531_1040	亏损交易平均值	-68.29
FutureGrid_cta_crypto_portfolio_testing_0531_1040	盈亏比：	2.01
FutureGrid_cta_crypto_portfolio_testing_0531_1040	最大资金占比：	6.7
FutureGrid_cta_crypto_portfolio_testing_0531_1040	平均每笔盈利：	5.03
FutureGrid_cta_crypto_portfolio_testing_0531_1040	平均每笔滑点成本：	0.0
FutureGrid_cta_crypto_portfolio_testing_0531_1040	平均每笔佣金：	2.1
FutureGrid_cta_crypto_portfolio_testing_0531_1040	Sharpe Ratio：	0.68
测试结束
all done... 2021-05-31 11:31:03.016363


FutureGrid_cta_crypto_portfolio_testing_0531_0823	------------------------------
FutureGrid_cta_crypto_portfolio_testing_0531_0823	第一笔交易：	2019-09-16 00:02:00
FutureGrid_cta_crypto_portfolio_testing_0531_0823	最后一笔交易：	2021-05-28 16:32:00
FutureGrid_cta_crypto_portfolio_testing_0531_0823	总交易次数：	3,475
FutureGrid_cta_crypto_portfolio_testing_0531_0823	期初资金：	10,000
FutureGrid_cta_crypto_portfolio_testing_0531_0823	总盈亏：	13,394.37
FutureGrid_cta_crypto_portfolio_testing_0531_0823	资金最高净值：	43,469.86
FutureGrid_cta_crypto_portfolio_testing_0531_0823	资金最高净值时间：	2021/05/19
FutureGrid_cta_crypto_portfolio_testing_0531_0823	每笔最大盈利：	4,574.01
FutureGrid_cta_crypto_portfolio_testing_0531_0823	每笔最大亏损：	-3,598.95
FutureGrid_cta_crypto_portfolio_testing_0531_0823	净值最大回撤: 	-19,615.47
FutureGrid_cta_crypto_portfolio_testing_0531_0823	净值最大回撤率: 	67.11
FutureGrid_cta_crypto_portfolio_testing_0531_0823	净值最大回撤时间：	2020/11/02
FutureGrid_cta_crypto_portfolio_testing_0531_0823	胜率：	33.32
FutureGrid_cta_crypto_portfolio_testing_0531_0823	盈利交易平均值	246.24
FutureGrid_cta_crypto_portfolio_testing_0531_0823	亏损交易平均值	-117.28
FutureGrid_cta_crypto_portfolio_testing_0531_0823	盈亏比：	2.1
FutureGrid_cta_crypto_portfolio_testing_0531_0823	最大资金占比：	14.6
FutureGrid_cta_crypto_portfolio_testing_0531_0823	平均每笔盈利：	3.85
FutureGrid_cta_crypto_portfolio_testing_0531_0823	平均每笔滑点成本：	0.0
FutureGrid_cta_crypto_portfolio_testing_0531_0823	平均每笔佣金：	2.62
FutureGrid_cta_crypto_portfolio_testing_0531_0823	Sharpe Ratio：	0.45
测试结束
all done... 2021-05-31 09:35:03.265790


FutureGrid_cta_crypto_portfolio_testing_0529_2234	------------------------------
FutureGrid_cta_crypto_portfolio_testing_0529_2234	第一笔交易：	2019-09-16 00:02:00
FutureGrid_cta_crypto_portfolio_testing_0529_2234	最后一笔交易：	2021-05-29 11:02:00
FutureGrid_cta_crypto_portfolio_testing_0529_2234	总交易次数：	2,515
FutureGrid_cta_crypto_portfolio_testing_0529_2234	期初资金：	10,000
FutureGrid_cta_crypto_portfolio_testing_0529_2234	总盈亏：	194,517.6
FutureGrid_cta_crypto_portfolio_testing_0529_2234	资金最高净值：	323,232.28
FutureGrid_cta_crypto_portfolio_testing_0529_2234	资金最高净值时间：	2021/05/27
FutureGrid_cta_crypto_portfolio_testing_0529_2234	每笔最大盈利：	29,541.66
FutureGrid_cta_crypto_portfolio_testing_0529_2234	每笔最大亏损：	-22,459.7
FutureGrid_cta_crypto_portfolio_testing_0529_2234	净值最大回撤: 	-80,493.55
FutureGrid_cta_crypto_portfolio_testing_0529_2234	净值最大回撤率: 	55.65
FutureGrid_cta_crypto_portfolio_testing_0529_2234	净值最大回撤时间：	2020/04/01
FutureGrid_cta_crypto_portfolio_testing_0529_2234	胜率：	46.64
FutureGrid_cta_crypto_portfolio_testing_0529_2234	盈利交易平均值	888.6
FutureGrid_cta_crypto_portfolio_testing_0529_2234	亏损交易平均值	-631.75
FutureGrid_cta_crypto_portfolio_testing_0529_2234	盈亏比：	1.41
FutureGrid_cta_crypto_portfolio_testing_0529_2234	最大资金占比：	20.41
FutureGrid_cta_crypto_portfolio_testing_0529_2234	平均每笔盈利：	77.34
FutureGrid_cta_crypto_portfolio_testing_0529_2234	平均每笔滑点成本：	0.0
FutureGrid_cta_crypto_portfolio_testing_0529_2234	平均每笔佣金：	7.7
FutureGrid_cta_crypto_portfolio_testing_0529_2234	Sharpe Ratio：	1.33
测试结束

Process finished with exit code 0



FutureGrid_cta_crypto_portfolio_testing_0529_0740	------------------------------
FutureGrid_cta_crypto_portfolio_testing_0529_0740	第一笔交易：	2019-09-11 21:02:00
FutureGrid_cta_crypto_portfolio_testing_0529_0740	最后一笔交易：	2021-05-08 14:33:00
FutureGrid_cta_crypto_portfolio_testing_0529_0740	总交易次数：	808
FutureGrid_cta_crypto_portfolio_testing_0529_0740	期初资金：	10,000
FutureGrid_cta_crypto_portfolio_testing_0529_0740	总盈亏：	242,137.39
FutureGrid_cta_crypto_portfolio_testing_0529_0740	资金最高净值：	540,575.47
FutureGrid_cta_crypto_portfolio_testing_0529_0740	资金最高净值时间：	2021/05/09
FutureGrid_cta_crypto_portfolio_testing_0529_0740	每笔最大盈利：	62,428.02
FutureGrid_cta_crypto_portfolio_testing_0529_0740	每笔最大亏损：	-66,209.38
FutureGrid_cta_crypto_portfolio_testing_0529_0740	净值最大回撤: 	-194,449.42
FutureGrid_cta_crypto_portfolio_testing_0529_0740	净值最大回撤率: 	71.33
FutureGrid_cta_crypto_portfolio_testing_0529_0740	净值最大回撤时间：	2020/04/21
FutureGrid_cta_crypto_portfolio_testing_0529_0740	胜率：	43.32
FutureGrid_cta_crypto_portfolio_testing_0529_0740	盈利交易平均值	4,159.44
FutureGrid_cta_crypto_portfolio_testing_0529_0740	亏损交易平均值	-2,649.92
FutureGrid_cta_crypto_portfolio_testing_0529_0740	盈亏比：	1.57
FutureGrid_cta_crypto_portfolio_testing_0529_0740	最大资金占比：	33.58
FutureGrid_cta_crypto_portfolio_testing_0529_0740	平均每笔盈利：	299.67
FutureGrid_cta_crypto_portfolio_testing_0529_0740	平均每笔滑点成本：	0.0
FutureGrid_cta_crypto_portfolio_testing_0529_0740	平均每笔佣金：	55.63
FutureGrid_cta_crypto_portfolio_testing_0529_0740	Sharpe Ratio：	1.15
测试结束

Process finished with exit code 0

'''