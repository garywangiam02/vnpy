# encoding: UTF-8

'''
本文件中包含的是遗传算法参数优化器的实现
华富资产 李来佳
'''


from typing import Callable, List
from itertools import product
from functools import lru_cache
from time import time
from datetime import  datetime
import multiprocessing
import random
import traceback
from copy import copy, deepcopy
from uuid import uuid1
import logging
import os
import numpy as np

# deap是遗传算法的实现工具
from deap import creator, base, tools, algorithms

from vnpy.app.cta_strategy_pro.portfolio_testing import PortfolioTestingEngine
from vnpy.trader.util_logger import setup_logger

class OptimizationSetting:
    """
    Setting for runnning optimization.
    设置参数优化
    """

    def __init__(self):
        """"""
        self.params = {}
        self.target_name = ""

    def add_parameter(
            self, name: str,
            start: float,
            end: float = None,
            step: float = None
    ):
        """添加int/float类型参数优化"""
        if not end and not step:
            self.params[name] = [start]
            return

        if start >= end:
            print("参数优化起始点必须小于终止点")
            return

        if step <= 0:
            print("参数优化步进必须大于0")
            return

        value = start
        value_list = []

        while value <= end:
            value_list.append(value)
            value += step

        # win: [20,22,24]
        self.params[name] = value_list

    def add_parameters(self,
                       name: str,
                       values: List = []):
        """添加参数清单"""
        self.params[name] = values

    def set_target(self, target_name: str):
        """"""
        self.target_name = target_name

    def generate_setting(self) -> List:
        """"""
        keys = self.params.keys()
        values = self.params.values()
        products = list(product(*values))

        settings = []
        for p in products:
            setting = dict(zip(keys, p))
            settings.append(setting)

        return settings

    def generate_setting_ga(self):
        """"""
        settings_ga = []  # [ [(key1:value1), (key2:value2)], [],,,]
        settings = self.generate_setting()
        for d in settings:
            # dict => [(key1:value1), (key2:value2)]
            param = [tuple(i) for i in d.items()]
            settings_ga.append(param)
        return settings_ga


class GeneticOptimize(object):
    """
    遗传算法优化器
    """
    def __init__(self):

        self.s = OptimizationSetting()  # 参数生成器
        self.be = PortfolioTestingEngine()  # (组合)回测引擎

        self.settings_for_ga = []  # 供遗传算法进行优化的参数列表
        self.settings_for_env = {}  # 供回测使用的参数配置，如合约、资金账号、回测时间等
        self.settings_for_strategies = {}  # cta_strategy_settings， dict格式：{ "strategy_instance_name": {策略配置}}

        self.target_names = []  # 回测结果目标值名称列表，如最大化收益回撤比，最大化夏普比率
        self.logger = None

    creator.create("FitnessMulti", base.Fitness, weights=(1.0, 1.0))
    creator.create("Individual", list, fitness=creator.FitnessMulti)

    def write_log(self,msg: str, level: int = logging.DEBUG):

        if self.logger:
            self.logger.log(msg=msg, level=level)

    def init_setting(self,
                     test_settings,
                     strategy_settings,
                     target_names):

        # 回测引擎所需的所有基本配置
        self.settings_for_env = deepcopy(test_settings)

        if not self.logger:
            logs_folder = os.path.abspath(os.path.join(os.getcwd(), 'log'))
            filename = os.path.abspath(os.path.join(logs_folder, '{}'.format(test_settings['name'])))
            self.logger = setup_logger(file_name=filename,
                                       name="go_{}".format(datetime.now().strftime("%Y%m%d_%H%M%S")),
                                       log_level=logging.DEBUG,
                                       backtesing=True)

        # 回测实例及配置
        self.settings_for_strategies = deepcopy(strategy_settings)

        # 优化的目标值清单
        self.target_names = deepcopy(target_names)

        return self.settings_for_env

    def add_parameter(self, name, start, end, step):
        """
        添加参数
        :param name: 参数名称
        :param start: 开始数字
        :param end: 结束数字
        :param step: 步进
        :return:
        """
        self.s.add_parameter(name, start, end, step)

    def add_parameters(self, name, values):
        """
        添加参数
        :param name: 参数名
        :param values: 参数值列表
        :return:
        """
        self.s.add_parameters(name, deepcopy(values))

    def generate_setting_for_ga(self):
        """
        产生遗传算法的参数列表
        :return:
        """
        settings = self.s.generate_setting()
        for d in settings:
            param = [tuple(i) for i in d.items()]
            self.settings_for_ga.append(param)
        return self.settings_for_ga

    def generate_parameter(self):
        """"""
        return random.choice(self.settings_for_ga)

    def mutArrayGroup(self, individual, indpb):
        size = len(individual)
        paralist = self.generate_parameter()
        for i in range(size):
            if random.random() < indpb:
                individual[i] = paralist[i]
        return individual,

    def object_func(self, strategy_avg):
        """
        遗传优化的目标执行函数
        :param strategy_avg:
        :return:
        """
        return self._object_func(tuple(strategy_avg))

    def optimize(self):
        """
        运行优化
        :return:
        """
        start = time()
        toolbox = base.Toolbox()

        # 使用多进程
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        toolbox.register("map", pool.map)
        # 初始化
        toolbox.register("individual", tools.initIterate, creator.Individual, self.generate_parameter)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)
        toolbox.register("mate", tools.cxTwoPoint)
        toolbox.register("mutate", self.mutArrayGroup, indpb=1)
        toolbox.register("evaluate", self.object_func)
        toolbox.register("select", tools.selNSGA2)

        MU = 16
        LAMBDA = 20
        POP = 20
        pop = toolbox.population(POP)
        CXPB, MUTPB, NGEN = 0.95, 0.05, 4
        hof = tools.ParetoFront()

        stats = tools.Statistics(lambda ind: ind.fitness.values)
        np.set_printoptions(suppress=True)
        stats.register("mean", np.mean, axis=0)
        stats.register("std", np.std, axis=0)
        stats.register("min", np.min, axis=0)
        stats.register("max", np.max, axis=0)

        self.write_log("开始运行遗传算法，每代族群总数：%s, 优良品种筛选个数：%s，迭代次数：%s，交叉概率：%s，突变概率：%s" % (POP, MU, NGEN, CXPB, MUTPB))
        algorithms.eaMuPlusLambda(pop, toolbox, MU, LAMBDA, CXPB, MUTPB, NGEN, stats, halloffame=hof, verbose=True)
        end = time()
        cost = int((end - start))

        self.write_log("遗传算法优化完成，耗时%s秒" % (cost))
        self.write_log("----------输出帕累托前沿解集,解集数量%s----------" % (len(hof)))
        # return hof
        for i in range(len(hof)):
            solution = hof[i]
            self.write_log(solution)


    @lru_cache(maxsize=1000000)
    def _object_func(self, strategy_avg):
        """
        使用了缓存，可以减少重复参数的运行
        :param strategy_avg: 选取的策略参数
        :return:
        """
        engine = self.be
        self.settings_for_env['name'] = self.settings_for_env['name'] + '_' + str(uuid1())
        engine.prepare_env(self.settings_for_env)
        # 选取参数 => dict 结构
        ga_setting = dict(strategy_avg)
        settings_for_strategies = deepcopy(self.settings_for_strategies)

        # 策略实例名
        for ins_name in list(settings_for_strategies.keys()):
            # 策略实例配置
            ins_config = settings_for_strategies[ins_name]
            # 策略实例的策略参数
            strategy_setting = ins_config['setting']
            # 候选参数值 => 更新 => 策略参数值
            for k, v in ga_setting.items():
                if k in strategy_setting:
                    strategy_setting.update({k: v})
            # 更新回测实例配置
            settings_for_strategies.update({ins_name: ins_config})

        try:
            # 运行（组合）回测
            engine.run_portfolio_test(settings_for_strategies)
            # 回测结果，保存
            result = engine.show_backtesting_result()

            # 保存策略得内部数据
            engine.save_strategy_data()

            # 根据target_names => (value1, value2)
            return tuple([round(result.get(k, 0), 2) for k in self.target_names])

        except Exception as ex:
            self.write_log('组合回测异常{}'.format(str(ex)))
            traceback.print_exc()
            engine.save_fail_to_mongo(f'回测异常{str(ex)}')
            return tuple([0 for k in self.target_names])


def run_go(test_setting: dict, strategy_setting: dict, ga_setting: dict, target_names: List):
    """
    遗传算法优化+回测
    : test_setting, 组合回测所需的配置，包括合约信息，数据bar信息，回测时间，资金等。
    ：strategy_setting, dict, 一个或多个策略配置
    : ga_setting,dict, 代优化参数的设定： name: (开始值，结束值，步进）, name: [value1, value2,value3,,,]

    : return 定义的
    """
    # 创建遗传优化器
    GO = GeneticOptimize()

    # 初始化 环境参数、策略参数，优化目标清单
    GO.init_setting(
        test_settings=test_setting,
        strategy_settings=strategy_setting,
        target_names=target_names
    )

    # 添加代优化的参数
    for k, v in ga_setting.items():
        if isinstance(v, tuple) and len(v) == 3:
            GO.add_parameter(k, v[0], v[1], v[2])

        if isinstance(v, list):
            GO.add_parameters(k, v)

    GO.generate_setting_for_ga()

    GO.generate_parameter()

    GO.optimize()
