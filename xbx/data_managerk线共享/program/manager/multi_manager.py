# -*- coding: utf-8 -*-
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler  # 后台定时器不能用时可以用阻塞版的
from manager.functions import *
from manager.utility import *
from config.config import *
from datetime import datetime

from icecream import ic
import glob

def Timestamp():
    return '%s |> ' % time.strftime("%Y-%m-%d %T")

# 定制输出格式
ic.configureOutput(prefix=Timestamp)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 6000)  # 最多显示数据的行数


class DataManagerFather():
    def __init__(self,exchange,needed_time_interval_list):
        self.exchange = exchange 
        self.time_interval_list = needed_time_interval_list
        self.son = []  # 存放所有子数据管理类
        self.scheduler = BackgroundScheduler()  # 创建定时器以定时执行任务
        # 通过配置文件创建子策略实例
        for time_interval in self.time_interval_list:
            exec("self.data_%s=DataManagerSon(time_interval,self.exchange)" % time_interval)  # 通过配置创建子类
            exec("self.son.append(self.data_%s)" % time_interval)  # 将子类保存在类变量son中
        for son in self.son:
            if son.time_interval.find('m') >= 0:  # 添加循环间隔是分钟的子类的定时任务
                self.scheduler.add_job(son.scheduler, trigger='cron', minute='*/' + son.time_interval.split('m')[0],
                                       misfire_grace_time=10, max_instances=3, id=son.name)
            elif son.time_interval.find('h') >= 0:  # 添加循环间隔是小时的子类的定时任务
                self.scheduler.add_job(son.scheduler, trigger='cron', hour='*/' + son.time_interval.split('h')[0],
                                       misfire_grace_time=1, max_instances=3, id=son.name)
            else:  # 注意暂时未判断按天的策略
                ic(son.name, '时间间隔格式错误，请修改')
                raise ValueError
        self.scheduler.add_job(self.clean_outrange_data, trigger='cron', hour='12')
        self.scheduler.start()  # 定时器开始工作


    def clean_outrange_data():
        '''
        清理过期文件
        '''
        flag_file_list = glob.glob(flag_path_root+'/*.flag')
        for file in flag_file_list:
            today = time.strftime("%Y-%m-%d", time.localtime()) 
            if today not in file:
                os.remove(file)


class DataManagerSon():

    def __init__(self, time_interval, exchange):
        self.exchange = exchange  # 从母类继承交易所实例
        self.time_interval = time_interval
        self.name = 'data_'+time_interval  # 子类名称，用于区分
        self.re_download_all_his_coin_data = True # 下载全币种历史数据标志
        

    def scheduler(self):  # 供定时器调用
        exchange_info = robust(self.exchange.fapiPublic_get_exchangeinfo, )  
        _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
        _symbol_list = [x['symbol'] for x in exchange_info['symbols'] if x['status'] == 'TRADING']  # 过滤出交易状态正常的币种
        _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # 过滤usdt合约
        symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名单

        run_time = datetime.now().replace(second=0, microsecond=0)  #aps调度 ,误差基本是毫秒级

        if self.re_download_all_his_coin_data:
            symbol_candle_data = get_binance_history_candle_data(self.exchange, symbol_list, self.time_interval, run_time,
                                                                 MAX_KEEP_LEN)
            for symbol in symbol_list:
                symbol_candle_data[symbol].to_feather(f'{data_path_root}/{symbol}-{self.time_interval}.pkl')
            self.re_download_all_his_coin_data = False
        
        # =====防止数据越积越多
        old_symbol_candle_data = {}  # {"symbol":"data_df"}
        for symbol in symbol_list:
            if not os.path.exists(f'{data_path_root}/{symbol}-{self.time_interval}.pkl'):
                # 新上线的币
                symbol_old_data = get_data(symbol, self.exchange, MAX_KEEP_LEN, self.time_interval, run_time)
                symbol_old_data[symbol].to_feather(f'{data_path_root}/{symbol}-{self.time_interval}.pkl')
                old_symbol_candle_data[symbol] = symbol_old_data[symbol] 

            df = pd.read_feather(
                f'{data_path_root}/{symbol}-{self.time_interval}.pkl')
            if len(df) > MAX_KEEP_LEN:
                df = df[-MAX_KEEP_LEN:]
            old_symbol_candle_data[symbol] = df

        # =====并行获取所有币种的1小时K线,增量更新10条数据
        symbol_candle_data = get_binance_history_candle_data(self.exchange, symbol_list, self.time_interval, run_time, 10)
        ic('数据实时更新完毕',(datetime.now()-run_time).seconds,symbol_candle_data['BTCUSDT'].tail(2))
        for symbol in symbol_list:
            df = old_symbol_candle_data[symbol]
            df = df.append(symbol_candle_data[symbol])
            df.sort_values(by=['candle_begin_time'], inplace=True)
            df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
            df.reset_index(inplace=True, drop=True)
            df.to_feather(f'{data_path_root}/{symbol}-{self.time_interval}.pkl')
        # 创建flag文件，文件存在代表runtime时间点的全币种数据已下载完成
        with open(f'{flag_path_root}/{run_time}-{self.time_interval}.flag', 'w'):
            pass
