# flake8: noqa

from vnpy.trader.util_monitor import OrderMonitor, TradeMonitor, PositionMonitor, AccountMonitor, LogMonitor
from vnpy.trader.util_pid import update_pid
from vnpy.app.account_recorder import AccountRecorderApp
from vnpy.app.rpc_service import RpcServiceApp
from vnpy.app.cta_crypto.base import EVENT_CTA_LOG
from vnpy.app.cta_crypto.engine import CtaEngine
from vnpy.app.cta_crypto import CtaCryptoApp
from vnpy.gateway.binancef import BinancefGateway
from vnpy.trader.utility import load_json, save_json
from vnpy.trader.engine import MainEngine
from vnpy.trader.setting import SETTINGS
from vnpy.event import EventEngine, EVENT_TIMER
import os
import sys
import multiprocessing
from time import sleep
from datetime import datetime, time
from logging import DEBUG

# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)
    print(f'append {ROOT_PATH} into sys.path')

# from vnpy.trader.util_wechat import send_wx_msg
# from vnpy.trader.util_dingding2 import dingding as send_wx_msg

SETTINGS["log.active"] = True
SETTINGS["log.level"] = DEBUG
SETTINGS["log.console"] = True
SETTINGS["log.file"] = True

gateway_name = 'binance01'
gw_setting = load_json(f'connect_{gateway_name}.json')


class DaemonService(object):

    def __init__(self):
        self.event_engine = EventEngine()
        self.g_count = 0
        self.last_dt = datetime.now()
        # 创建账号/持仓/委托/交易/日志记录
        # self.acc_monitor = AccountMonitor(self.event_engine)
        self.pos_monitor = PositionMonitor(self.event_engine)
        self.ord_monitor = OrderMonitor(self.event_engine)
        self.trd_monitor = TradeMonitor(self.event_engine)
        self.log_monitor = LogMonitor(self.event_engine)

        # 创建主引擎
        self.main_engine = MainEngine(self.event_engine)

        self.save_data_time = None
        self.save_snapshot_time = None
        self.report_time = None

        # 注册定时器，用于判断重连
        self.event_engine.register(EVENT_TIMER, self.on_timer)

    def on_timer(self, event):
        """定时器执行逻辑，每十秒执行一次"""
        # account = self.main_engine.get_account(f'{gateway_name}.{gateway_name}_USDT')
        # 60秒才执行一次检查
        self.g_count += 1
        if self.g_count <= 60:
            return

        # 强制写入一次gpid
        update_pid()

        self.g_count = 0
        dt = datetime.now()

        if dt.hour != self.last_dt.hour:
            self.last_dt = dt
            print(u'run_server.py checkpoint:{0}'.format(dt))
            self.main_engine.write_log(u'run_server.py checkpoint:{0}'.format(dt))

        # 定时发送净值
        if dt.hour in [8, 20]:
            if self.report_time != dt.hour:
                self.main_engine.write_log(f'开始计算净值并推送')
                self.report_time = dt.hour

                try:
                    report_setting = load_json('report_setting.json')
                    if report_setting:
                        report_recivers = report_setting.get('recivers', [])
                        init_balance = report_setting.get('init_balance', None)

                        account = self.main_engine.get_account(f'{gateway_name}.{gateway_name}_USDT')
                        if account and account.balance > 0:
                            if init_balance is None or account.balance == 0:
                                rate = 1
                                report_setting.update({'init_balance': account.balance})
                                save_json('report_setting.json', report_setting)
                            else:
                                rate = round(account.balance / init_balance, 3)

                            # send_wx_msg(
                            #     content=f"净值报告, 账号{gateway_name},"
                            #             f"{dt.strftime('%Y-%m-%d %H:%M:%S')},"
                            #             f"账号资金{round(account.balance, 2)}usdt，净值:{rate}",
                            #     uids=report_recivers)

                except Exception as ex:
                    self.main_engine.write_error('计算净值/推送异常{}'.format(str(ex)))

        # 定时保存策略内数据(2小时保存一次）
        if dt.hour % 2 == 0:
            if self.save_data_time != dt.hour:
                self.main_engine.write_log(u'保存策略内数据')
                self.save_data_time = dt.hour
                try:
                    self.main_engine.save_strategy_data('ALL')
                except Exception as ex:
                    self.main_engine.write_error('保存策略内数据异常')
        # 定时保存切片
        if dt.strftime('%H:%M') in ['02:32', '10:16', '11:31', '15:17', '23:01']:
            if self.save_snapshot_time != dt.strftime('%H:%M'):
                self.main_engine.write_log(u'保存策略内K线切片数据')
                self.save_snapshot_time = dt.strftime('%H:%M')
                try:
                    self.main_engine.save_strategy_snapshot('ALL')
                except Exception as ex:
                    self.main_engine.write_error('保存策略内数据异常')

    def start(self):
        """
        Running in the child process.
        """
        SETTINGS["log.file"] = True

        timer_count = 0

        # 远程调用服务
        rpc_server = self.main_engine.add_app(RpcServiceApp)
        ret, msg = rpc_server.start()
        if not ret:
            self.main_engine.write_log(f"RPC服务未能启动:{msg}")
            return
        else:
            self.main_engine.write_log(f'RPC服务已启动')

        update_pid()

        # 接入网关
        self.main_engine.add_gateway(BinancefGateway, gateway_name)
        self.main_engine.write_log(f"连接{gateway_name}接口")
        self.main_engine.connect(gw_setting, gateway_name)

        sleep(5)

        # 添加加密数字货币cta引擎
        cta_engine = self.main_engine.add_app(CtaCryptoApp)
        cta_engine.init_engine()

        # cta_engine.main_engine.save_strategy_data()
        # cta_engine.close()
        # sleep(20)
        # del cta_engine
        # cta_engine = self.main_engine.add_app(CtaCryptoApp)
        # cta_engine.init_engine()

        # 添加账号同步app
        # self.main_engine.add_app(AccountRecorderApp)

        self.main_engine.write_log("主引擎创建成功")

        while True:
            sleep(1)


if __name__ == "__main__":
    s = DaemonService()
    s.start()
