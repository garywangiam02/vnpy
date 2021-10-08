# flake8: noqa

import os,sys
# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..' ))
if VNPY_ROOT not in sys.path:
    sys.path.append(VNPY_ROOT)
    print(f'append {VNPY_ROOT} into sys.path')
from vnpy.trader.constant import Exchange, OrderType
from vnpy.trader.object import (
    SubscribeRequest, OrderRequest, Direction, Offset, CancelRequest
)
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_BAR,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_LOG,
)
from vnpy.event import EventEngine
from vnpy.gateway.eastmoney import EastmoneyGateway
import sys
import os
import traceback
from time import sleep

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    print(u'append {}'.format(vnpy_root))
    sys.path.append(vnpy_root)

# 这里放需要你连接的测试系统的相关信息
connect_setting = {
    "资金账号": "xxxx",
    "资金密码": "xxxx",
    "账号类型": "普通"     # 普通 信用
}


def test():
    """测试"""
    from qtpy import QtCore
    import sys

    def print_log(event):
        log = event.data
        print(f'{log.time}: {log.msg}\n')

    def print_event(event):
        data = event.data
        print(f'{data.__dict__}')

    app = QtCore.QCoreApplication(sys.argv)

    event_engine = EventEngine()
    event_engine.register(EVENT_LOG, print_log)
    event_engine.register(EVENT_TICK, print_event)
    event_engine.register(EVENT_BAR, print_event)
    event_engine.register(EVENT_ACCOUNT, print_event)
    event_engine.register(EVENT_ORDER, print_event)
    event_engine.register(EVENT_TRADE, print_event)
    event_engine.register(EVENT_POSITION, print_event)

    event_engine.start()

    gateway = EastmoneyGateway(event_engine)
    print(f'开始接入东财:{connect_setting}')
    gateway.connect(connect_setting)

    # # 订阅行情
    gateway.subscribe(SubscribeRequest(symbol='300059', exchange=Exchange.SZSE, is_bar=True))
    #
    # # 委托下单
    from datetime import datetime
    #
    # import urllib.parse
    # uuid = "Z%2fYO%2bXfSUeEB8ofsYitDlQ%3d%3d"
    # d_uuid = urllib.parse.unquote(uuid)
    # print(d_uuid)
    # e_uuid = urllib.parse.quote_plus(d_uuid)
    # print(e_uuid)
    # # e_uuid = urllib.parse.quote(d_uuid)
    # # print(e_uuid)
    # d_uuid =  urllib.parse.unquote(e_uuid)
    # print(d_uuid)
    # # st_inirUrl= "https://jy.xzsec.com/Search/Position"
    # # s = urllib.parse.quote_plus(st_inirUrl,encoding='utf-8')
    # # print(s)
    # #
    # orderid = gateway.send_order(OrderRequest(symbol='123010', exchange=Exchange.SZSE, direction=Direction.LONG, offset=Offset.OPEN, type=OrderType.LIMIT, price=103.001, volume=10))
    # print(f'委托订单编号:{orderid}')
    # sleep(10)
    # # #gateway.cancel_order(CancelRequest(orderid='20210723_510313',symbol='123010', exchange=Exchange.SZSE))
    # # gateway.cancel_order(CancelRequest(orderid='534155',symbol='123010', exchange=Exchange.SZSE))

    couter = 20

    sys.exit(app.exec_())


if __name__ == '__main__':

    try:
        test()
    except Exception as ex:
        print(u'异常:{},{}'.format(str(ex), traceback.format_exc()), file=sys.stderr)
    print('Finished')
