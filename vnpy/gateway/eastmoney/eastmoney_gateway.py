"""
Gateway for east stock Exchange(website).
"""
import os
import urllib
import hashlib
import hmac
import time
import json
from copy import copy, deepcopy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Dict, List
import traceback

from vnpy.api.rest import RestClient, Request
from vnpy.api.eastmoney_api.eastmoney import EastMoneyBackend
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)

from vnpy.trader.gateway import BaseGateway, LocalOrderManager
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.utility import print_dict, get_stock_exchange, extract_vt_symbol, load_json, save_json
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event, EventEngine
from vnpy.trader.util_wechat import send_wx_msg

# 通达信股票行情
STOCK_CONFIG_FILE = 'tdx_stock_config.pkb2'
from vnpy.data.tdx.tdx_common import get_cache_config, get_tdx_market_code

TRADE_REST_HOST: str = "https://jy.xzsec.com"
MARKET_REST_HOST: str = "https://hsmarket.eastmoney.com"
KEEP_COOKIES = ['uid', 'uuid', 'khmc', 'eastmoney_tzxq_zjzh', 'mobileimei']
STATUS_STOCK2VT: Dict[str, Status] = {
    "已报": Status.NOTTRADED,
    "部成": Status.PARTTRADED,
    "已成": Status.ALLTRADED,
    "已撤": Status.CANCELLED,
    "拒单": Status.REJECTED
}

ORDERTYPE_VT2STOCK: Dict[OrderType, str] = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET",
    OrderType.STOP: "STOP_MARKET"
}

ORDERTYPE_STOCK2VT: Dict[str, str] = {
    f'{Direction.LONG}_{Offset.OPEN}_{OrderType.LIMIT}': 'B',
    f'{Direction.LONG}_{Offset.OPEN}_{OrderType.MARKET}': '0a',  # 市价买入
    # "0b":"本方最优价",
    f'{Direction.LONG}_{Offset.OPEN}_{OrderType.FAK}': '0d',  # 最优五档剩余撤销
    f'{Direction.LONG}_{Offset.OPEN}_{OrderType.FOK}': '0e',  # 全额成交或撤销
    f'{Direction.SHORT}_{Offset.CLOSE}_{OrderType.LIMIT}': 'S',
    f'{Direction.SHORT}_{Offset.CLOSE}_{OrderType.MARKET}': '0f',  # 市价卖出
    f'{Direction.SHORT}_{Offset.CLOSE}_{OrderType.FAK}': '0i',  # 最优五档剩余撤销
}

INTERVAL_VT2STOCK: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

STOCKTYPE_DICT: Dict[str, str] = {
    "1": "股票",
    "2": "债券"
}


class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class EastmoneyGateway(BaseGateway):
    """
    VN Trader Gateway for Eastmoney connection.
    普通账号/信用账号
    """

    default_setting = {
        "资金账号": "",
        "资金密码": "",
        "账号类型": ["普通", "信用"],
        "并发连接数": 3,
        "session缓存文件": "session.json"
    }

    exchanges: Exchange = [Exchange.SZSE, Exchange.SSE]

    def __init__(self, event_engine: EventEngine, gateway_name="EASTMONEY"):
        """Constructor"""
        super().__init__(event_engine, gateway_name)
        self.count = 0
        # 本地订单号 <=> 服务器订单号标识符
        self.order_manager = LocalOrderManager(self, datetime.now().strftime('%Y%m%d'), 4)

        self.contracts = {}
        self.symbol_name_map: Dict[str, str] = {}
        self.symbol_exchange_map: Dict[str, Exchange] = {}  # 股票代码与vn交易所的字典

        # 账号类型
        self.account_type = "普通"  # "信用"
        self.md_api = MarketApi(self)
        self.td_api = None
        self.setting = {}

    def connect(self, setting: dict) -> None:
        """"""

        accountid = setting['资金账号']
        password = setting['资金密码']
        self.account_type = setting.get('账号类型', "普通")
        if self.account_type == '普通':
            if not self.td_api:
                self.td_api = NormalTradeApi(self)
        else:
            if not self.td_api:
                self.td_api = MarginTradeApi(self)
        accountid = f'{accountid}[{self.account_type}]'
        session_file = setting.get('session缓存文件', "session.json")
        validatekey = ""
        cookie_str = ""

        session_login = False
        # 优先尝试使用session配置进行登录验证
        if len(session_file) > 0 and os.path.exists(session_file):
            try:
                session_data = load_json(session_file, auto_save=False)
                validatekey = session_data.get("validatekey", "")
                cookie_str = session_data.get('cookie_str', "")

                if len(validatekey) > 0 and len(cookie_str) > 0:
                    ret = self.td_api.connect(accountid, validatekey, cookie_str)
                    if ret:
                        self.write_log(f'使用session配置登录成功')
                        session_login = True
            except Exception as ex:
                self.write_log(f'尝试session缓存登录，失败')
                session_login = False

        if not session_login:
            # session文件不存在，或者已经失效，需要重新通过账号密码登录
            api = EastMoneyBackend()
            import asyncio
            loop = asyncio.get_event_loop()
            task = api.login(accountid, password, max_retries=50)
            result = loop.run_until_complete(task)
            if not result:
                msg = f'{self.gateway_name} web登录失败'
                send_wx_msg(content=msg, )
                self.write_error(msg)
                self.event_engine.register(EVENT_TIMER, self.process_timer_event)
                return

            validatekey = api.validatekey
            cookie_str = api.cookies

            session_data = {
                "validatekey": api.validatekey,
                "cookie_str": api.cookies
            }
            # 保存session文件
            save_json(session_file, session_data)

            self.td_api.connect(accountid, validatekey, cookie_str)

        pool_connect = setting.get('并发连接数', 3)
        self.md_api.connect(pool_connect)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

        if len(self.setting) == 0:
            self.setting = deepcopy(setting)

    def reconnect(self):
        """
        重新连接
        :return:
        """
        self.write_log(f'重新连接')
        self.connect(self.setting)

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> Request:
        """"""
        return self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """"""
        pass

    def query_position(self) -> None:
        """"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """"""
        return self.td_api.query_history(req)

    def close(self) -> None:
        """"""
        if self.td_api:
            self.td_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """"""
        if self.status.get('td_con', False) \
                and self.status.get('tdws_con', False) \
                and self.status.get('mdws_con', False):
            self.status.update({'con': True})

        self.count += 1
        if self.count < 10:
            return
        self.count = 0
        if len(self.query_functions) > 0:
            func = self.query_functions.pop(0)
            func()
            self.query_functions.append(func)

        dt = datetime.now()
        if '0930' < dt.strftime('%H%M') < '1500' and dt.minute % 5 == 0:
            if self.td_api is not None and not self.td_api.validate_conn():
                self.write_log(f'启动重新连接')
                self.reconnect()

    def get_order(self, orderid: str):
        return self.td_api.get_order(orderid)


class NormalTradeApi(RestClient):
    """
    Eastmoney Trade REST API
    普通账号
    """

    def __init__(self, gateway: EastmoneyGateway):
        """"""
        super().__init__()

        self.gateway: EastmoneyGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.accountid: str = ""
        self.validatekey: str = ""  # ，每一个post url都需要添加这个validatekey
        self.cookies: dict = {}
        #     {"st_si": 51159162223318,
        #                      "st_pvi": 27796171418642,
        #                      "st_sp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        #                      "st_inirUrl" :"",
        #                     "st_sn": 1,
        #                     "st_psi": "{}000-11923323313501-8455476579".format(datetime.now().strftime("%Y%m%d%H%M%S")),
        #                     "st_asi": "delete",
        #                     "Yybdm": 5406,
        #                     "Uid": "Z/YO+XfSUeEB8ofsYitDlQ==",
        #                     "Khmc": "用户姓名",
        #                     "mobileimei": "38ad005a-7b7c-449b-8c98-d6d864fa2374",
        #                     "Uuid": "8d2111e366364f1e9f3f493630c788ea",
        #                     "eastmoney_txzq_zjzh":"NTQwNjYwMTMwNDcxfA=="
        # }
        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.holder_code = {}  # 交易所:股东代码
        self.orders = {}
        self.trades = {}

        self.cache_position_symbols = {}

        self.init(TRADE_REST_HOST)

    def sign(self, request: Request) -> Request:
        """
        Generate  signature.
        """
        security = request.data.pop("security", Security.NONE)
        if security == Security.NONE:
            request.data = None
            return request

        if security == Security.SIGNED:
            if request.params is None:
                request.params = {'validatekey': self.validatekey}
            elif 'validatekey' not in request.params:
                request.params.update({'validatekey': self.validatekey})

        if request.params:
            path = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path = request.path

        request.path = path
        request.params = {}
        # request.data = {}

        # Add headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "gw_reqtimestamp": str(int(datetime.now().timestamp() * 1000)),
            "Host": "jy.xzsec.com",
            "Orgin": "https://jy.xzsec.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36"
        }

        if len(self.cookies) > 0:
            # 更新cookie时间
            # self.cookies.update({"st_sp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            # self.cookies.update({"st_psi": "{}000-11923323313501-8455476579".format(datetime.now().strftime("%Y%m%d%H%M%S"))})
            # cookie => string
            new_cookie_str = self.get_cookie_str()

            # 更新headers的cookie
            headers.update({"Cookie": new_cookie_str})

        if request.headers:
            request.headers.update(headers)
        else:
            request.headers = headers
        if 'Submit' in path:
            print(request.headers)

        return request

    def extra_cookies(self, cookie_str):
        """
        string =》 cookies {}
        :param cookie_str:
        :return:
        """
        cookies = cookie_str.split(';')
        for cookie in cookies:
            if len(cookie) == 0:
                continue
            k, v = cookie.split('=')
            k = k.strip()
            v = v.strip()
            if k.lower() not in KEEP_COOKIES:
                v = urllib.parse.unquote(v)
            else:
                a = 1
            self.cookies.update({k: v})

    def get_cookie_str(self):
        """
        cookie => string
        :return:
        """
        s = ""
        for k, v in self.cookies.items():
            if len(s) > 0:
                s = s + '; '
            if k.lower() in KEEP_COOKIES:
                s = s + f'{k}={v}'
            else:
                if 'https' in v.lower():
                    s = s + '{}={}'.format(k, urllib.parse.quote_plus(v))
                else:
                    s = s + '{}={}'.format(k, urllib.parse.quote(v))
        return s

    def connect(
            self,
            accountid: str,
            validatekey: str,
            cookie_str: str
    ) -> None:
        """
        Initialize connection to REST server.
        """
        self.accountid = accountid
        self.validatekey = validatekey
        if len(cookie_str) > 0:
            self.extra_cookies(cookie_str)

        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        if not self.validate_conn():
            self.gateway.write_error(f'验证登录失败')
            return False

        self.start()

        self.gateway.write_log("REST API启动成功")
        self.gateway.status.update({'td_con': True, 'td_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        if self.gateway.status.get('md_con', False):
            self.gateway.status.update({'con': True})
        # self.query_time()
        self.query_account()
        # self.query_position()
        self.query_order()
        # self.query_contract()
        self.query_trade()

        # 添加到定时查询队列中
        self.gateway.query_functions = [self.query_account, self.query_order, self.query_trade]
        return True

    def validate_conn(self):
        """
        验证是否登录成功, 例如请求查询资金和持仓，如果返回非json的结果数据，就表示失败
        :return:
        """
        request = Request(
            method="POST",
            path="/Com/queryAssetAndPositionV1",
            data={"security": Security.SIGNED},
            params={},
            headers={}
        )
        request = self.sign(request)
        try:
            with self._get_session() as session:
                request = self.sign(request)
                url = self.make_full_url(request.path)

                # send request
                stream = request.stream
                method = request.method
                headers = request.headers
                params = request.params
                data = request.data
                response = session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    data=data,
                    proxies=self.proxies,
                    stream=stream,
                )
                request.response = response
                status_code = response.status_code
                if status_code // 100 == 2:  # 2xx codes are all successful
                    if status_code == 204:
                        json_body = None
                    else:
                        try:
                            json_body = response.json()
                            if json_body.get('Status') == 0:
                                return True
                            return False
                        except Exception as ex:
                            return False

        except Exception as ex:
            return False

        return False

    def query_account(self) -> Request:
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="POST",
            path="/Com/queryAssetAndPositionV1",
            callback=self.on_query_account,
            data=data
        )

    def query_position(self) -> Request:
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="POST",
            path="/Search/GetStockList",
            callback=self.on_query_position,
            data=data
        )

    def query_order(self) -> Request:
        """查询获取所有当日委托"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/Search/GetOrdersData",
            callback=self.on_query_order,
            headers={'Referer': 'https://jy.xzsec.com/Trade/Buy'},
            data=data
        )

    def query_trade(self) -> Request:
        """获取当日所有成交"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/Search/GetDealData",
            callback=self.on_query_trade,
            headers={'Referer': 'https://jy.xzsec.com/Trade/Buy'},
            data=data
        )

    def _new_order_id(self) -> int:
        """"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def get_order(self, orderid: str):
        """返回缓存的Order"""
        return self.orders.get(orderid, None)

    def send_order(self, req: OrderRequest) -> str:
        """发送委托"""
        # 创建本地orderid(str格式， HHMM+00序列号)
        local_orderid = self.gateway.order_manager.new_local_orderid()

        # 东财特殊的处理，强制小数点后两位，不支持类似转债类的小数点后三位报价
        # req.price = round(req.price, 2)

        # req => order
        order = req.create_order_data(orderid=local_orderid, gateway_name=self.gateway_name)

        order.accountid = self.accountid
        order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        order.datetime = datetime.now()

        # 构建request的data
        data = {
            "stockCode": req.symbol,
            "tradeType": ORDERTYPE_STOCK2VT[f'{req.direction}_{req.offset}_{req.type}'],
            "price": float(req.price),
            "amount": float(req.volume)
        }
        data.update({"security": Security.SIGNED})

        # 需要名称
        if req.offset == Offset.OPEN:
            zqmc = self.gateway.symbol_name_map.get(req.symbol, None)
            if zqmc is None:
                self.gateway.subscribe(SubscribeRequest(symbol=req.symbol, exchange=req.exchange))
                return ""
            data.update({"zqmc": zqmc})

        # 卖出时，需要股东代码
        if req.direction == Direction.SHORT:
            gddm = self.holder_code.get(req.exchange.value, None)
            if not gddm:
                self.gateway.write_error(f'找不到{req.symbol}{req.exchange}对应的股东代码')
                return ""

            data.update({"gddm": gddm})
            # 股东代码

        self.add_request(
            method="POST",
            path="/Trade/SubmitTradeV2",
            callback=self.on_send_order,
            headers={'Referer': 'https://jy.xzsec.com/Trade/Buy',
                     'Connection': 'keep-alive',
                     'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
                     'sec-ch-ua-mobile': '?0',
                     'Sec-Fetch-Site': 'same-origin',
                     'Sec-Fetch-Mode': 'cors',
                     'Sec-Fetch-Dest': 'empty',
                     'X-Requested-With': 'XMLHttpRequest'},
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> Request:
        """
        撤单
        :param req:
        :return:
        """
        # 东财撤单，提交的编码是 交易日_委托编号
        if '_' not in req.orderid:
            order = self.gateway.order_manager.get_order_with_local_orderid(req.orderid)
            if order is None:
                order = self.orders.get(req.orderid)
                # 交易日
                if order is None:
                    return False
            if order.datetime.hour >= 15:
                dt = order.datetime + timedelta(days=1)
            else:
                dt = order.datetime
            req_orderid = '{}_{}'.format(dt.strftime('%Y%m%d'), order.sys_orderid)
        else:
            req_orderid = req.orderid

        data = {
            "security": Security.SIGNED,
            "revokes": req_orderid
        }

        self.add_request(
            method="POST",
            path="/Trade/RevokeOrders",
            callback=self.on_cancel_order,
            headers={'Referer': 'https://jy.xzsec.com/Trade/Buy'},
            data=data,
            extra=req
        )
        return True

    def on_query_time(self, data: dict, request: Request) -> None:
        """"""
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

    def on_query_account(self, data: dict, request: Request) -> None:
        """
        查询账号和资产
            {"Status":0,"Count":0,"Data":
            [{"Djzj":"0.00","Dryk":"1277.65","Kqzj":"2244.45","Kyzj":"7894.68","Ljyk":"3909.39","Money_type":"RMB","RMBZzc":"173450.93","
            Zjye":"2244.45","Zxsz":"165556.15","Zzc":"173450.93",
            "positions":[
            {"Bz":"RMB","Cbjg":"98.732","Cbjgex":"98.732","Ckcb":"4936.60","Ckcbj":"98.732","Ckyk":"262.90","Cwbl":"0.02998","Djsl":"0","
            Dqcb":"4936.60","Dryk":"33.50","Drykbl":"0.006485","Gddm":"0183xxxx9","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","
            Khdm":"110200xxxx","Ksssl":"50","Kysl":"50","Ljyk":"262.90","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.053255",
            "Zjzh":"11020xxxxx7","Zqdm":"123010","Zqlx":"8","Zqlxmc":"8","Zqmc":"博世转债","Zqsl":"50","Ztmc":"0","Ztmr":"0",
            "Zxjg":"103.990","Zxsz":"5199.50"},
            {"Bz":"RMB","Cbjg":"98.667","Cbjgex":"98.667","Ckcb":"4933.34","Ckcbj":"98.667",            "Ckyk":"234.21","Cwbl":"0.02979","Djsl":"0","Dqcb":"4933.34","Dryk":"37.55",            "Drykbl":"0.007320","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0",            "Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50",            "Kysl":"50","Ljyk":"234.21","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","            Ykbl":"0.047473","Zjzh":"110200006197","Zqdm":"123023","Zqlx":"8","Zqlxmc":"8","            Zqmc":"迪森转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"103.351","Zxsz":"5167.55"},
            {"Bz":"RMB","Cbjg":"0.000","Cbjgex":"0.000","Ckcb":"-688.04","Ckcbj":"0.000","Ckyk":"688.04","Cwbl":"0.00000","Djsl":"0","Dqcb":"0.00","Dryk":"536.35","Drykbl":"0.104858","Gddm":"0183523549","Gfmcdj":"50","Gfmrjd":"0","Gfssmmce":"-50","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"0","Kysl":"0","Ljyk":"688.04","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.000000","Zjzh":"110200006197","Zqdm":"123028","Zqlx":"8","Zqlxmc":"8","Zqmc":"清水转债","Zqsl":"0","Ztmc":"0","Ztmr":"0","Zxjg":"109.990","Zxsz":"0.00"},
            {"Bz":"RMB","Cbjg":"99.088","Cbjgex":"99.088","Ckcb":"4954.39","Ckcbj":"99.088","Ckyk":"205.36","Cwbl":"0.02975","Djsl":"0","Dqcb":"4954.39","Dryk":"72.25","Drykbl":"0.014201","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"205.36","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.041448","Zjzh":"110200006197","Zqdm":"123076","Zqlx":"8","Zqlxmc":"8","Zqmc":"强力转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"103.195","Zxsz":"5159.75"},
            {"Bz":"RMB","Cbjg":"109.373","Cbjgex":"109.373","Ckcb":"5468.63","Ckcbj":"109.373","Ckyk":"165.37","Cwbl":"0.03248","Djsl":"0","Dqcb":"5468.63","Dryk":"21.20","Drykbl":"0.003777","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"165.37","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.030236","Zjzh":"110200006197","Zqdm":"123082","Zqlx":"8","Zqlxmc":"8","Zqmc":"北陆转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"112.680","Zxsz":"5634.00"},{"Bz":"RMB","Cbjg":"105.475","Cbjgex":"105.475","Ckcb":"5273.75","Ckcbj":"105.475","Ckyk":"241.25","Cwbl":"0.03180","Djsl":"0","Dqcb":"5273.75","Dryk":"25.50","Drykbl":"0.004645","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"241.25","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.045745","Zjzh":"110200006197","Zqdm":"123087","Zqlx":"8","Zqlxmc":"8","Zqmc":"明电转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"110.300","Zxsz":"5515.00"},{"Bz":"RMB","Cbjg":"109.868","Cbjgex":"109.868","Ckcb":"5493.39","Ckcbj":"109.868","Ckyk":"148.11","Cwbl":"0.03253","Djsl":"0","Dqcb":"5493.39","Dryk":"13.50","Drykbl":"0.002399","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"148.11","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.026960","Zjzh":"110200006197","Zqdm":"123096","Zqlx":"8","Zqlxmc":"8","Zqmc":"思创转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"112.830","Zxsz":"5641.50"},{"Bz":"RMB","Cbjg":"99.192","Cbjgex":"99.192","Ckcb":"4959.60","Ckcbj":"99.192","Ckyk":"280.40","Cwbl":"0.03021","Djsl":"0","Dqcb":"4959.60","Dryk":"35.50","Drykbl":"0.006821","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"280.40","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.056537","Zjzh":"110200006197","Zqdm":"127018","Zqlx":"8","Zqlxmc":"8","Zqmc":"本钢转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"104.800","Zxsz":"5240.00"},{"Bz":"RMB","Cbjg":"88.878","Cbjgex":"88.878","Ckcb":"4443.89","Ckcbj":"88.878","Ckyk":"160.06","Cwbl":"0.02654","Djsl":"0","Dqcb":"4443.89","Dryk":"-0.55","Drykbl":"-0.000119","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"160.06","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.036016","Zjzh":"110200006197","Zqdm":"127019","Zqlx":"8","Zqlxmc":"8","Zqmc":"国城转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"92.079","Zxsz":"4603.95"},{"Bz":"RMB","Cbjg":"103.155","Cbjgex":"105.751","Ckcb":"5157.74","Ckcbj":"103.155","Ckyk":"262.26","Cwbl":"0.03125","Djsl":"0","Dqcb":"5287.53","Dryk":"3.15","Drykbl":"0.000582","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"262.26","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.050846","Zjzh":"110200006197","Zqdm":"127028","Zqlx":"8","Zqlxmc":"8","Zqmc":"英特转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"108.400","Zxsz":"5420.00"},{"Bz":"RMB","Cbjg":"106.351","Cbjgex":"106.351","Ckcb":"5317.56","Ckcbj":"106.351","Ckyk":"28.94","Cwbl":"0.03082","Djsl":"0","Dqcb":"5317.56","Dryk":"66.45","Drykbl":"0.012585","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"28.94","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.005444","Zjzh":"110200006197","Zqdm":"127033","Zqlx":"8","Zqlxmc":"8","Zqmc":"中装转2","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"106.930","Zxsz":"5346.50"},{"Bz":"RMB","Cbjg":"107.349","Cbjgex":"107.349","Ckcb":"5367.47","Ckcbj":"107.349","Ckyk":"-24.97","Cwbl":"0.03080","Djsl":"0","Dqcb":"5367.47","Dryk":"29.95","Drykbl":"0.005638","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"-24.97","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"-0.004648","Zjzh":"110200006197","Zqdm":"127034","Zqlx":"8","Zqlxmc":"8","Zqmc":"绿茵转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"106.850","Zxsz":"5342.50"},{"Bz":"RMB","Cbjg":"105.932","Cbjgex":"105.932","Ckcb":"5296.61","Ckcbj":"105.932","Ckyk":"253.89","Cwbl":"0.03200","Djsl":"0","Dqcb":"5296.61","Dryk":"50.50","Drykbl":"0.009182","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"253.89","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.047936","Zjzh":"110200006197","Zqdm":"127035","Zqlx":"8","Zqlxmc":"8","Zqmc":"濮耐转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"111.010","Zxsz":"5550.50"},{"Bz":"RMB","Cbjg":"104.443","Cbjgex":"106.306","Ckcb":"5222.16","Ckcbj":"104.443","Ckyk":"132.59","Cwbl":"0.03087","Djsl":"0","Dqcb":"5315.31","Dryk":"9.20","Drykbl":"0.001721","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"132.59","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.025392","Zjzh":"110200006197","Zqdm":"128013","Zqlx":"8","Zqlxmc":"8","Zqmc":"洪涛转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"107.095","Zxsz":"5354.75"},{"Bz":"RMB","Cbjg":"105.544","Cbjgex":"105.544","Ckcb":"5277.20","Ckcbj":"105.544","Ckyk":"47.85","Cwbl":"0.03070","Djsl":"0","Dqcb":"5277.20","Dryk":"-2.25","Drykbl":"-0.000422","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"47.85","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.009067","Zjzh":"110200006197","Zqdm":"128034","Zqlx":"8","Zqlxmc":"8","Zqmc":"江银转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"106.501","Zxsz":"5325.05"},{"Bz":"RMB","Cbjg":"106.981","Cbjgex":"106.981","Ckcb":"5349.07","Ckcbj":"106.981","Ckyk":"24.33","Cwbl":"0.03098","Djsl":"0","Dqcb":"5349.07","Dryk":"-4.35","Drykbl":"-0.000809","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"24.33","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.004552","Zjzh":"110200006197","Zqdm":"128037","Zqlx":"8","Zqlxmc":"8","Zqmc":"岩土转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"107.468","Zxsz":"5373.40"},{"Bz":"RMB","Cbjg":"109.561","Cbjgex":"109.561","Ckcb":"5478.04","Ckcbj":"109.561","Ckyk":"13.41","Cwbl":"0.03166","Djsl":"0","Dqcb":"5478.04","Dryk":"15.40","Drykbl":"0.002812","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"13.41","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.002446","Zjzh":"110200006197","Zqdm":"128040","Zqlx":"8","Zqlxmc":"8","Zqmc":"华通转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"109.829","Zxsz":"5491.45"},{"Bz":"RMB","Cbjg":"104.206","Cbjgex":"104.206","Ckcb":"5210.29","Ckcbj":"104.206","Ckyk":"-52.79","Cwbl":"0.02973","Djsl":"0","Dqcb":"5210.29","Dryk":"11.90","Drykbl":"0.002313","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"-52.79","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"-0.010134","Zjzh":"110200006197","Zqdm":"128066","Zqlx":"8","Zqlxmc":"8","Zqmc":"亚泰转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"103.150","Zxsz":"5157.50"},{"Bz":"RMB","Cbjg":"109.350","Cbjgex":"109.350","Ckcb":"5467.49","Ckcbj":"109.350","Ckyk":"262.51","Cwbl":"0.03304","Djsl":"0","Dqcb":"5467.49","Dryk":"23.45","Drykbl":"0.004109","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"262.51","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.048011","Zjzh":"110200006197","Zqdm":"128081","Zqlx":"8","Zqlxmc":"8","Zqmc":"海亮转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"114.600","Zxsz":"5730.00"},{"Bz":"RMB","Cbjg":"109.515","Cbjgex":"109.515","Ckcb":"5475.74","Ckcbj":"109.515","Ckyk":"-23.14","Cwbl":"0.03144","Djsl":"0","Dqcb":"5475.74","Dryk":"22.80","Drykbl":"0.004199","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"-23.14","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"-0.004228","Zjzh":"110200006197","Zqdm":"128107","Zqlx":"8","Zqlxmc":"8","Zqmc":"交科转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"109.052","Zxsz":"5452.60"},{"Bz":"RMB","Cbjg":"98.620","Cbjgex":"98.620","Ckcb":"4930.99","Ckcbj":"98.620","Ckyk":"-30.99","Cwbl":"0.02825","Djsl":"0","Dqcb":"4930.99","Dryk":"3.50","Drykbl":"0.000715","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"-30.99","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"-0.006287","Zjzh":"110200006197","Zqdm":"128127","Zqlx":"8","Zqlxmc":"8","Zqmc":"文科转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"98.000","Zxsz":"4900.00"},{"Bz":"RMB","Cbjg":"107.812","Cbjgex":"107.812","Ckcb":"5390.58","Ckcbj":"107.812","Ckyk":"-54.78","Cwbl":"0.03076","Djsl":"0","Dqcb":"5390.58","Dryk":"-4.20","Drykbl":"-0.000787","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"-54.78","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"-0.010166","Zjzh":"110200006197","Zqdm":"128129","Zqlx":"8","Zqlxmc":"8","Zqmc":"青农转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"106.716","Zxsz":"5335.80"},{"Bz":"RMB","Cbjg":"99.010","Cbjgex":"99.010","Ckcb":"4950.50","Ckcbj":"99.010","Ckyk":"89.35","Cwbl":"0.02906","Djsl":"0","Dqcb":"4950.50","Dryk":"8.35","Drykbl":"0.001660","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"89.35","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.018049","Zjzh":"110200006197","Zqdm":"128139","Zqlx":"8","Zqlxmc":"8","Zqmc":"祥鑫转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"100.797","Zxsz":"5039.85"},{"Bz":"RMB","Cbjg":"100.000","Cbjgex":"100.000","Ckcb":"1000.00","Ckcbj":"100.000","Ckyk":"0.00","Cwbl":"0.00577","Djsl":"0","Dqcb":"1000.00","Dryk":"","Drykbl":"","Gddm":"0183523549","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"10","Jgbm":"1102","Khdm":"110200006561","Ksssl":"10","Kysl":"10","Ljyk":"0.00","Market":"SA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.000000","Zjzh":"110200006197","Zqdm":"370601","Zqlx":"8","Zqlxmc":"8","Zqmc":"康泰发债","Zqsl":"10","Ztmc":"0","Ztmr":"0","Zxjg":"100.000","Zxsz":"1000.00"},{"Bz":"RMB","Cbjg":"106.549","Cbjgex":"106.549","Ckcb":"5327.46","Ckcbj":"106.549","Ckyk":"16.54","Cwbl":"0.03081","Djsl":"0","Dqcb":"5327.46","Dryk":"28.00","Drykbl":"0.005267","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"16.54","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.003107","Zjzh":"110200006197","Zqdm":"110041","Zqlx":"8","Zqlxmc":"8","Zqmc":"蒙电转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"106.880","Zxsz":"5344.00"},{"Bz":"RMB","Cbjg":"110.590","Cbjgex":"110.590","Ckcb":"5529.50","Ckcbj":"110.590","Ckyk":"259.00","Cwbl":"0.03337","Djsl":"0","Dqcb":"5529.50","Dryk":"117.00","Drykbl":"0.020629","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"259.00","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.046840","Zjzh":"110200006197","Zqdm":"110060","Zqlx":"8","Zqlxmc":"8","Zqmc":"天路转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"115.770","Zxsz":"5788.50"},{"Bz":"RMB","Cbjg":"105.881","Cbjgex":"105.881","Ckcb":"5294.06","Ckcbj":"105.881","Ckyk":"25.44","Cwbl":"0.03067","Djsl":"0","Dqcb":"5294.06","Dryk":"38.50","Drykbl":"0.007290","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"25.44","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.004807","Zjzh":"110200006197","Zqdm":"113505","Zqlx":"8","Zqlxmc":"8","Zqmc":"杭电转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"106.390","Zxsz":"5319.50"},{"Bz":"RMB","Cbjg":"109.790","Cbjgex":"109.790","Ckcb":"5489.49","Ckcbj":"109.790","Ckyk":"53.01","Cwbl":"0.03195","Djsl":"0","Dqcb":"5489.49","Dryk":"40.00","Drykbl":"0.007269","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"53.01","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.009655","Zjzh":"110200006197","Zqdm":"113567","Zqlx":"8","Zqlxmc":"8","Zqmc":"君禾转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"110.850","Zxsz":"5542.50"},{"Bz":"RMB","Cbjg":"97.059","Cbjgex":"97.059","Ckcb":"4852.97","Ckcbj":"97.059","Ckyk":"66.53","Cwbl":"0.02836","Djsl":"0","Dqcb":"4852.97","Dryk":"-15.00","Drykbl":"-0.003040","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"66.53","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.013713","Zjzh":"110200006197","Zqdm":"113569","Zqlx":"8","Zqlxmc":"8","Zqmc":"科达转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"98.390","Zxsz":"4919.50"},{"Bz":"RMB","Cbjg":"94.509","Cbjgex":"94.509","Ckcb":"4725.45","Ckcbj":"94.509","Ckyk":"12.05","Cwbl":"0.02731","Djsl":"0","Dqcb":"4725.45","Dryk":"11.50","Drykbl":"0.002433","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"12.05","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.002550","Zjzh":"110200006197","Zqdm":"113589","Zqlx":"8","Zqlxmc":"8","Zqmc":"天创转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"94.750","Zxsz":"4737.50"},{"Bz":"RMB","Cbjg":"104.809","Cbjgex":"104.809","Ckcb":"5240.44","Ckcbj":"104.809","Ckyk":"13.56","Cwbl":"0.03029","Djsl":"0","Dqcb":"5240.44","Dryk":"16.50","Drykbl":"0.003150","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"13.56","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.002586","Zjzh":"110200006197","Zqdm":"113591","Zqlx":"8","Zqlxmc":"8","Zqmc":"胜达转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"105.080","Zxsz":"5254.00"},{"Bz":"RMB","Cbjg":"91.048","Cbjgex":"91.048","Ckcb":"4552.41","Ckcbj":"91.048","Ckyk":"42.59","Cwbl":"0.02649","Djsl":"0","Dqcb":"4552.41","Dryk":"2.00","Drykbl":"0.000435","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"42.59","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.009358","Zjzh":"110200006197","Zqdm":"113596","Zqlx":"8","Zqlxmc":"8","Zqmc":"城地转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"91.900","Zxsz":"4595.00"},{"Bz":"RMB","Cbjg":"99.360","Cbjgex":"99.360","Ckcb":"4967.99","Ckcbj":"99.360","Ckyk":"106.51","Cwbl":"0.02926","Djsl":"0","Dqcb":"4967.99","Dryk":"30.50","Drykbl":"0.006047","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"50","Jgbm":"1102","Khdm":"110200006561","Ksssl":"50","Kysl":"50","Ljyk":"106.51","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.021437","Zjzh":"110200006197","Zqdm":"113601","Zqlx":"8","Zqlxmc":"8","Zqmc":"塞力转债","Zqsl":"50","Ztmc":"0","Ztmr":"0","Zxjg":"101.490","Zxsz":"5074.50"},{"Bz":"RMB","Cbjg":"100.000","Cbjgex":"100.000","Ckcb":"1000.00","Ckcbj":"100.000","Ckyk":"0.00","Cwbl":"0.00577","Djsl":"0","Dqcb":"1000.00","Dryk":"","Drykbl":"","Gddm":"A625856709","Gfmcdj":"0","Gfmrjd":"0","Gfssmmce":"0","Gfye":"10","Jgbm":"1102","Khdm":"110200006561","Ksssl":"10","Kysl":"10","Ljyk":"0.00","Market":"HA","Mrssc":"0","Sssl":"0","Szjsbs":"1","Ykbl":"0.000000","Zjzh":"110200006197","Zqdm":"783016","Zqlx":"8","Zqlxmc":"8","Zqmc":"节能发债","Zqsl":"10","Ztmc":"0","Ztmr":"0","Zxjg":"100.000","Zxsz":"1000.00"}]}],"Errcode":0}
        """
        if not isinstance(data, dict):
            self.gateway.write_error(f'不是dict结构')
            self.gateway.write_error(print_dict(data))
            return

        assets = data.get('Data', [])
        for asset in assets:
            # self.gateway.write_log(print_dict(asset))
            if asset['Money_type'] != "RMB":
                continue
            if not self.accountid:
                self.accountid = f"{self.gateway_name}_{asset['asset']}"
            balance_str = asset["RMBZzc"]
            if len(balance_str) > 0:
                balance = float(balance_str)
            else:
                balance = 0
            frozen_str = asset["Djzj"]
            if len(frozen_str) > 0:
                frozen = float(frozen_str)
            else:
                frozen = 0
            holding_profit_str = asset['Ljyk']
            if len(holding_profit_str) > 0:
                holding_profit = float(holding_profit_str)
            else:
                holding_profit = 0
            close_profit_str = asset['Dryk']
            if len(close_profit_str) > 0:
                close_profit = float(close_profit_str)
            else:
                close_profit = 0

            account = AccountData(
                accountid=self.accountid,
                balance=balance,
                frozen=frozen,
                holding_profit=holding_profit,
                close_profit=close_profit,
                currency='RMB',
                margin=0,
                gateway_name=self.gateway_name,
                trading_day=datetime.now().strftime('%Y-%m-%d')
            )
            account.available = float(asset['Kyzj']) if len(asset['Kyzj']) > 0 else 0

            if account.balance:
                self.gateway.on_account(account)

            # 临时缓存合约的配置信息
            for position in asset["positions"]:
                # 证券代码
                symbol = position.get('Zqdm')
                # 交易市场
                market = position.get('Market')
                if market == 'SA':
                    exchange = Exchange.SZSE
                elif market == 'HA':
                    exchange = Exchange.SSE
                else:
                    exchange = Exchange.LOCAL

                # 设置交易所 <=>股东代码
                if self.holder_code.get(exchange.value, None) is None:
                    self.holder_code.update({exchange.value: position.get('Gddm')})

                if symbol:
                    position = PositionData(
                        accountid=self.accountid,
                        symbol=symbol,
                        name=position['Zqmc'],
                        exchange=exchange,
                        direction=Direction.NET,
                        volume=int(position['Zqsl']),
                        yd_volume=int(position['Kysl']),
                        price=float(position['Cbjg']),
                        cur_price=float(position["Zxjg"]),
                        pnl=float(position["Ljyk"]),
                        gateway_name=self.gateway_name,
                    )
                    self.gateway.on_position(position)
                    if position.symbol not in self.gateway.symbol_name_map:
                        self.gateway.symbol_name_map[position.symbol] = position.name

                    if position.symbol not in self.gateway.symbol_exchange_map:
                        self.gateway.symbol_exchange_map[position.symbol] = position.exchange

            self.gateway.write_log("账户资金查询成功")

    def on_query_position(self, data: dict, request: Request) -> None:
        """"""
        pass

    def on_query_order(self, data: dict, request: Request) -> None:
        """
        响应当日所有委托的查询
        :param data:
        :param request:
        :return:
        """
        # {"Message":null,"Status":0,"Data":
        # [{"Wtsj":"163618","Zqdm":"123010","Zqmc":"博世转债","Mmsm":"证券买入","Mmlb":"B","Wtsl":"10","Wtzt":"未报","Wtjg":"101.160","Cjsl":"0","Cjje":".00","Cjjg":"0.000000","Market":"SA","Wtbh":"196","Gddm":"xxxx","Dwc":"","Qqhs":null,"Wtrq":"20210723","Wtph":"196","Khdm":"xxx","Khxm":"用户姓名","Zjzh":"xxx","Jgbm":"5406","Bpsj":"000000","Cpbm":"","Cpmc":"","Djje":"1011.80","Cdsl":"0","Jyxw":"009535","Cdbs":"F","Czrq":"20210722","Wtqd":"9","Bzxx":"夜市委托不报盘","Sbhtxh":"XXXXX","Mmlb_ex":"B","Mmlb_bs":"B"},
        # {"Wtsj":"091840","Zqdm":"799999","Zqmc":"登记指定","Mmsm":"指定交易","Mmlb":"O","Wtsl":"1","Wtzt":"已成","Wtjg":"1.000","Cjsl":"0","Cjje":".00","Cjjg":"0.000000","Market":"HA","Wtbh":"22279","Gddm":"xxxxx","Dwc":"20210722|22279","Qqhs":null,"Wtrq":"20210722","Wtph":"22279","Khdm":"xxx","Khxm":"用户姓名","xxx":"xxx","Jgbm":"5406","Bpsj":"091840","Cpbm":"","Cpmc":"","Djje":".00","Cdsl":"0","Jyxw":"46110","Cdbs":"F","Czrq":"20210722","Wtqd":"9","Bzxx":"0","Sbhtxh":"XXXXX","Mmlb_ex":"U","Mmlb_bs":"U"}]}
        orders = data.get('Data', [])
        for d in orders:
            # 系统委托编号
            sys_orderid = d.get('Wtbh')
            # 委托状态
            wtzt = d.get('Wtzt')
            status = STATUS_STOCK2VT.get(wtzt, Status.UNKNOWN)
            # 成交数量
            traded_volume = int(d.get('Cjsl', 0))

            # 检查是否存在本地order_manager缓存中
            local_order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)
            # 比对状态和成交数量
            if local_order is not None and local_order.status == status and local_order.traded == traded_volume:
                continue

            # 时间
            dt_str = d.get('Czrq', "") + d.get('Wtsj')
            if len(dt_str) == 0:
                continue
            order_time = datetime.strptime(dt_str, '%Y%m%d%H%M%S')

            # 证券代码
            symbol = d.get('Zqdm', None)
            # 市场
            market = d.get('Market')
            if market == 'SA':
                exchange = Exchange.SZSE
            else:
                exchange = Exchange.SSE
            # 更新 证券代码 <=> 市场
            if symbol not in self.gateway.symbol_exchange_map:
                self.gateway.symbol_exchange_map[symbol] = exchange

            # 委托价格
            price = float(d.get('Wtjg', 0))
            # 委托数量
            volume = int(d.get('Wtsl', 0))
            # 买卖类别
            mmlb = d.get('Mmlb')
            if mmlb == 'B':
                direction = Direction.LONG
                offset = Offset.OPEN
            else:  # 'S
                direction = Direction.SHORT
                offset = Offset.CLOSE

            order = OrderData(
                accountid=self.accountid,
                orderid=sys_orderid if not local_order else local_order.orderid,
                sys_orderid=sys_orderid,
                symbol=symbol,
                exchange=exchange,
                name=d.get('Zqmc', ""),
                price=price,
                volume=volume,
                type=OrderType.LIMIT,
                direction=direction,
                offset=offset,
                traded=traded_volume,
                status=status,
                datetime=order_time,
                time=d.get('Wtsj'),
                gateway_name=self.gateway_name,
            )
            # 更新 sys_order
            self.orders.update({order.orderid: copy(order)})

            # 更新本地order
            self.gateway.order_manager.on_order(copy(order))

            # 如果本地不存在映射，建立映射关系
            if local_order is None:
                self.gateway.order_manager.update_orderid_map(local_orderid=order.orderid,
                                                              sys_orderid=order.sys_orderid)
                self.gateway.write_log(f'本地order不存在，添加{order.orderid} <=> {order.sys_orderid}')

        self.gateway.write_log("委托信息查询成功")

    def on_query_trade(self, data: dict, request: Request) -> None:
        """
        今日成交清单
        :param data:
        :param request:
        :return:
        """
        # {"Message":null,"Status":0,"Data":[{"Cjsj":"101240","Zqdm":"123010","Zqmc":"博世转债","Cjsj2":"101240",
        # "Mmsm":"证券买入","Cjjg":"105.320","Cjsl":"10","Cjje":"1053.20","Cjbh":"xxxx","Market":"SA",
        # "Wtbh":"218449","Gddm":"xxxxx","Dwc":"20210723|75","Qqhs":null,"Cjrq":"20210723","Htxh":null,"Cpbm":"","Cpmc":"","Cjlx":"0","Wtsl":"10",
        # "Wtjg":"105.320","Sbhtxh":"xxxx","Zqyjlx":".00000000","Mmlb":"B","Mmlb_ex":"B","Mmlb_bs":"B"}]}

        trades = data.get('Data', [])
        for d in trades:

            # 时间
            dt_str = d.get('Cjrq', "") + d.get('Cjsj')
            if len(dt_str) == 0:
                continue
            trade_dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
            # 成交编号
            tradeid = d.get('Cjbh')
            if tradeid in self.trades:
                continue
            sys_orderid = d.get('Wtbh')
            local_orderid = self.gateway.order_manager.get_local_orderid(sys_orderid)

            # 创建trade对象
            trade = TradeData(
                accountid=self.accountid,
                symbol=d['Zqdm'],
                exchange=Exchange.SZSE if d['Market'] == 'SA' else Exchange.SSE,
                name=d.get('Zqmc', ""),
                orderid=local_orderid,
                tradeid=tradeid,
                direction=Direction.SHORT if d['Mmlb'] == 'S' else Direction.LONG,
                offset=Offset.CLOSE if d['Mmlb'] == 'S' else Offset.OPEN,
                price=float(d["Cjjg"]),
                volume=float(d['Cjsl']),
                time=d.get('Cjsj'),
                datetime=trade_dt,
                gateway_name=self.gateway_name
            )
            # 更新本地字典
            self.trades.update({trade.tradeid: copy(trade)})
            # 推送事件
            self.gateway.on_trade(trade)

        self.gateway.write_log("成交信息查询成功")

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托回报"""
        # <class 'dict'>: {'Status': 0, 'Count': 1, 'Data': [{'Wtbh': '534155'}], 'Errcode': 0}
        self.gateway.write_log(f'委托返回:{print_dict(data)}')
        if data.get('Status', -1) != 0:
            self.gateway.write_error(f'委托失败,{data}')
            order = request.extra
            order.status = Status.REJECTED
            self.orders.update({order.orderid: copy(order)})
            self.gateway.write_log(f'订单委托失败:{order.__dict__}')
            if not order.accountid:
                order.accountid = self.accountid
                order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
            if not order.datetime:
                order.datetime = datetime.now()
            self.gateway.order_manager.on_order(order)
            return

        result = data.get('Data', [])
        if len(result) == 0:
            self.gateway.write_error(f'委托数据没有:{data}')
            return

        d = result[0]
        sys_orderid = d.get('Wtbh')
        if not sys_orderid:
            self.gateway.write_error(f'委托返回中没有委托编号:{data}')
            return

        # 获取提交的order
        order = request.extra
        if not order:
            self.gateway.write_error(f'无法从request中获取提交的order')
            return

        # 更新本地orderid 与 sys_order的绑定关系
        local_orderid = order.orderid
        if local_orderid and sys_orderid:
            self.gateway.order_manager.update_orderid_map(local_orderid=local_orderid, sys_orderid=sys_orderid)

        # 推送委托更新消息
        order.sys_orderid = sys_orderid
        order.status = Status.NOTTRADED
        self.gateway.order_manager.on_order(copy(order))

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'订单委托失败:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_error(msg)

    def on_send_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'发送订单异常:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，拒单"
        self.gateway.write_error(msg)
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """"""
        self.gateway.write_log(data)

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """"""
        history = []
        limit = 1000
        start_time = int(datetime.timestamp(req.start))

        while True:
            # Create query params
            params = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2STOCK[req.interval],
                "limit": limit,
                "startTime": start_time * 1000,  # convert to millisecond
            }

            # Add end time if specified
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000  # convert to millisecond

            # Get response from server
            resp = self.request(
                "GET",
                "/fapi/v1/klines",
                data={"security": Security.NONE},
                params=params
            )

            # Break if request failed with other status code
            if resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    msg = f"获取历史数据为空，开始时间：{start_time}"
                    self.gateway.write_log(msg)
                    break

                buf = []

                for l in data:
                    dt = datetime.fromtimestamp(l[0] / 1000)  # convert to second

                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(l[5]),
                        open_price=float(l[1]),
                        high_price=float(l[2]),
                        low_price=float(l[3]),
                        close_price=float(l[4]),
                        trading_day=dt.strftime('%Y-%m-%d'),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin = buf[0].datetime
                end = buf[-1].datetime
                msg = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # Break if total data count less than limit (latest date collected)
                if len(data) < limit:
                    break

                # Update start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

        return history


class MarginTradeApi(RestClient):
    """
    Eastmoney Margin Trade REST API
    信用账号
    """

    def __init__(self, gateway: EastmoneyGateway):
        """"""
        super().__init__()

        self.gateway: EastmoneyGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.accountid: str = ""
        self.validatekey: str = ""  # ，每一个post url都需要添加这个validatekey
        self.cookies: dict = {}
        #     {"st_si": 51159162223318,
        #                      "st_pvi": 27796171418642,
        #                      "st_sp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        #                      "st_inirUrl" :"",
        #                     "st_sn": 1,
        #                     "st_psi": "{}000-11923323313501-8455476579".format(datetime.now().strftime("%Y%m%d%H%M%S")),
        #                     "st_asi": "delete",
        #                     "Yybdm": 5406,
        #                     "Uid": "Z/YO+XfSUeEB8ofsYitDlQ==",
        #                     "Khmc": "用户姓名",
        #                     "mobileimei": "38ad005a-7b7c-449b-8c98-d6d864fa2374",
        #                     "Uuid": "8d2111e366364f1e9f3f493630c788ea",
        #                     "eastmoney_txzq_zjzh":"NTQwNjYwMTMwNDcxfA=="
        # }
        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.holder_code = {}  # 交易所:股东代码
        self.orders = {}
        self.trades = {}

        self.cache_position_symbols = {}

    def sign(self, request: Request) -> Request:
        """
        Generate  signature.
        """
        security = request.data.pop("security", Security.NONE)
        if security == Security.NONE:
            request.data = None
            return request

        if security == Security.SIGNED:
            if request.params is None:
                request.params = {'validatekey': self.validatekey}
            elif 'validatekey' not in request.params:
                request.params.update({'validatekey': self.validatekey})

        if request.params:
            path = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path = request.path

        request.path = path
        request.params = {}
        # request.data = {}

        # Add headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "gw_reqtimestamp": str(int(datetime.now().timestamp() * 1000)),
            "Host": "jy.xzsec.com",
            "Orgin": "https://jy.xzsec.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36"
        }

        if len(self.cookies) > 0:
            # 更新cookie时间
            # self.cookies.update({"st_sp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            # self.cookies.update({"st_psi": "{}000-11923323313501-8455476579".format(datetime.now().strftime("%Y%m%d%H%M%S"))})
            # cookie => string
            new_cookie_str = self.get_cookie_str()

            # 更新headers的cookie
            headers.update({"Cookie": new_cookie_str})

        if request.headers:
            request.headers.update(headers)
        else:
            request.headers = headers
        if 'Submit' in path:
            print(request.headers)

        return request

    def extra_cookies(self, cookie_str):
        """
        string =》 cookies {}
        :param cookie_str:
        :return:
        """
        cookies = cookie_str.split(';')
        for cookie in cookies:
            if len(cookie) == 0:
                continue
            k, v = cookie.split('=')
            k = k.strip()
            v = v.strip()
            if k.lower() not in KEEP_COOKIES:
                v = urllib.parse.unquote(v)
            else:
                a = 1
            self.cookies.update({k: v})

    def get_cookie_str(self):
        """
        cookie => string
        :return:
        """
        s = ""
        for k, v in self.cookies.items():
            if len(s) > 0:
                s = s + '; '
            if k.lower() in KEEP_COOKIES:
                s = s + f'{k}={v}'
            else:
                if 'https' in v.lower():
                    s = s + '{}={}'.format(k, urllib.parse.quote_plus(v))
                else:
                    s = s + '{}={}'.format(k, urllib.parse.quote(v))
        return s

    def connect(
            self,
            accountid: str,
            validatekey: str,
            cookie_str: str
    ) -> None:
        """
        Initialize connection to REST server.
        """
        self.accountid = accountid
        self.validatekey = validatekey
        if len(cookie_str) > 0:
            self.extra_cookies(cookie_str)

        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        self.init(TRADE_REST_HOST)

        if not self.validate_conn():
            self.gateway.write_log(f'验证登录失败')
            return False

        self.start()

        self.gateway.write_log("REST API启动成功")
        self.gateway.status.update({'td_con': True, 'td_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        if self.gateway.status.get('md_con', False):
            self.gateway.status.update({'con': True})
        # self.query_time()
        self.query_account()
        self.query_position()
        self.query_order()
        # self.query_contract()
        self.query_trade()

        # 添加到定时查询队列中
        self.gateway.query_functions = [self.query_account, self.query_position, self.query_order, self.query_trade]
        return True

    def validate_conn(self):
        """
        验证是否登录成功, 例如请求查询资金和持仓，如果返回非json的结果数据，就表示失败
        :return:
        """
        request = Request(
            method="POST",
            path="/MarginSearch/GetRzrqAssets",
            data={"security": Security.SIGNED, "hblx": "RMB"},
            params={},
            headers={}
        )
        request = self.sign(request)
        try:
            with self._get_session() as session:
                request = self.sign(request)
                url = self.make_full_url(request.path)

                # send request
                stream = request.stream
                method = request.method
                headers = request.headers
                params = request.params
                data = request.data
                response = session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    data=data,
                    proxies=self.proxies,
                    stream=stream,
                )
                request.response = response
                status_code = response.status_code
                if status_code // 100 == 2:  # 2xx codes are all successful
                    if status_code == 204:
                        json_body = None
                    else:
                        try:
                            json_body = response.json()
                            return True
                        except Exception as ex:
                            return False

        except Exception as ex:
            return False

        return False

    def query_account(self) -> Request:
        """"""
        data = {"security": Security.SIGNED,
                "hblx": "RMB"}

        self.add_request(
            method="POST",
            path="/MarginSearch/GetRzrqAssets",
            headers={'Referer': 'https://jy.xzsec.com/MarginSearch/MyAssets'},
            callback=self.on_query_account,
            data=data
        )

    def query_position(self) -> Request:
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="POST",
            path="/MarginSearch/GetStockList",
            callback=self.on_query_position,
            data=data
        )

    def query_order(self) -> Request:
        """查询获取所有当日委托"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/MarginSearch/GetOrdersData",
            callback=self.on_query_order,
            headers={'Referer': 'https://jy.xzsec.com/MarginTrade/Buy'},
            data=data
        )

    def query_trade(self) -> Request:
        """获取当日所有成交"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/MarginSearch/GetDealData",
            callback=self.on_query_trade,
            headers={'Referer': 'https://jy.xzsec.com/MarginTrade/Buy'},
            data=data
        )

    def _new_order_id(self) -> int:
        """"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def get_order(self, orderid: str):
        """返回缓存的Order"""
        return self.orders.get(orderid, None)

    def send_order(self, req: OrderRequest) -> str:
        """发送委托"""
        # 创建本地orderid(str格式， HHMM+00序列号)
        local_orderid = self.gateway.order_manager.new_local_orderid()

        # req => order
        order = req.create_order_data(orderid=local_orderid, gateway_name=self.gateway_name)

        order.accountid = self.accountid
        order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        order.datetime = datetime.now()

        # 构建request的data
        data = {
            "stockCode": req.symbol,
            "tradeType": ORDERTYPE_STOCK2VT[f'{req.direction}_{req.offset}_{req.type}'],
            "price": float(req.price),
            "amount": float(req.volume)
        }
        data.update({"security": Security.SIGNED})

        # 需要名称
        if req.offset == Offset.OPEN:
            zqmc = self.gateway.symbol_name_map.get(req.symbol, None)
            if zqmc is None:
                self.gateway.subscribe(SubscribeRequest(symbol=req.symbol, exchange=req.exchange))
                return ""
            data.update({"zqmc": zqmc})

        # 卖出时，需要股东代码
        if req.direction == Direction.SHORT:
            gddm = self.holder_code.get(req.exchange, None)
            if not gddm:
                self.gateway.write_error(f'找不到{req.symbol}{req.exchange}对应的股东代码')
                return ""

            data.update({"gddm": gddm})
            # 股东代码

        self.add_request(
            method="POST",
            path="/Trade/SubmitTradeV2",
            callback=self.on_send_order,
            headers={'Referer': 'https://jy.xzsec.com/Trade/Buy',
                     'Connection': 'keep-alive',
                     'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
                     'sec-ch-ua-mobile': '?0',
                     'Sec-Fetch-Site': 'same-origin',
                     'Sec-Fetch-Mode': 'cors',
                     'Sec-Fetch-Dest': 'empty',
                     'X-Requested-With': 'XMLHttpRequest'},
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> Request:
        """
        撤单
        :param req:
        :return:
        """
        # 东财撤单，提交的编码是 交易日_委托编号
        if '_' not in req.orderid:
            order = self.gateway.order_manager.get_order_with_local_orderid(req.orderid)
            if order is None:
                order = self.orders.get(req.orderid)
                # 交易日
                if order is None:
                    return False
            if order.datetime.hour >= 15:
                dt = order.datetime + timedelta(days=1)
            else:
                dt = order.datetime
            req_orderid = '{}_{}'.format(dt.strftime('%Y%m%d'), order.sys_orderid)
        else:
            req_orderid = req.orderid

        data = {
            "security": Security.SIGNED,
            "revokes": req_orderid
        }

        self.add_request(
            method="POST",
            path="/Trade/RevokeOrders",
            callback=self.on_cancel_order,
            headers={'Referer': 'https://jy.xzsec.com/Trade/Buy'},
            data=data,
            extra=req
        )
        return True

    def on_query_time(self, data: dict, request: Request) -> None:
        """"""
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

    def on_query_account(self, data: dict, request: Request) -> None:
        """
        查询账号和资产
            {"Status":0,"Data":
            {"Bz":"RMB","Zjzh":"XXXXX","Rzll":"0.06990000","Rqll":"0.10600000","Fxll":"0.18000000","Xyzt":"0",
            "Wcdbbl":"100000000.0000","Ssdbbl":"100000000.0000","Zzc":"128715.50","Zfz":"0.00","Bzjkys":"96010.35","
            Zjye":"16193.36","Zjkys":"16890.26","rqmcsdzj":"0.00","Kzcdbzc":"128715.50","Dbzqsz":"111825.24","Rzbj":"0.00",
            "Rzxf":"0.00","Rzfzhj":"0.00","Yfrqsz":"0.00","Rqxf":"0.00","Rqfzhj":"0.00","Rzsxed":"807200.00",
            "Rzkyed":"807200.00","Rzeddj":"0.00","Rqsxed":"807200.00","Rqkyed":"807200.00","Rqeddj":"0.00",
            "Hlqy":"0.00","Hlqyzt":"0.00","Hgqy":"0","Hgqyzt":"0","Zed":"807200.00","Zkyed":"807200.00",
            "Zsz":"111825.24","Fdyk":"-12250.84","Kqzj":"16193.36"},"Message":""}
        """
        if not isinstance(data, dict):
            self.gateway.write_error(f'不是dict结构')
            self.gateway.write_error(data)
            return
        if data.get('Status', None) != 0:
            self.gateway.write_error(f'返回数据状态不政策:{data}')
            return

        asset = data.get('Data', {})
        self.gateway.write_log(print_dict(asset))
        if asset['Bz'] != "RMB":
            return
        if not self.accountid:
            self.accountid = f"{self.gateway_name}_{asset['asset']}"
        balance_str = asset["Zzc"]
        if len(balance_str) > 0:
            balance = float(balance_str)
        else:
            balance = 0
        # 冻结（还没找到）
        frozen_str = "0"  # asset["Djzj"]
        if len(frozen_str) > 0:
            frozen = float(frozen_str)
        else:
            frozen = 0
        # 持仓盈亏
        holding_profit_str = asset['Fdyk']
        if len(holding_profit_str) > 0:
            holding_profit = float(holding_profit_str)
        else:
            holding_profit = 0
        # 平仓盈亏
        close_profit_str = "0"  # asset['Dryk']
        if len(close_profit_str) > 0:
            close_profit = float(close_profit_str)
        else:
            close_profit = 0
        # 可用保证金
        margin_str = asset['Bzjkys']
        if len(margin_str) > 0:
            margin = float(margin_str)
        else:
            margin = 0
        account = AccountData(
            accountid=self.accountid,
            balance=balance,
            frozen=frozen,
            holding_profit=holding_profit,
            close_profit=close_profit,
            currency='RMB',
            margin=margin,
            gateway_name=self.gateway_name,
            trading_day=datetime.now().strftime('%Y-%m-%d')
        )
        # 可用资金（这里可用资金是给普通买入得), margin 是提供给融资买入得
        account.available = float(asset['Zjkys']) if len(asset['Zjkys']) > 0 else 0

        if account.balance:
            self.gateway.on_account(account)

        self.gateway.write_log("账户资金查询成功")

    def on_query_position(self, data: dict, request: Request) -> None:
        """
        返回仓位查询结果
        :param data:
        :param request:
        :return:
        """
        # {"Message":null,"Status":0,"Data":
        # [{"Zjzh":"XXXXX","Market":"SA","Gddm":"xxx","Zqmc":"许继电气","Zqdm":"000400","Gfye":"700","Gfky":"300","Cbjg":"11.586","Yk":"1027.18","Zxjg":"15.010","Sz":"4503.00","Ykbl":"0.295529","Zqsl":"300","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.03498"},
        # {"Zjzh":"XXXXX","Market":"SA","Gddm":"xx","Zqmc":"东财转3","Zqdm":"123111","Gfye":"130","Gfky":"130","Cbjg":"150.756","Yk":"1129.97","Zxjg":"159.448","Sz":"20728.24","Ykbl":"0.057656","Zqsl":"130","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.16104"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"核建转债","Zqdm":"113024","Gfye":"100","Gfky":"100","Cbjg":"103.471","Yk":"89.90","Zxjg":"104.370","Sz":"10437.00","Ykbl":"0.008688","Zqsl":"100","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.08109"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"核能转债","Zqdm":"113026","Gfye":"100","Gfky":"100","Cbjg":"106.276","Yk":"331.40","Zxjg":"109.590","Sz":"10959.00","Ykbl":"0.031183","Zqsl":"100","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.08514"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"军工ETF","Zqdm":"512660","Gfye":"0","Gfky":"0","Cbjg":"1.277","Yk":"-105.28","Zxjg":"1.251","Sz":"5004.00","Ykbl":"-0.020360","Zqsl":"4000","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.03888"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"招商银行","Zqdm":"600036","Gfye":"200","Gfky":"200","Cbjg":"53.703","Yk":"-1222.60","Zxjg":"47.590","Sz":"9518.00","Ykbl":"-0.113830","Zqsl":"200","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.07395"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"特变电工","Zqdm":"600089","Gfye":"700","Gfky":"700","Cbjg":"15.303","Yk":"-380.21","Zxjg":"14.760","Sz":"10332.00","Ykbl":"-0.035483","Zqsl":"700","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.08027"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"中信建投","Zqdm":"601066","Gfye":"300","Gfky":"300","Cbjg":"35.667","Yk":"-2636.10","Zxjg":"26.880","Sz":"8064.00","Ykbl":"-0.246362","Zqsl":"300","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.06265"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"兴业银行","Zqdm":"601166","Gfye":"600","Gfky":"600","Cbjg":"21.655","Yk":"-2409.00","Zxjg":"17.640","Sz":"10584.00","Ykbl":"-0.185408","Zqsl":"600","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"","Cwbl":"0.08223"},
        # {"Zjzh":"XXXXX","Market":"HA","Gddm":"xx","Zqmc":"中国平安","Zqdm":"601318","Gfye":"400","Gfky":"400","Cbjg":"74.430","Yk":"-8076.10","Zxjg":"54.240","Sz":"21696.00","Ykbl":"-0.271262","Zqsl":"400","Rzmrgfye":"0","Rzmrgfky":"0","Khyq":"0","Sfwdbp":"1","Dwc":"E05650511311021601318","Cwbl":"0.16856"}]}

        if data.get('Status', None) != 0:
            self.gateway.write_error(f'返回数据状态不政策:{data}')
            return

        # 临时缓存合约的配置信息
        for d in data["Data"]:
            # 资金账号
            accountid = d.get('Zjzh')

            # 证券代码
            symbol = d.get('Zqdm')
            # 交易市场
            market = d.get('Market')
            if market == 'SA':
                exchange = Exchange.SZSE
            elif market == 'HA':
                exchange = Exchange.SSE
            else:
                exchange = Exchange.LOCAL

            # 设置交易所 <=>股东代码
            if self.holder_code.get(exchange.value, None) is None:
                self.holder_code.update({exchange.value: d.get('Gddm')})

            if symbol:
                position = PositionData(
                    accountid=accountid,
                    symbol=symbol,
                    name=d['Zqmc'],
                    exchange=exchange,
                    direction=Direction.NET,
                    volume=int(d['Zqsl']),
                    yd_volume=int(d['Gfye']),
                    price=float(d['Cbjg']),
                    cur_price=float(d["Zxjg"]),
                    pnl=float(d["Yk"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_position(position)
                if position.symbol not in self.gateway.symbol_name_map:
                    self.gateway.symbol_name_map[position.symbol] = position.name

                if position.symbol not in self.gateway.symbol_exchange_map:
                    self.gateway.symbol_exchange_map[position.symbol] = position.exchange

        # self.gateway.write_log("持仓信息查询成功")

    def on_query_order(self, data: dict, request: Request) -> None:
        """
        响应当日所有委托的查询
        :param data:
        :param request:
        :return:
        """
        # {"Message":null,"Status":0,"Data":
        # [{"Wtrq":"20210803","Wtsj":"095527","Zqdm":"300844","Zqmc":"山水比德","Mmsm":"配售申购","Wtsl":"1000","Wtzt":"已报","Wtjg":"80.230","Cjsl":"0","Cjje":"0.00","Market":"SA","Wtbh":"125194","Gddm":"0605624416","Xyjylx":"","Dwc":"","Cjjg":"0.000000","Xyjylbbz":""},
        # {"Wtrq":"20210803","Wtsj":"095527","Zqdm":"301047","Zqmc":"义翘神州","Mmsm":"配售申购","Wtsl":"1000","Wtzt":"已报","Wtjg":"292.920","Cjsl":"0","Cjje":"0.00","Market":"SA","Wtbh":"125193","Gddm":"0605624416","Xyjylx":"","Dwc":"","Cjjg":"0.000000","Xyjylbbz":""},
        # {"Wtrq":"20210803","Wtsj":"095527","Zqdm":"787787","Zqmc":"海天申购","Mmsm":"配售申购","Wtsl":"2500","Wtzt":"已报","Wtjg":"36.940","Cjsl":"0","Cjje":"0.00","Market":"HA","Wtbh":"125192","Gddm":"E056505113","Xyjylx":"","Dwc":"","Cjjg":"0.000000","Xyjylbbz":""},
        # {"Wtrq":"20210803","Wtsj":"095256","Zqdm":"512660","Zqmc":"军工ETF","Mmsm":"证券买入","Wtsl":"4000","Wtzt":"已成","Wtjg":"1.277","Cjsl":"4000","Cjje":"5108.00","Market":"HA","Wtbh":"117915","Gddm":"E056505113","Xyjylx":"0","Dwc":"","Cjjg":"1.277000","Xyjylbbz":"买入担保品"},
        # {"Wtrq":"20210803","Wtsj":"094716","Zqdm":"000400","Zqmc":"许继电气","Mmsm":"证券卖出","Wtsl":"400","Wtzt":"已成","Wtjg":"14.530","Cjsl":"400","Cjje":"5817.00","Market":"SA","Wtbh":"101715","Gddm":"0605624416","Xyjylx":"0","Dwc":"","Cjjg":"14.542500","Xyjylbbz":"卖出担保品"},
        # {"Wtrq":"20210803","Wtsj":"094438","Zqdm":"000400","Zqmc":"许继电气","Mmsm":"证券卖出","Wtsl":"400","Wtzt":"已撤","Wtjg":"14.750","Cjsl":"0","Cjje":"0.00","Market":"SA","Wtbh":"93192","Gddm":"0605624416","

        if data.get('Status', None) != 0:
            self.gateway.write_error(f'返回数据状态不政策:{data}')
            return

        orders = data.get('Data', [])
        for d in orders:
            # 系统委托编号
            sys_orderid = d.get('Wtbh')
            # 委托状态
            wtzt = d.get('Wtzt')
            status = STATUS_STOCK2VT.get(wtzt, Status.UNKNOWN)
            # 成交数量
            traded_volume = int(d.get('Cjsl', 0))

            # 检查是否存在本地order_manager缓存中
            local_order = self.gateway.order_manager.get_order_with_sys_orderid(sys_orderid)
            # 比对状态和成交数量
            if local_order is not None and local_order.status == status and local_order.traded == traded_volume:
                continue

            # 检查是否存在相同系统编号、且状态一致的订单（部分成交时，要判断是否更新成交数量)
            if sys_orderid in self.orders:
                # 如果本地没有，sys_order => local_order
                if local_order is None:
                    sys_order = copy(self.orders[sys_orderid])
                    self.gateway.order_manager.on_order(sys_order)
                    self.gateway.order_manager.update_orderid_map(local_orderid=sys_order.orderid,
                                                                  sys_orderid=sys_order.sys_orderid)
                    self.gateway.write_log(f'本地order不存在，添加{sys_order.orderid} <=> {sys_order.sys_orderid}')
                if self.orders[sys_orderid].status == status:
                    if self.orders[sys_orderid].traded == traded_volume:
                        continue

            # 时间
            dt_str = d.get('Wtrq', "") + d.get('Wtsj')
            if len(dt_str) == 0:
                continue
            order_time = datetime.strptime(dt_str, '%Y%m%d%H%M%S')

            # 证券代码
            symbol = d.get('Zqdm', None)
            # 市场
            market = d.get('Market')
            if market == 'SA':
                exchange = Exchange.SZSE
            else:
                exchange = Exchange.SSE
            # 更新 证券代码 <=> 市场
            if symbol not in self.gateway.symbol_exchange_map:
                self.gateway.symbol_exchange_map[symbol] = exchange

            # 委托价格
            price = float(d.get('Wtjg', 0))
            # 委托数量
            volume = int(d.get('Wtsl', 0))
            # 买卖类别
            mmlb = d.get('Mmsm')
            if mmlb == '证券卖出':
                direction = Direction.SHORT
                offset = Offset.CLOSE
            else:  # 'S
                direction = Direction.LONG
                offset = Offset.OPEN

            order = OrderData(
                accountid=self.accountid,
                orderid=sys_orderid if not local_order else local_order.orderid,
                sys_orderid=sys_orderid,
                symbol=symbol,
                exchange=exchange,
                name=d.get('Zqmc', ""),
                price=price,
                volume=volume,
                type=OrderType.LIMIT,
                direction=direction,
                offset=offset,
                traded=traded_volume,
                status=status,
                datetime=order_time,
                time=d.get('Wtsj'),
                gateway_name=self.gateway_name,
            )
            # 更新 sys_order
            self.orders.update({order.orderid: copy(order)})
            # 更新本地order
            self.gateway.order_manager.on_order(copy(order))

        self.gateway.write_log("委托信息查询成功")

    def on_query_trade(self, data: dict, request: Request) -> None:
        """
        今日成交清单
        :param data:
        :param request:
        :return:
        """
        # {"Message":null,"Status":0,"Data":
        # [{"Cjrq":"20210803","Gddm":"E056505113","Mmsm":"证券买入","Wtxh":"117915","Market":"HA","Zqdm":"512660","Zqmc":"军工ETF","Cjsj":"095347","Cjbh":"7993052","Cjjg":"1.277","Cjsl":"4000","Cjje":"5108.00","Cjlx":"0","Wtsl":"4000","Wtjg":"1.277","Dwc":"","Xyjylx":"买入担保品"},
        # {"Cjrq":"20210803","Gddm":"0605624416","Mmsm":"证券卖出","Wtxh":"101715","Market":"SA","Zqdm":"000400","Zqmc":"许继电气","Cjsj":"094716","Cjbh":"0101000010987972","Cjjg":"14.540","Cjsl":"300","Cjje":"4362.00","Cjlx":"0","Wtsl":"400","Wtjg":"14.530","Dwc":"","Xyjylx":"卖出担保品"},
        # {"Cjrq":"20210803","Gddm":"0605624416","Mmsm":"证券卖出","Wtxh":"101715","Market":"SA","Zqdm":"000400","Zqmc":"许继电气","Cjsj":"094716","Cjbh":"0101000010987970","Cjjg":"14.550","Cjsl":"100","Cjje":"1455.00","Cjlx":"0","Wtsl":"400","Wtjg":"14.530","Dwc":"20210803|154759","Xyjylx":"卖出担保品"}]}
        trades = data.get('Data', [])
        for d in trades:

            # 时间
            dt_str = d.get('Cjrq', "") + d.get('Cjsj')
            if len(dt_str) == 0:
                continue
            trade_dt = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
            # 成交编号
            tradeid = d.get('Cjbh')
            if tradeid in self.trades:
                continue
            sys_orderid = d.get('Wtbh')
            local_orderid = self.gateway.order_manager.get_local_orderid(sys_orderid)

            # 创建trade对象
            trade = TradeData(
                accountid=self.accountid,
                symbol=d['Zqdm'],
                exchange=Exchange.SZSE if d['Market'] == 'SA' else Exchange.SSE,
                name=d.get('Zqmc', ""),
                orderid=local_orderid,
                tradeid=tradeid,
                direction=Direction.SHORT if d['Mmsm'] == '证券卖出' else Direction.LONG,
                offset=Offset.CLOSE if d['Mmsm'] == '证券卖出' else Offset.OPEN,
                price=float(d["Cjjg"]),
                volume=float(d['Cjsl']),
                time=d.get('Cjsj'),
                datetime=trade_dt,
                gateway_name=self.gateway_name
            )
            # 更新本地字典
            self.trades.update({trade.tradeid: copy(trade)})
            # 推送事件
            self.gateway.on_trade(trade)

        self.gateway.write_log("成交信息查询成功")

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托回报"""
        # <class 'dict'>: {'Status': 0, 'Count': 1, 'Data': [{'Wtbh': '534155'}], 'Errcode': 0}
        self.gateway.write_log(f'委托返回:{print_dict(data)}')
        if data.get('Status', -1) != 0:
            self.gateway.write_error(f'委托失败,{data}')
            return

        result = data.get('Data', [])
        if len(result) == 0:
            self.gateway.write_error(f'委托数据没有:{data}')
            return

        d = result[0]
        sys_orderid = d.get('Wtbh')
        if not sys_orderid:
            self.gateway.write_error(f'委托返回中没有委托编号:{data}')
            return

        # 获取提交的order
        order = request.extra
        if not order:
            self.gateway.write_error(f'无法从request中获取提交的order')
            return

        # 更新本地orderid 与 sys_order的绑定关系
        local_orderid = order.orderid
        if local_orderid and sys_orderid:
            self.gateway.order_manager.update_orderid_map(local_orderid=local_orderid, sys_orderid=sys_orderid)

        # 推送委托更新消息
        order.sys_orderid = sys_orderid
        order.status = Status.NOTTRADED
        self.gateway.order_manager.on_order(copy(order))

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'订单委托失败:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_error(msg)

    def on_send_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'发送订单异常:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，拒单"
        self.gateway.write_error(msg)
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """"""
        self.gateway.write_log(print_dict(data))


class MarketApi(RestClient):
    """
    Eastmoney MarketData REST API
    """

    def __init__(self, gateway: EastmoneyGateway):
        """"""
        super().__init__()

        self.gateway: EastmoneyGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.config = get_cache_config(STOCK_CONFIG_FILE)
        self.symbol_dict = self.config.get('symbol_dict', {})
        self.gateway.write_log(f'一共获取{len(self.symbol_dict)}个股票信息')
        self.cache_time = self.config.get('cache_time', datetime.now() - timedelta(days=7))

        # vt_symbol的订阅清单
        self.subscribe_tick_array = []  # tick订阅
        self.subscribe_bar_array = []  # bar订阅

        self.quote_interval = 5

        self.bar_dt_dict = {}

        self.count = 0
        self.init(MARKET_REST_HOST)
        self.gateway.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def connect(
            self, pool_sessions=3
    ) -> None:
        """
        Initialize connection to REST server.
        """
        self.start(pool_sessions)

        # 转换本地缓存合约
        self.cov_contracts()

        self.gateway.write_log("MARKET API启动成功")
        self.gateway.status.update({'md_con': True, 'md_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def process_timer_event(self, event: Event) -> None:
        """
        定时器
        :param event:
        :return:
        """
        if self.count > self.quote_interval:
            self.count = 0
            return

        self.count += 1
        # 逐一订阅tick行情
        for vt_symbol in self.subscribe_tick_array:
            symbol, exchange = extract_vt_symbol(vt_symbol)
            if symbol not in self.gateway.symbol_exchange_map:
                self.gateway.symbol_exchange_map[symbol] = exchange
            self.query_quote(symbol)
            time.sleep(0.02)

        dt = datetime.now()
        if 0 < dt.second <= self.quote_interval and self.count ==1:
            # 逐一订阅bar行情
            for vt_symbol in self.subscribe_bar_array:
                symbol, exchange = extract_vt_symbol(vt_symbol)
                if symbol not in self.gateway.symbol_exchange_map:
                    self.gateway.symbol_exchange_map[symbol] = exchange
                self.query_bar(symbol)
                time.sleep(0.02)

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        # 更新缓存
        if req.symbol not in self.gateway.symbol_exchange_map:
            self.gateway.symbol_exchange_map[req.symbol] = req.exchange

        # 添加订阅
        if req.is_bar:
            if req.vt_symbol not in self.subscribe_bar_array:
                self.gateway.write_log(f'添加bar订阅:{req.vt_symbol}')
                self.subscribe_bar_array.append(req.vt_symbol)
        else:
            if req.vt_symbol not in self.subscribe_tick_array:
                self.gateway.write_log(f'添加tick订阅:{req.vt_symbol}')
                self.subscribe_tick_array.append(req.vt_symbol)

    def cov_contracts(self):
        """转换本地缓存=》合约信息推送"""
        for symbol_marketid, info in self.symbol_dict.items():
            symbol, market_id = symbol_marketid.split('_')
            exchange = info.get('exchange', '')
            if len(exchange) == 0:
                continue

            vn_exchange_str = get_stock_exchange(symbol)

            # 排除通达信的指数代码
            if exchange != vn_exchange_str:
                continue

            exchange = Exchange(exchange)
            if info['stock_type'] == 'stock_cn':
                product = Product.EQUITY
            elif info['stock_type'] in ['bond_cn', 'cb_cn']:
                product = Product.BOND
            elif info['stock_type'] == 'index_cn':
                product = Product.INDEX
            elif info['stock_type'] == 'etf_cn':
                product = Product.ETF
            else:
                product = Product.EQUITY

            volume_tick = info['volunit']
            if symbol.startswith('688'):
                volume_tick = 200

            contract = ContractData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=exchange,
                name=info['name'],
                product=product,
                pricetick=round(0.1 ** info['decimal_point'], info['decimal_point']),
                size=1,
                min_volume=volume_tick,
                margin_rate=1
            )

            if product != Product.INDEX:
                # 缓存 合约 =》 中文名
                self.gateway.symbol_name_map.update({contract.symbol: contract.name})

                # 缓存代码和交易所的印射关系
                self.gateway.symbol_exchange_map[contract.symbol] = contract.exchange

                self.gateway.contracts.update({contract.symbol: contract})
                self.gateway.contracts.update({contract.vt_symbol: contract})
                # 推送
                self.gateway.on_contract(contract)

    def query_contract(self, symbol) -> Request:
        """查询股票信息"""
        params = {

        }
        params.update({'id': symbol,
                       'count': 10,
                       'callback': 'sData'})

        self.add_request(
            method="GET",
            path="/api/SHSZQuery",  # ?id=12308&count=10&callback=sData
            callback=self.on_query_contract,
            params=params
        )

    def on_query_contract(self, data: str, request: Request) -> None:
        """处理合约配置"""
        # 'var sData = "123081,123081,4,JYZZ,精研转债,2,0,1;";'"
        self.gateway.write_log(data)
        results = data.strip('var sData =')[1:-2]
        results = results.split(';')
        for info in results:
            s = info.split(',')
            if len(s) != 8:
                continue
            symbol, _, symbol_type, short_name, cn_name, _, _, _ = s
            stock_type_name = STOCKTYPE_DICT.get(symbol_type, '其他')
            exchange = get_stock_exchange(symbol, vn=True)

            contract = ContractData(
                symbol=symbol,
                exchange=Exchange(exchange),
                name=cn_name,
                pricetick=0.001,
                size=1,
                margin_rate=1,
                min_volume=100 if symbol_type != '4' else 10,
                product=Product.SPOT,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
            self.gateway.symbol_name_map[contract.symbol] = contract.name
            self.gateway.symbol_exchange_map[contract.symbol] = contract.exchange

    def query_quote(self, symbol) -> Request:
        """
        查询五档行情
        :param symbol: 合约
        :return:
        """
        params = {
        }
        callback = 'jQuery18304280805817340536_{}'.format(int(datetime.now().timestamp() * 1000))
        params.update({'id': symbol,
                       'market': 'SZ' if self.gateway.symbol_exchange_map.get(symbol) == Exchange.SZSE else 'SH',
                       'callback': callback,
                       'rdm': str(int(datetime.now().timestamp() * 1000 + 1)),
                       '_': str(int(datetime.now().timestamp() * 1000 + 2))
                       })

        self.add_request(
            method="GET",
            path="/api/SHSZQuoteSnapshot",
            # id=600600&market=SH&rdm=1627023689001&callback=jQuery18304280805817340536_1627023081539&_=1627023689002
            callback=self.on_query_quote,
            params=params,
            extra=callback
        )

    def on_query_quote(self, data, request) -> None:
        """
        五档行情返回
        :param data:
        :param request:
        :return:
        """
        # jQuery18304280805817340536_1627023081539(
        # {"code":"123010","name":"博世转债","topprice":"-","bottomprice":"-","status":0,
        # "fivequote":
        #   {"yesClosePrice":"105.150","openPrice":"105.300","sale1":"104.730","sale2":"104.750","sale3":"104.760","sale4":"104.770",
        #   "sale5":"104.780","buy1":"104.679","buy2":"104.620","buy3":"104.610","buy4":"104.601","buy5":"104.600",
        #   "sale1_count":1,"sale2_count":1,"sale3_count":1,"sale4_count":1,"sale5_count":1,"buy1_count":1,"buy2_count":1,"buy3_count":1,"buy4_count":15,
        #   "buy5_count":12},
        #   "realtimequote":{"open":"105.300","high":"105.665","low":"104.560","avg":"105.249","zd":"-0.430",
        #   "zdf":"-0.41%","turnover":"1.88%","currentPrice":"104.720","volume":"8083","amount":"8507311","wp":"3894","np":"4189","time":"14:53:27"},
        # "pricelimit":null,"tradeperiod":0}
        # );
        try:
            results = data.strip(request.extra)[1:-2]
            d = json.loads(results)
            # self.gateway.write_log(d)
            if not isinstance(d, dict):
                return

            symbol = d.get('code')
            fivequote = d.get('fivequote')
            realtimequote = d.get('realtimequote')

            dt_str = datetime.now().strftime('%Y-%m-%d') + " " + realtimequote.get('time')
            tick = TickData(
                symbol=symbol,
                exchange=self.gateway.symbol_exchange_map.get(symbol),
                datetime=datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"),
                name=d.get('name'),
                volume=int(realtimequote["volume"]),
                last_volume=int(realtimequote["volume"]),
                open_interest=realtimequote["volume"],
                last_price=float(realtimequote["currentPrice"]),
                limit_up=float(d["topprice"]) if d['topprice'] != '-' else 0,
                limit_down=float(d["bottomprice"]) if d['bottomprice'] != '-' else 0,
                open_price=float(realtimequote["open"]),
                high_price=float(realtimequote["high"]),
                low_price=float(realtimequote["low"]),
                pre_close=float(fivequote["yesClosePrice"]),
                bid_price_1=float(fivequote["buy1"]) if fivequote['buy1'] != '-' else 0,
                bid_price_2=float(fivequote["buy2"]) if fivequote['buy2'] != '-' else 0,
                bid_price_3=float(fivequote["buy3"]) if fivequote['buy3'] != '-' else 0,
                bid_price_4=float(fivequote["buy4"]) if fivequote['buy4'] != '-' else 0,
                bid_price_5=float(fivequote["buy5"]) if fivequote['buy5'] != '-' else 0,
                ask_price_1=float(fivequote["sale1"]) if fivequote['sale1'] != '-' else 0,
                ask_price_2=float(fivequote["sale2"]) if fivequote['sale2'] != '-' else 0,
                ask_price_3=float(fivequote["sale3"]) if fivequote['sale3'] != '-' else 0,
                ask_price_4=float(fivequote["sale4"]) if fivequote['sale4'] != '-' else 0,
                ask_price_5=float(fivequote["sale5"]) if fivequote['sale5'] != '-' else 0,
                bid_volume_1=int(fivequote["buy1_count"]) if fivequote['buy1_count'] != '-' else 0,
                bid_volume_2=int(fivequote["buy2_count"]) if fivequote['buy2_count'] != '-' else 0,
                bid_volume_3=int(fivequote["buy3_count"]) if fivequote['buy3_count'] != '-' else 0,
                bid_volume_4=int(fivequote["buy4_count"]) if fivequote['buy4_count'] != '-' else 0,
                bid_volume_5=int(fivequote["buy5_count"]) if fivequote['buy5_count'] != '-' else 0,
                ask_volume_1=int(fivequote["sale1_count"]) if fivequote['sale1_count'] != '-' else 0,
                ask_volume_2=int(fivequote["sale2_count"]) if fivequote['sale2_count'] != '-' else 0,
                ask_volume_3=int(fivequote["sale3_count"]) if fivequote['sale3_count'] != '-' else 0,
                ask_volume_4=int(fivequote["sale4_count"]) if fivequote['sale4_count'] != '-' else 0,
                ask_volume_5=int(fivequote["sale5_count"]) if fivequote['sale5_count'] != '-' else 0,
                gateway_name=self.gateway_name
            )
            self.gateway.on_tick(tick)

        except Exception as ex:
            self.gateway.write_error(f'转换tick异常:{str(ex)}')
            self.gateway.write_error(traceback.format_exc())
            return

    def query_bar(self, symbol) -> Request:
        params = {
            'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'ndays': 1,  # 多少天得分时
            'iscr': '0'
        }
        callback = 'Query1124042513914550779375_{}'.format(int(datetime.now().timestamp() * 1000))
        params.update({
            'secid': '{}.{}'.format('0' if self.gateway.symbol_exchange_map.get(symbol) == Exchange.SZSE else '1',
                                    symbol),
            'cb': callback,
            '_': str(int(datetime.now().timestamp() * 1000 + 2))
        })

        self.add_request(
            method="GET",
            path="http://push2his.eastmoney.com/api/qt/stock/trends2/get",
            # id=600600&market=SH&rdm=1627023689001&callback=jQuery18304280805817340536_1627023081539&_=1627023689002
            callback=self.on_query_bar,
            params=params,
            extra=callback
        )

    def on_query_bar(self, data, request) -> None:
        """
        处理分时数据查询返回，推送一分钟on_bar事件
        :param data:
        :param request:
        :return:
        """
        try:
            # 去除回调函数的字符串，去除括号和结尾;
            raw_data = data.strip(request.extra)[1:-2]
            # 文本 => JSON
            j_data = json.loads(raw_data)

            data = j_data.get('data', None)
            if data is None:
                return
            symbol = data.get('code', None)
            if symbol is None:
                return
            status = data.get('status', -1)
            if status != 0:
                return

            # json数据得返回时间戳
            time_stamp = data.get('time', None)
            if time_stamp is None:
                return
            # 时间戳 => datetime
            t = datetime.fromtimestamp(time_stamp)

            klines = data.get('trends', [])
            if len(klines) == 0:
                return

            # 获取倒数两条记录
            for kline in klines[-1:]:
                # str => []
                kline = kline.split(',')
                # bar时间str
                dt_str = kline[0]
                bar_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M')
                bar_hm = bar_dt.strftime('%H%M')

                # 开盘第一根bar，不完整，丢弃
                if bar_hm == '0900':
                    continue

                print('{} <==> {}'.format(bar_hm, t.strftime('%H%M%S')))
                # bar记录时间+1分钟,与返回得时间，属于同一分钟
                if bar_hm != t.strftime('%H%M'):
                    continue

                if self.bar_dt_dict.get(symbol, None) == bar_hm:
                    continue

                self.bar_dt_dict[symbol] = bar_hm

                # 减少一分钟=》bar得开始时间
                bar_dt -= timedelta(minutes=1)
                bar = BarData(
                    symbol=symbol,
                    exchange=self.gateway.symbol_exchange_map.get(symbol),
                    datetime=bar_dt,
                    trading_day=bar_dt.strftime('%Y-%m-%d'),
                    interval=Interval.MINUTE,
                    volume=float(kline[5]),
                    open_price=float(kline[1]),
                    close_price=float(kline[2]),
                    high_price=float(kline[3]),
                    low_price=float(kline[4]),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_bar(bar)

        except Exception as ex:
            err_msg = 'quote_bar Exception:{}'.format(str(ex))
            self.gateway.write_error(err_msg)
