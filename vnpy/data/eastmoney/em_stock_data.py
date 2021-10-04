# 东方财富股票历史行情数据
# 仅用于策略启动时，补充近期数据。

import os
import json
from typing import Dict, List, Any
from datetime import datetime, timedelta
from copy import deepcopy
from time import sleep
from vnpy.api.rest.rest_client import RestClient
from vnpy.trader.object import (
    Interval,
    Exchange,
    Product,
    BarData,
    HistoryRequest
)

KLINE_INTERVALS = ["5m", "15m", "30m", "60m", "1d", "1w"]

INTERVAL_VT2EM: Dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
    "1d": 101,
    "1w": 102,
    "1M": 103,
}

REST_HOST: str = "http://push2his.eastmoney.com"

# 请求K线的参数
kline_params = {
    "cb": "jQuery1124038766196589431523_1631424147365",
    "fields1": "f1,f2,f3,f4,f5,f6",
    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
    "ut": "7eea3edcaed734bea9cbfc24409ed989",
    "klt": 5,  # K线类型
    "fqt": 1,  # 复权类型 1前复权，0不复权，2后复权
    "secid": "0.300933",  # 交易所.股票代码, 0 深圳，1，上海
    "beg": 0,
    "end": 20500000,
    "_": 1631424147396  # 时间戳
}

# 请求分时数据的参数
trend_params = {
    'cb': 'jQuery1124042513914550779375_1578882361468',
    'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13',
    'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
    'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
    'ndays': 1,  # 多少天得分时
    'iscr': '0',
    'secid': '1.603286',  # 交易所.股票代码, 0 深圳，1，上海
    '_': '1578882361472'
}


# f43：最新价
# f44：最高
# f45:最低
# f46:今开
# f47:成交量
# f48：成交额
# f50:量比
# f51：涨停
# f52：跌停
# f60：昨收
# f116：总市值
# f117：流通市值
# f162：市盈动
# f167：市净
# f168：换手


class EastMoneyData(RestClient):
    """
    东方财富行情数据
    """

    def __init__(self, parent=None):
        super().__init__()
        self.parent = parent
        self.init(url_base=REST_HOST)

    def write_log(self, msg):
        """日志"""
        if self.parent and hasattr(self.parent, 'write_log'):
            func = getattr(self.parent, 'write_log')
            func(msg)
        else:
            print(msg)

    def get_interval(self, interval, interval_num):
        """ =》K线间隔"""
        t = interval[-1]
        k_interval = f'{interval_num}{t}'
        if k_interval not in KLINE_INTERVALS:
            raise Exception(f"{k_interval}不在支持得K线间隔中")
        else:
            return k_interval

    def get_bars(self,
                 req: HistoryRequest,
                 return_dict=True,
                 ) -> List[Any]:
        """
        获取历史kline
        东财行情，暂时没有找到更长得数据接口，因此不能循环递归下载数据,req.start参数不起作用
        :param req:
        :param return_dict: 返回数据，属于dict结构，还是bar对象
        :return:
        """
        bars = []

        # 如果是分时数据，从get_1m_bars中获取
        if req.interval == Interval.MINUTE and req.interval_num == 1:
            return self.get_1m_bars(req, return_dict)

        # K线类型
        klt = INTERVAL_VT2EM.get(self.get_interval(req.interval.value, req.interval_num))
        # 市场+股票代码
        secid = '{}.{}'.format(0 if req.exchange == Exchange.SZSE else 1, req.symbol)
        beg = 0

        # Create query params
        params = deepcopy(kline_params)
        params.update({'klt': klt,
                       'secid': secid,
                       'beg': beg,
                       '_': int(datetime.now().timestamp())})

        # Get response from server
        resp = self.request(
            "GET",
            "/api/qt/stock/kline/get",
            data={},
            params=params
        )

        # Break if request failed with other status code
        if resp.status_code // 100 != 2:
            msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.write_log(msg)

        else:
            # => 文本数据
            raw_data = resp.text
            # 去除回调函数的字符串，去除括号和结尾;
            raw_data = raw_data.strip(kline_params['cb'])[1:-2]
            # 文本 => JSON
            j_data = json.loads(raw_data)

            buf = []
            begin_dt, end_dt = None, None
            r_data = j_data.get('data', {})
            klines = r_data.get('klines', [])
            for s in klines:
                kline = s.split(',')
                dt_str = kline[0]

                if req.interval in [Interval.MINUTE]:
                    # 这里是k先的结束时间
                    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M')
                else:
                    # 这里是日线、周线的开始时间
                    dt = datetime.strptime(dt_str, '%Y-%m-%d').replace(hour=9, minute=30)
                if not begin_dt:
                    begin_dt = dt
                end_dt = dt

                if return_dict:
                    bar = {
                        "datetime": dt,
                        "symbol": req.symbol,
                        "exchange": req.exchange.value,
                        "vt_symbol": f'{req.symbol}.{req.exchange.value}',
                        "interval": req.interval.value,
                        "open": float(kline[1]),
                        "close": float(kline[2]),
                        "high": float(kline[3]),
                        "low": float(kline[4]),
                        "volume": float(kline[5]),
                        "gateway_name": "",
                        "open_interest": 0,
                        "trading_day": dt.strftime('%Y-%m-%d')
                    }
                else:
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        trading_day=dt.strftime('%Y-%m-%d'),
                        interval=req.interval,
                        volume=float(kline[5]),
                        open_price=float(kline[1]),
                        close_price=float(kline[2]),
                        high_price=float(kline[3]),
                        low_price=float(kline[4]),
                        gateway_name=self.gateway_name
                    )
                buf.append(bar)

            bars.extend(buf)

            msg = f"获取历史数据成功，{req.symbol} - {klt}，{begin_dt} - {end_dt}"
            self.write_log(msg)

        return bars

    def get_1m_bars(self,
                    req: HistoryRequest,
                    return_dict=True,
                    ) -> List[Any]:
        """
        获取分时图
        :param req:
        :param return_dict:
        :return:
        """
        bars = []
        if req.start is None:
            ndays = 1
        else:
            ndays = min(5, (datetime.now() - req.start).days)
        secid = '{}.{}'.format(0 if req.exchange == Exchange.SZSE else 1, req.symbol)
        params = deepcopy(trend_params)
        params.update({'ndays': ndays,
                       'secid': secid,
                       '_': int(datetime.now().timestamp())})

        # Get response from server
        resp = self.request(
            "GET",
            "/api/qt/stock/trends2/get",
            data={},
            params=params
        )

        # Break if request failed with other status code
        if resp.status_code // 100 != 2:
            msg = f"获取分时数据失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.write_log(msg)

        else:
            raw_data = resp.text
            raw_data = raw_data.strip(kline_params['cb'])[1:-2]
            j_data = json.loads(raw_data)

            buf = []
            begin_dt, end_dt = None, None
            r_data = j_data.get('data', {})
            if r_data is None:
                return bars
            klines = r_data.get('trends', [])
            for s in klines:
                kline = s.split(',')
                dt_str = kline[0]
                # 这里是分时得开始时间
                dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M')
                if not begin_dt:
                    begin_dt = dt
                end_dt = dt
                open_price = float(kline[1])
                close_price = float(kline[2])
                high_price = float(kline[3])
                low_price = float(kline[4])
                if open_price == 0:
                    open_price = close_price
                if high_price == 0:
                    high_price = close_price

                if low_price == 0:
                    low_price = close_price

                if close_price == 0:
                    continue

                if return_dict:
                    bar = {
                        "datetime": dt,
                        "symbol": req.symbol,
                        "exchange": req.exchange.value,
                        "vt_symbol": f'{req.symbol}.{req.exchange.value}',
                        "interval": req.interval.value,
                        "open": open_price,
                        "close": close_price,
                        "high": high_price,
                        "low": low_price,
                        "volume": float(kline[5]),
                        "gateway_name": "",
                        "open_interest": 0,
                        "trading_day": dt.strftime('%Y-%m-%d')
                    }
                else:
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        trading_day=dt.strftime('%Y-%m-%d'),
                        interval=req.interval,
                        volume=float(kline[5]),
                        open_price=open_price,
                        close_price=close_price,
                        high_price=high_price,
                        low_price=low_price,
                        gateway_name="em"
                    )
                buf.append(bar)

            bars.extend(buf)

            msg = f"获取历史分时数据成功，{req.symbol} ，{begin_dt} - {end_dt}"
            self.write_log(msg)

        return bars


if __name__ == '__main__':
    api = EastMoneyData()

    # req = HistoryRequest(symbol='300059',
    #                      exchange=Exchange.SZSE,
    #                      interval=Interval.MINUTE,
    #                      interval_num=5,
    #                      start=datetime.now()-timedelta(days=120))

    req = HistoryRequest(symbol='300059',
                         exchange=Exchange.SZSE,
                         interval=Interval.MINUTE,
                         interval_num=1,
                         start=datetime.now() - timedelta(days=5))

    bars = api.get_bars(req=req, return_dict=True)

    for bar in bars[0:5]:
        print(bar)

    for bar in bars[-5:]:
        print(bar)
