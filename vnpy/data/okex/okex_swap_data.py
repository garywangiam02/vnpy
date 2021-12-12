# okex合约数据

import os
import json
from pandas import *
from typing import Dict, List, Any
from datetime import datetime, timedelta
from vnpy.api.rest.rest_client import RestClient
from vnpy.trader.object import (
    Interval,
    Exchange,
    Product,
    BarData,
    HistoryRequest
)
from vnpy.trader.utility import save_json, load_json

OKEXE_INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

INTERVAL_VT2OKEXEF: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

REST_HOST: str = "https://www.okex.com"



def sortbar(barlist):
    """
    实现bar的排序

    """
    # print(123)
    bardf = DataFrame(columns=('datetime','bar'))
    for bar in barlist:
        barsr = Series({'datetime':bar.datetime,'bar':bar})
        bardf = bardf.append(barsr, ignore_index=True)

    bardf.index = bardf['datetime']
    del bardf['datetime']
    bardf = bardf.sort_index()
    barlist = bardf['bar'].values.tolist()

    return barlist
    # t = {}
    # addrlist = list()
    # for item in barlist:
    #     if item['MODBUS从站ID'] not in t.keys():
    #         t[item['MODBUS从站ID']] = list()
    #         t[item['MODBUS从站ID']].append(item)
    #     else:
    #         t[item['MODBUS从站ID']].append(item)
    # for key in t.keys():
    #     t[key].sort(key=lambda k: (k.get('起始地址', 0)))
    #     addrlist += t[key]
    # return addrlist


class OKEXSwapData(RestClient):

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
        b_interval = f'{interval_num}{t}'
        if b_interval not in OKEXE_INTERVALS:
            return interval
        else:
            return b_interval


    def get_bars(self,
                 req: HistoryRequest,
                 return_dict=True,
                 ) -> List[Any]:
        """获取历史kline"""
        bars = []
        while True:
            # Create query params
            params = {
                "granularity":'60',
                "start": req.start.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                "end": datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.000Z')
            }

            # Add end time if specified
            if req.end:
                params["end"] =  req.end.strftime('%Y-%m-%dT%H:%M:%S.000Z')  # convert to millisecond

            # Get response from server
            resp = self.request(
                "GET",
                "/api/swap/v3/instruments/"+req.symbol +"/candles",
                data={},
                params=params
            )

            # Break if request failed with other status code
            if resp.status_code // 100 != 2:
                msg = f"获取历史数据失败或超过1天阈值，状态码：{resp.status_code}，信息：{resp.text}"
                self.write_log(msg)
                break
            else:
                datas = resp.json()
                if not datas:
                    msg = f"获取历史数据为空"
                    self.write_log(msg)
                    break

                buf = []
                begin, end = None, None
                for data in datas:
                    dt = datetime.strptime(data[0],'%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=8)
                    # dt = datetime.fromtimestamp(data[0] / 1000)  # convert to second
                    if not begin:
                        begin = dt
                    end = dt
                    if return_dict:
                        bar = {
                            "datetime": dt,
                            "symbol": req.symbol,
                            "exchange": req.exchange.value,
                            "vt_symbol": f'{req.symbol}.{req.exchange.value}',
                            "interval": req.interval.value,
                            "volume": float(data[5]),
                            "open": float(data[1]),
                            "high": float(data[2]),
                            "low": float(data[3]),
                            "close": float(data[4]),
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
                            volume=float(data[5]),
                            open_price=float(data[1]),
                            high_price=float(data[2]),
                            low_price=float(data[3]),
                            close_price=float(data[4]),
                            gateway_name="OKEXS"
                        )
                    buf.append(bar)

                bars.extend(buf)

                msg = f"获取历史数据成功，{req.symbol} - {1}min，{begin} - {end}"
                self.write_log(msg)

                # Break if total data count less than limit (latest date collected)
                # if len(datas) < limit:
                #     break

                # Update start time
                req.start = end + timedelta(minutes=200)
                # self.write_log('一次取200个1min Bar')

        #按照时间小序列进行排序。
        bars = sortbar(bars)

        return bars

    def export_to(self, bars, file_name):
        """导出bar到文件"""
        if len(bars) == 0:
            self.write_log('not data in bars')
            return

        import pandas as pd
        df = pd.DataFrame(bars)
        df = df.set_index('datetime')
        df.index.name = 'datetime'
        df.to_csv(file_name, index=True)
        self.write_log('保存成功')

    def get_contracts(self):

        contracts = {}
        # Get response from server
        resp = self.request(
            "GET",
            "/fapi/v1/exchangeInfo/",
            data={}
        )
        if resp.status_code // 100 != 2:
            msg = f"获取交易所失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.write_log(msg)
        else:
            data = resp.json()
            for d in data["symbols"]:
                self.write_log(json.dumps(d, indent=2))
                base_currency = d["baseAsset"]
                quote_currency = d["quoteAsset"]
                name = f"{base_currency.upper()}/{quote_currency.upper()}"

                pricetick = 1
                min_volume = 1

                for f in d["filters"]:
                    if f["filterType"] == "PRICE_FILTER":
                        pricetick = float(f["tickSize"])
                    elif f["filterType"] == "LOT_SIZE":
                        min_volume = float(f["stepSize"])

                contract = {
                    "symbol": d["symbol"],
                    "exchange": Exchange.OKEXE.value,
                    "vt_symbol": d["symbol"] + '.' + Exchange.OKEXE.value,
                    "name": name,
                    "price_tick": pricetick,
                    "symbol_size": 20,
                    "margin_rate": round(float(d['requiredMarginPercent']) / 100, 5),
                    "min_volume": min_volume,
                    "product": Product.FUTURES.value,
                    "commission_rate": 0.005
                }

                contracts.update({contract.get('vt_symbol'): contract})

        return contracts

    @classmethod
    def load_contracts(self):
        """读取本地配置文件获取期货合约配置"""
        f = os.path.abspath(os.path.join(os.path.dirname(__file__), 'future_contracts.json'))
        contracts = load_json(f, auto_save=False)
        return contracts

    def save_contracts(self):
        """保存合约配置"""
        contracts = self.get_contracts()

        if len(contracts) > 0:
            f = os.path.abspath(os.path.join(os.path.dirname(__file__), 'future_contracts.json'))
            save_json(f, contracts)
            self.write_log(f'保存合约配置=>{f}')



if __name__== '__main__':

    start_dt = datetime.utcnow() - timedelta(hours=5000)
    # start_dt = datetime.strftime(datetime.now() - timedelta(minutes=100), '%Y-%m-%dT%H:%M:%S.000Z')
    # start_dt = '2021-01-21T07:00:00.000Z'
    end_dt = datetime.utcnow()

    req = HistoryRequest(
        symbol='BTC-USD-SWAP',
        exchange=Exchange.OKEX,
        start=start_dt,
        end=end_dt,
        interval_num=60
    )
    okexData = OKEXSwapData()
    data2 = okexData.get_bars(req=req,return_dict=False)
    print(data2)