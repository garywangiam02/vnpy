# encoding: UTF-8

# 股票砖图数据重构器
# v2： 20200910
# 支持使用1分钟数据，进行快速更新
# 支持检查是否发生复权数据，如果发生复权，将现有renko数据全部清除，重新进行

import os
import copy
import csv
import signal
import traceback
import pandas as pd
import numpy as np

from queue import Queue
from datetime import datetime, timedelta
from time import sleep
from threading import Thread

from vnpy.data.tdx.tdx_stock_data import TdxStockData
from vnpy.data.tdx.tdx_common import FakeStrategy
from vnpy.data.mongo.mongo_data import MongoData
from vnpy.data.renko.config import STOCK_RENKO_DB_NAME, HEIGHT_LIST
from vnpy.component.cta_renko_bar import CtaRenkoBar
from vnpy.trader.object import TickData, RenkoBarData, Exchange, Color
from vnpy.trader.utility import get_trading_date, get_stock_exchange, extract_vt_symbol
from vnpy.data.stock.adjust_factor import get_all_adjust_factor
from vnpy.data.common import stock_to_adj

class StockRenkoRebuilder(FakeStrategy):

    def __init__(self, setting={}):

        self.tdx_api = None

        self.queue = Queue()
        self.active = False
        self.loaded = False

        self.thread = None

        self.symbol = None
        self.price_tick = 0.01

        self.renko_bars = {}  # bar_name: renko_bar

        self.setting = setting
        self.mongo_client = MongoData(host=self.setting.get('host', 'localhost'), port=self.setting.get('port', 27017))

        self.db_name = setting.get('db_name', STOCK_RENKO_DB_NAME)

        self.last_close_dt_dict = {}

        self.cache_folder = setting.get('cache_folder', None)

        self.bar_folder = setting.get('bar_folder', None)

        self.bar_df_dict = {}
        # 复权因子
        self.adjust_factors = get_all_adjust_factor()

    def get_last_bar(self, renko_name):
        """
         通过mongo获取最新一个bar的数据
        :param renko_name:
        :return:
        """
        qryData = self.mongo_client.db_query_by_sort(db_name=self.db_name,
                                                     col_name=renko_name,
                                                     filter_dict={},
                                                     sort_name='datetime',
                                                     sort_type=-1,
                                                     limitNum=1)

        last_renko_close_dt = None
        bar = None
        for d in qryData:
            d.pop('_id', None)
            symbol = d.get('symbol')
            exchange = d.pop('exchange', None)
            d.pop('vt_exchange', None)
            if exchange == '' or exchange is None:
                exchange = get_stock_exchange(symbol)
                if exchange == "":
                    exchange = Exchange.LOCAL.value

            bar = RenkoBarData(gateway_name='',
                               symbol=symbol,
                               exchange=Exchange(exchange),
                               datetime=None)
            d.update({'open_price': d.pop('open')})
            d.update({'close_price': d.pop('close')})
            d.update({'high_price': d.pop('high')})
            d.update({'low_price': d.pop('low')})
            bar.__dict__.update(d)
            bar.color = Color(d.get('color'))

            last_renko_open_dt = d.get('datetime', None)
            if last_renko_open_dt is not None:
                last_renko_close_dt = last_renko_open_dt + timedelta(seconds=d.get('seconds', 0))
            break

        return bar, last_renko_close_dt

    def load_bar_csv_to_df(self, vt_symbol, data_start_date='2016-01-01', data_end_date='2099-01-01'):
        """
        加载回测bar数据到DataFrame
        1. 增加前复权/后复权
        :param vt_symbol:
        :param bar_file:
        :param data_start_date:
        :param data_end_date:
        :return:
        """

        if not self.bar_folder:
            self.write_error(f'参数没有配置bar_folder路径')
            return False

        if vt_symbol in self.bar_df_dict:
            return True

        symbol, exchange = extract_vt_symbol(vt_symbol)

        bar_file = os.path.join(self.bar_folder, exchange.value, f'{symbol}_1m.csv')
        self.write_log(u'loading {} from {}'.format(vt_symbol, bar_file))
        if bar_file is None or not os.path.exists(bar_file):
            self.write_error(u'回测时，{}对应的csv bar文件{}不存在'.format(vt_symbol, bar_file))
            return False

        try:
            data_types = {
                "datetime": str,
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "open_interest": float,
                "volume": float,
                "instrument_id": str,
                "symbol": str,
                "total_turnover": float,
                "limit_down": float,
                "limit_up": float,
                "trading_day": str,
                "date": str,
                "time": str
            }
            # 加载csv文件 =》 dateframe
            symbol_df = pd.read_csv(bar_file, dtype=data_types)
            # 转换时间，str =》 datetime
            symbol_df["datetime"] = pd.to_datetime(symbol_df["datetime"], format="%Y-%m-%d %H:%M:%S")
            # 设置时间为索引
            symbol_df = symbol_df.set_index("datetime")

            start_date = datetime.strptime(data_start_date, "%Y-%m-%d")
            end_date = datetime.strptime(data_end_date, "%Y-%m-%d")
            # 裁剪数据
            symbol_df = symbol_df.loc[start_date:end_date]

            # 复权转换
            adj_list = self.adjust_factors.get(vt_symbol, [])
            # 按照结束日期，裁剪复权记录
            adj_list = [row for row in adj_list if
                        row['dividOperateDate'].replace('-', '') <= data_end_date.replace('-', '')]

            if adj_list:
                self.write_log(f'需要对{vt_symbol}进行前复权处理')
                for row in adj_list:
                    row.update({'dividOperateDate': row.get('dividOperateDate') + ' 09:31:00'})
                # list -> dataframe, 转换复权日期格式
                adj_data = pd.DataFrame(adj_list)
                adj_data["dividOperateDate"] = pd.to_datetime(adj_data["dividOperateDate"], format="%Y-%m-%d %H:%M:%S")
                adj_data = adj_data.set_index("dividOperateDate")
                # 调用转换方法，对open,high,low,close, volume进行复权, fore, 前复权， 其他，后复权
                symbol_df = stock_to_adj(symbol_df, adj_data, adj_type='fore')

            # 添加到待合并dataframe dict中
            self.bar_df_dict.update({vt_symbol: symbol_df})

        except Exception as ex:
            self.write_error(u'回测时读取{} csv文件{}失败:{}'.format(vt_symbol, bar_file, ex))
            self.write_log(u'回测时读取{} csv文件{}失败:{}'.format(vt_symbol, bar_file, ex))
            return False

        return True

    def start_with_bar(self, symbol, price_tick, height, start_date='2016-01-01'):
        """启动renko重建工作，使用分钟bar"""
        self.symbol = symbol.upper()
        self.price_tick = price_tick

        if not isinstance(height, list):
            height = [height]

        exchange_value = get_stock_exchange(self.symbol)
        vt_symbol = f'{self.symbol}.{exchange_value}'

        # 复权转换
        adj_list = self.adjust_factors.get(vt_symbol, [])

        db_last_close_dt = None
        for h in height:
            bar_name = '{}_{}'.format(self.symbol, h)
            bar_setting = {'name': bar_name,
                           'symbol': self.symbol,
                           'price_tick': price_tick}
            if isinstance(h, str) and 'K' in h:
                kilo_height = int(h.replace('K', ''))
                renko_height = price_tick * kilo_height
                bar_setting.update({'kilo_height': kilo_height})
            else:
                renko_height = price_tick * int(h)
                bar_setting.update({'renko_height': price_tick * int(h)})

            self.renko_bars[bar_name] = CtaRenkoBar(None, cb_on_bar=self.on_renko_bar, setting=bar_setting)
            bar, bar_last_close_dt = self.get_last_bar(bar_name)

            # 检查是否要清除
            if bar_last_close_dt:
                if len(adj_list) > 0:
                    last_adj_date = adj_list[-1]['dividOperateDate']
                    if (db_last_close_dt - timedelta(days=7)).strftime('%Y%m%d') > last_adj_date.replace('-', ''):
                        self.write_log(f'移除现有的renko bar')
                        self.remove_renkos(symbol=symbol, height=h)
                        continue

                if db_last_close_dt:
                    db_last_close_dt = min(bar_last_close_dt, db_last_close_dt)
                else:
                    db_last_close_dt = bar_last_close_dt

            if bar:
                self.write_log(u'重新添加最后一根{} Bar:{}'.format(bar_name, bar.__dict__))
                # 只添加bar，不触发onbar事件
                self.renko_bars[bar_name].add_bar(bar, is_init=True)
                self.renko_bars[bar_name].update_renko_height(bar.close_price, renko_height)

        # 创建tick更新线程
        self.thread = Thread(target=self.run, daemon=True)
        self.active = True
        self.thread.start()

        # 读取bar_data, 转换为tick，灌输进去
        # 开始时间~结束时间
        start_day = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(db_last_close_dt, datetime):
            if start_day < db_last_close_dt:
                start_day = db_last_close_dt
        cur_trading_date = get_trading_date(datetime.now())

        if self.load_bar_csv_to_df(vt_symbol=vt_symbol,
                                   data_start_date=start_day.strftime('%Y-%m-%d')):

            df = self.bar_df_dict.get(vt_symbol, None)
            if df is not None:
                for idx, bar_data in df.iterrows():
                    if 'close' in bar_data:
                        price = float(bar_data.get('close', 0))
                    else:
                        price = float(bar_data.get('close_price', 0))

                    volume = int(bar_data.get('volume', 0))
                    self.queue.put(item=(idx, price, volume))

        self.write_log(u'加载完毕')
        self.loaded = True

        while (self.active):
            sleep(1)

        self.exit()

    def start(self, symbol, price_tick, height, start_date='2016-01-01', end_date='2099-01-01', refill=False):
        """启动renko重建工作，使用tick"""
        self.symbol = symbol.upper()
        self.price_tick = price_tick
        if not isinstance(height, list):
            height = [height]

        db_last_close_dt = None
        for h in height:
            bar_name = '{}_{}'.format(self.symbol, h)
            bar_setting = {'name': bar_name,
                           'symbol': self.symbol,
                           'price_tick': price_tick}
            if isinstance(h, str) and 'K' in h:
                kilo_height = int(h.replace('K', ''))
                renko_height = price_tick * kilo_height
                bar_setting.update({'kilo_height': kilo_height})
            else:
                renko_height = price_tick * int(h)
                bar_setting.update({'renko_height': price_tick * int(h)})

            self.renko_bars[bar_name] = CtaRenkoBar(None, cb_on_bar=self.on_renko_bar, setting=bar_setting)

            if refill:
                bar, bar_last_close_dt = self.get_last_bar(bar_name)

                if bar:
                    self.write_log(u'重新添加最后一根{} Bar:{}'.format(bar_name, bar.__dict__))
                    # 只添加bar，不触发onbar事件
                    self.renko_bars[bar_name].add_bar(bar, is_init=True)
                    self.renko_bars[bar_name].update_renko_height(bar.close_price, renko_height)
                if bar_last_close_dt:
                    self.last_close_dt_dict.update({bar_name: bar_last_close_dt})
                    if db_last_close_dt:
                        db_last_close_dt = min(bar_last_close_dt, db_last_close_dt)
                    else:
                        db_last_close_dt = bar_last_close_dt

        # 创建tick更新线程
        self.thread = Thread(target=self.run, daemon=True)
        self.active = True
        self.thread.start()

        # 创建tdx连接
        self.tdx_api = TdxStockData(self)

        # 开始时间~结束时间
        start_day = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(db_last_close_dt, datetime):
            if start_day < db_last_close_dt:
                start_day = db_last_close_dt
        end_day = datetime.strptime(end_date, '%Y-%m-%d')
        cur_trading_date = get_trading_date(datetime.now())
        if end_day >= datetime.now():
            end_day = datetime.strptime(cur_trading_date, '%Y-%m-%d') + timedelta(days=1)
            self.write_log(u'结束日期=》{}'.format(cur_trading_date))

        days = (end_day - start_day).days + 1
        self.write_log(u'数据范围：{}~{},{}天'.format(start_day.strftime('%Y-%m-%d'), end_day.strftime('%Y-%m-%d'), days))

        self.loaded = False
        last_tick_dt = None
        try:
            for i in range(days):
                trading_day = start_day + timedelta(days=i)
                self.write_log(u'获取{}分笔交易数据'.format(trading_day.strftime('%Y-%m-%d')))
                ret, result = self.tdx_api.get_history_transaction_data(self.symbol, trading_day.strftime('%Y%m%d'),
                                                                        self.cache_folder)
                if not ret:
                    self.write_error(u'取{} {}数据失败'.format(trading_day, self.symbol))
                    continue

                for data in result:
                    dt = data.get('datetime')

                    # 更新tick时间
                    if last_tick_dt is None:
                        last_tick_dt = dt
                    if last_tick_dt > dt:
                        continue
                    last_tick_dt = dt

                    # 如果tick时间比数据库的记录时间还早，丢弃
                    if db_last_close_dt:
                        if dt < db_last_close_dt:
                            continue
                    price = data.get('price')
                    volume = data.get('volume')
                    self.queue.put(item=(dt, price, volume))

                sleep(5)

        except Exception as ex:
            self.write_error(u'tdx下载数据异常:{}'.format(str(ex)))

        self.tdx_api = None
        self.write_log(u'加载完毕')
        self.loaded = True

        while (self.active):
            sleep(1)

        self.exit()

    def run(self):
        """处理tick数据"""
        self.write_log(u'启动处理tick线程')
        while self.active:
            try:
                dt, price, volume = self.queue.get(timeout=1)
                exchange = Exchange(get_stock_exchange(self.symbol))
                tick = TickData(gateway_name='tdx', symbol=self.symbol, datetime=dt, exchange=exchange)

                tick.date = tick.datetime.strftime('%Y-%m-%d')
                tick.time = tick.datetime.strftime('%H:%M:%S')
                tick.trading_day = get_trading_date(tick.datetime)
                tick.last_price = float(price)
                tick.volume = int(volume)

                for bar_name, renko_bar in self.renko_bars.items():
                    last_dt = self.last_close_dt_dict.get(bar_name, None)
                    if last_dt and tick.datetime < last_dt:
                        continue

                    if tick.datetime.hour == 9 and tick.datetime.minute < 30:
                        continue

                    renko_bar.on_tick(tick)
            except Exception as ex:
                if self.queue.empty() and self.loaded:
                    self.active = False
                    self.write_log(u'队列清空完成')
                elif str(ex) not in ['', 'Empty']:
                    traceback.print_exc()

        self.write_log(u'处理tick线程结束')

    def exit(self):
        self.check_index()
        self.write_log(u'重建结束')
        if self.thread:
            self.thread.join()

        try:
            self.thread = None
            self.queue = None
        except Exception:
            pass

        os.kill(os.getpid(), signal.SIGTERM)

    def on_renko_bar(self, bar: RenkoBarData, bar_name: str):
        """bar到达,入库"""
        flt = {'datetime': bar.datetime, 'open': bar.open_price}

        d = copy.copy(bar.__dict__)
        d.pop('row_data', None)
        # 数据转换
        d.update({'exchange': bar.exchange.value})
        d.update({'color': bar.color.value})
        d.update({'open': d.pop('open_price')})
        d.update({'close': d.pop('close_price')})
        d.update({'high': d.pop('high_price')})
        d.update({'low': d.pop('low_price')})

        try:
            self.mongo_client.db_update(self.db_name, bar_name, d, flt, True)
            self.write_log(u'new Renko Bar:{},dt:{},open:{},close:{},high:{},low:{},color:{}'
                           .format(bar_name, bar.datetime, bar.open_price, bar.close_price, bar.high_price,
                                   bar.low_price, bar.color.value))
        except Exception as ex:
            self.write_error(u'写入数据库异常:{},bar:{}'.format(str(ex), d))

    def update_last_dt(self, symbol, height):
        """更新最后的时间到主力合约设置"""

        bar, last_dt = self.get_last_bar('_'.join([symbol, str(height)]))
        if not last_dt:
            return

        flt = {'symbol': symbol}
        d = {'renko_{}'.format(height): last_dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_dt,
                                                                                            datetime) else last_dt}
        d.update(flt)
        d.update({'vn_exchange': get_stock_exchange(symbol)})
        d.update({'exchange': get_stock_exchange(symbol, False)})
        self.write_log(f'更新合约表中:{symbol}的renko bar {symbol}_{height}最后时间:{d}')
        self.mongo_client.db_update(db_name='Contract', col_name='stock_symbols', filter_dict=flt, data_dict=d,
                                    upsert=True,
                                    replace=False)

    def remove_renkos(self, symbol, height):
        """移除砖图"""
        if not isinstance(height, list):
            height = [height]

        for renko_height in height:
            self.write_log(f'清除砖图{symbol}_{renko_height}')
            self.mongo_client.db_delete(db_name=self.db_name, col_name=f'{symbol}_{renko_height}')

    def check_index(self):
        """检查索引是否存在，不存在就建立新索引"""
        for col_name in self.renko_bars.keys():
            self.write_log(u'检查{}.{}索引'.format(self.db_name, col_name))
            self.mongo_client.db_create_index(dbName=self.db_name, collectionName=col_name, indexName='datetime',
                                              sortType=1)
            self.mongo_client.db_create_multi_index(db_name=self.db_name, col_name=col_name,
                                                    index_list=[('datetime', 1), ('open', 1), ('close', 1),
                                                                ('volume', 1)])
            symbol, height = col_name.split('_')
            self.write_log(u'更新{}最后日期'.format(col_name))
            self.update_last_dt(symbol, height)

    def check_all_index(self):
        contracts = self.mongo_client.db_query(db_name='Contract', col_name='stock_symbols', filter_dict={},
                                               sort_key='symbol')

        for contract in contracts:
            symbol = contract.get('symbol')

            for height in HEIGHT_LIST:
                col_name = '{}_{}'.format(symbol, height)
                self.write_log(u'检查{}.{}索引'.format(self.db_name, col_name))
                self.mongo_client.db_create_index(dbName=self.db_name, collectionName=col_name, indexName='datetime',
                                                  sortType=1)
                self.mongo_client.db_create_multi_index(db_name=self.db_name, col_name=col_name,
                                                        index_list=[('datetime', 1), ('open', 1), ('close', 1),
                                                                    ('volume', 1)])
                symbol, height = col_name.split('_')
                self.write_log(u'更新{}最后日期'.format(col_name))
                self.update_last_dt(symbol, height)

    def export(self, symbol, height=10, start_date='2016-01-01', end_date='2099-01-01', csv_file=None):
        """ 导出csv"""
        qry = {'trading_day': {'$gt': start_date, '$lt': end_date}}
        results = self.mongo_client.db_query_by_sort(db_name=self.db_name,
                                                     col_name='_'.join([symbol, str(height)]), filter_dict=qry,
                                                     sort_name='$natural', sort_type=1)

        if len(results) > 0:
            self.write_log(u'获取数据：{}条'.format(len(results)))
            header = None
            if csv_file is None:
                csv_file = 'renko_{}_{}_{}_{}.csv'.format(symbol, height, start_date.replace('-', ''),
                                                          end_date.replace('-', ''))
            f = open(csv_file, 'w', encoding=u'utf-8', newline="")
            dw = None
            for data in results:
                data.pop('_id', None)
                data['index'] = data.pop('datetime', None)
                data['trading_date'] = data.pop('trading_day', None)

                # 排除集合竞价导致的bar
                bar_start_dt = data.get('index')
                bar_end_dt = bar_start_dt + timedelta(seconds=int(data.get('seconds', 0)))
                if bar_start_dt.hour in [8, 20] and bar_end_dt.hour in [8, 20]:
                    continue

                if header is None and dw is None:
                    header = sorted(data.keys())
                    header.remove('index')
                    header.insert(0, 'index')
                    dw = csv.DictWriter(f, fieldnames=header, dialect='excel', extrasaction='ignore')
                    dw.writeheader()
                if dw:
                    dw.writerow(data)

            f.close()
            self.write_log(u'导出成功,文件:{}'.format(csv_file))
        else:
            self.write_error(u'导出失败')

    def export_refill_scripts(self):
        contracts = self.mongo_client.db_query(db_name='Contract', col_name='stock_symbols', filter_dict={},
                                               sort_key='symbol')

        for contract in contracts:
            symbol = contract.get('symbol')
            command = 'python refill_stock_renko.py {} {}'.format(self.setting.get('host', 'localhost'), symbol)
            self.write_log(command)

    def export_all(self, start_date='2016-01-01', end_date='2099-01-01', csv_folder=None):
        contracts = self.mongo_client.db_query(db_name='Contract', col_name='stock_symbols', filter_dict={},
                                               sort_key='symbol')

        for contract in contracts:
            symbol = contract.get('symbol')
            if contract.get('renko_3') is None or contract.get('renko_5') is None or contract.get('renko_10') is None:
                continue

            for height in HEIGHT_LIST:
                if csv_folder:
                    csv_file = os.path.abspath(os.path.join(csv_folder, 'renko_{}_{}_{}_{}.csv'
                                                            .format(symbol, height, start_date.replace('-', ''),
                                                                    end_date.replace('-', ''))))
                else:
                    csv_file = None
                self.export(symbol, height, start_date, end_date, csv_file)
