import datetime
import os
import time
import zipfile
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Dict

from boto3 import Session
from sqlalchemy import create_engine

from robust import Robust
import pandas as pd
import ccxt
from tempfile import TemporaryDirectory

from config import Config, utc_offset

kline_types = ['spot', 'swap']


class KlineFetcher(object):

    def __init__(self, origin_config: dict):
        self.config = Config(origin_config, False)
        self.fetch_worker_count = self.config['kline']['fetcher']['worker']['worker_count']
        self.kline_per_limit = self.config['kline']['fetcher']['kline_per_limit']
        self._robust = Robust(self.config)

    def _fetch_klines0(self, symbols: List[str], kline_type: str) -> Dict[str, pd.DataFrame]:
        pass

    def fetch_klines(self, symbols: List[str], kline_type: str) -> Dict[str, pd.DataFrame]:
        start_time = time.time()
        symbols_klines = self._fetch_klines0(symbols, kline_type)
        print(f'获取{kline_type} K线数据完成，花费时间：', time.time() - start_time, '\n')
        return symbols_klines


class KlineDbFetcher(KlineFetcher):
    def __init__(self, origin_config: dict):
        super().__init__(origin_config)
        db_config = self.config['kline']['fetcher']['config']['db']
        time_zone_str = f"{'+' if utc_offset >= 0 else '-'}{utc_offset}:00"
        self.db_engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?&charset=utf8"
                                       % (db_config['username'], db_config['password'],
                                          db_config['host'], db_config['port'], db_config['db_name']),
                                       connect_args={'init_command': f"SET SESSION time_zone='{time_zone_str}'"})

    def _fetch_klines0(self, symbols: List[str], kline_type: str):
        select_sql_prefix = f"select symbol, candle_begin_time, open, high, low, close, volume, quote_volume, trade_num " \
                            f"from klines where symbol in "
        select_sql_suffix = f" order by candle_begin_time desc limit {len(symbols) * self.kline_per_limit}"
        symbols_format = [f"'{symbol}'" for symbol in symbols]
        symbol_condition_sql = f"({','.join(symbols_format)})"
        select_sql = f'{select_sql_prefix}{symbol_condition_sql}{select_sql_suffix}'
        result = {}
        with self.db_engine.begin() as conn:
            records_result_set = conn.execute(select_sql).fetchall()
            if len(records_result_set) <= 0:
                return result
        columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num']
        df = pd.DataFrame(records_result_set, columns=columns, dtype='float')
        df.sort_values('candle_begin_time', inplace=True)
        for symbol, symbol_df in df.groupby('symbol'):
            result[symbol] = symbol_df
        return result


class KlineS3Fetcher(KlineFetcher):
    def __init__(self, origin_config: dict):
        super().__init__(origin_config)
        s3_config = self.config['kline']['fetcher']['config']['s3']
        self.bucket_name = s3_config['bucket_name']
        if 'access_key' in s3_config:
            access_key = s3_config['access_key']
            secret = s3_config['secret']
            session = Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret,
                region_name=s3_config['region_name'])
        else:
            session = Session(region_name=s3_config['region_name'])
        self.s3 = session.resource('s3')

    def _fetch_klines0(self, symbols: List[str], kline_type: str):
        with TemporaryDirectory() as tmp_dir:
            zip_dir_path = os.sep.join([tmp_dir, kline_type])
            kline_zip_temp_path = os.sep.join([zip_dir_path, 'klines.zip'])
            os.mkdir(zip_dir_path)
            with open(kline_zip_temp_path, 'wb') as zip_file:
                self.s3.Bucket(self.bucket_name).download_fileobj(f'{kline_type}/klines.zip', zip_file)
            zip_file = zipfile.ZipFile(kline_zip_temp_path)
            for pkl_name in zip_file.namelist():
                zip_file.extract(pkl_name, zip_dir_path)
            zip_file.close()
            pkl_suffix = '.pkl'
            klines = {}
            for file_name in os.listdir(zip_dir_path):
                if not file_name.endswith(pkl_suffix):
                    continue
                symbol = file_name[:-len(pkl_suffix)]
                if symbol not in symbols:
                    continue
                symbol_df = pd.read_pickle(os.sep.join([zip_dir_path, file_name]))
                symbol_df['candle_begin_time'] = \
                    pd.to_datetime(symbol_df['candle_begin_time'], unit='ms') + datetime.timedelta(
                    hours=utc_offset)
                klines[symbol] = symbol_df
            return klines


class KlineWebFetcher(KlineFetcher):
    LIMIT = 1000

    kline_type_api_map = {
        'spot': 'publicGetKlines',
        'swap': 'fapiPublicGetKlines'
    }

    def __init__(self, origin_config: dict):
        super().__init__(origin_config)
        fetcher_config = self.config['kline']['fetcher']['config']['web']
        self.fix_time = True
        if fetcher_config is not None:
            self.fix_time = fetcher_config['fix_time']
        self.exchange = ccxt.binance()

    def _fetch_klines0(self, symbols: List[str], kline_type: str):
        with ThreadPoolExecutor(max_workers=self.fetch_worker_count) as tp:
            futures = []
            for symbol in symbols:
                future = tp.submit(self._fetch_binance_candle_data, symbol, kline_type, 1100)
                futures.append(future)
            result = []
            for future in futures:
                future_result = future.result()
                if future_result is not None:
                    result.append(future_result)

        df = dict(result)
        return df

    def _fetch_binance_candle_data(self, symbol, kline_type, limit=LIMIT):
        """
        通过ccxt的接口fapiPublic_get_klines，获取永续合约k线数据
        获取单个币种的1小时数据
        :param exchange:
        :param symbol:
        :param limit:
        :param run_time:
        :return:
        """
        # 获取数据
        kline_type_api_name = self.kline_type_api_map[kline_type]
        params = {'symbol': symbol, 'interval': '1h', 'limit': limit}
        fetch_func = getattr(self.exchange, kline_type_api_name)
        kline = self._robust.robust(fetch_func, params)

        if kline is None:
            return None

        # 将数据转换为DataFrame
        columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume',
                   'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(kline, columns=columns, dtype='float')
        df.sort_values('candle_begin_time', inplace=True)
        df['symbol'] = symbol  # 添加symbol列
        columns = ['symbol', 'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num']
        df = df[columns]

        # 整理数据
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
        time_edge = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        if self.fix_time:
            time_edge += datetime.timedelta(hours=utc_offset)
            df['candle_begin_time'] += datetime.timedelta(hours=utc_offset)

        # 删除runtime那行的数据，如果有的话
        df = df[df['candle_begin_time'] < time_edge]

        return symbol, df


type_fetcher = {
    'web': KlineWebFetcher,
    'db': KlineDbFetcher,
    's3': KlineS3Fetcher
}

if __name__ == '__main__':
    # fetcher = KlineWebFetcher({
    #     'system': {
    #         'try_times': 3,
    #         'sleep_seconds': 10
    #     },
    #     'fetcher': {
    #         'worker': {
    #             'worker_count': 10
    #         }
    #     }
    # })
    fetcher = KlineS3Fetcher({
        'kline': {
            'fetcher': {
                'kline_per_limit': 1000,
                'worker': {
                    'worker_count': 10
                },
                'config': {
                    's3': {
                        "region_name": "ap-northeast-1",
                        "bucket_name": "middle-trade"
                    }
                }
            }
        }
    })
    klines = fetcher.fetch_klines(['BTCUSDT', 'ETHUSDT'], 'swap')
    print(klines)