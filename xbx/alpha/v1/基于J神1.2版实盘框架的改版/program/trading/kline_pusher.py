import os
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue
from typing import Dict

import pandas as pd
from boto3 import Session
from sqlalchemy import create_engine
from config import Config, time_zone_str
from tempfile import TemporaryDirectory
import zipfile


class KlinePusher(object):
    def __init__(self, origin_config: dict):
        self.config = Config(origin_config, False)

    def push_klines(self, kline_type: str, klines: Dict[str, pd.DataFrame]):
        pass


class KlineDbPusher(KlinePusher):
    def __init__(self, origin_config: dict):
        super().__init__(origin_config)
        db_config = self.config['kline']['pusher']['config']['db']
        self.db_engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?&charset=utf8"
                                       % (db_config['username'], db_config['password'],
                                          db_config['host'], db_config['port'], db_config['db_name']),
                                       connect_args={'init_command': f"SET SESSION time_zone='{time_zone_str}'"})
        self.worker_count = self.config['kline']['pusher']['worker']['worker_count']
        self.conn_pool_size = db_config['conn_pool_size']
        self.batch_save_buffer = db_config['batch_save_buffer']
        self.conn_pool = Queue()

    def _generate_kline_insert_sqls(self, symbol: str, kline_type: str, klines_df: pd.DataFrame):
        if klines_df is None or klines_df.empty:
            return

        batch_insert_sqls = []
        kline_sqls = []
        batch_insert_sql_prefix = f"insert into klines_{kline_type} (symbol, candle_begin_time, " \
                                  f"open, high, low, close, volume, quote_volume, trade_num) values "

        def kline_to_sql(kl):
            field_candle_begin_time = getattr(kl, 'candle_begin_time')
            field_open = getattr(kl, 'open')
            field_high = getattr(kl, 'high')
            field_low = getattr(kl, 'low')
            field_close = getattr(kl, 'close')
            field_volume = getattr(kl, 'volume')
            field_quote_volume = getattr(kl, 'quote_volume')
            field_trade_num = getattr(kl, 'trade_num')

            return f"('{symbol}', '{field_candle_begin_time}', " \
                   f"'{field_open}', " \
                   f"'{field_high}', " \
                   f"'{field_low}', " \
                   f"'{field_close}', " \
                   f"'{field_volume}', " \
                   f"'{field_quote_volume}', " \
                   f"'{field_trade_num}')"

        for kline in klines_df.itertuples():
            kline_sql = kline_to_sql(kline)
            kline_sqls.append(kline_sql)

        kline_sqls_groups = [kline_sqls[i: i + self.batch_save_buffer]
                             for i in range(0, len(kline_sqls), self.batch_save_buffer)]
        for kline_sqls_group in kline_sqls_groups:
            batch_insert_sql = batch_insert_sql_prefix + ','.join(kline_sqls_group)
            batch_insert_sqls.append(batch_insert_sql)
        return batch_insert_sqls

    def _truncate_db(self, kline_type: str, conn):
        truncate_sql = f"truncate table klines_{kline_type}"
        conn.execute(truncate_sql)

    def _save_klines(self, symbol: str, kline_type: str, symbol_klines: pd.DataFrame, conn):
        batch_insert_sqls = self._generate_kline_insert_sqls(symbol, kline_type, symbol_klines)
        [conn.execute(insert_sql) for insert_sql in batch_insert_sqls]

    def push_klines(self, kline_type: str, klines: Dict[str, pd.DataFrame]):
        def _init_db_conn():
            conn = self.db_engine.connect()
            self.conn_pool.put(conn)

        def _invoke_with_conn(func, *args):
            conn = self.conn_pool.get()
            args += (conn,)
            try:
                return func(*args)
            finally:
                self.conn_pool.put(conn)

        with ThreadPoolExecutor(max_workers=self.worker_count) as tp:
            conn_init_futures = [tp.submit(_init_db_conn) for i in range(self.worker_count)]
            [future.result() for future in conn_init_futures]
            _invoke_with_conn(self._truncate_db, kline_type)
            save_futures = []
            for symbol, symbol_klines in klines.items():
                future = tp.submit(_invoke_with_conn,
                                   self._save_klines, symbol, kline_type, symbol_klines)
                save_futures.append(future)
            [future.result() for future in save_futures]

            conns = []
            close_conn_futures = []
            while not self.conn_pool.empty():
                conn = self.conn_pool.get()
                conns.append(conn)
            for conn in conns:
                close_conn_futures.append(tp.submit(lambda: conn.close()))
            [future.result() for future in close_conn_futures]


class KlineS3Pusher(KlinePusher):
    def __init__(self, origin_config: dict):
        super().__init__(origin_config)
        s3_config = self.config['kline']['pusher']['config']['s3']
        self.bucket_name = s3_config['bucket_name']
        session = Session(region_name=s3_config['region_name'])
        self.s3 = session.resource('s3')

    def push_klines(self, kline_type: str, klines: Dict[str, pd.DataFrame]):
        pickle_paths = []
        zip_path_map = {}
        with TemporaryDirectory() as tmp_dir:
            for symbol, df in klines.items():
                pickle_rel_path = f'{symbol}.pkl'
                pickle_path = os.sep.join([tmp_dir, pickle_rel_path])
                df.to_pickle(pickle_path)
                pickle_paths.append(pickle_path)
                zip_path_map[pickle_path] = pickle_rel_path
            pkl_zip_path = os.sep.join([tmp_dir, 'pickles.zip'])
            pkl_zip = zipfile.ZipFile(pkl_zip_path, 'w', zipfile.ZIP_DEFLATED)
            for pickle_path in pickle_paths:
                pkl_zip.write(pickle_path, zip_path_map[pickle_path])
            pkl_zip.close()
            with open(pkl_zip_path, 'rb') as f:
                self.s3.Bucket(self.bucket_name).upload_fileobj(f, f'{kline_type}/klines.zip')


type_pusher = {
    'db': KlineDbPusher,
    's3': KlineS3Pusher
}


if __name__ == '__main__':
    pusher = KlineS3Pusher({
                'kline': {
                    'pusher': {
                        'config': {
                            's3': {
                              "region_name": "ap-northeast-1",
                              "bucket_name": "middle-trade"
                            }
                        }
                    }
                }
            })
    pusher.push_klines('spot', {
        'us': pd.DataFrame([]),
        'en': pd.DataFrame([]),
    })
