from coin_alpha_kopo_V1.comon.Functions import *
from coin_alpha_kopo_V1.program.config import *
from time import sleep
import os
import pickle
import ccxt

pd.options.mode.chained_assignment = None
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
import warnings

warnings.filterwarnings('ignore')

debug = 0

if __name__ == '__main__':
    while True:
        try:
            # =====sleep直到下一个整点小时
            if debug == 1:
                # 调试模式，直接进入下一个循环
                run_time = sleep_until_run_time('1h', if_sleep=False)
            else:
                run_time = sleep_until_run_time('1h', if_sleep=True)

            exchange = ccxt.binance()

            # ===获取交易所相关数据
            exchange_info = robust(exchange.fapiPublic_get_exchangeinfo, )

            _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
            _symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]  # 过滤usdt合约
            symbol_list = [symbol for symbol in _symbol_list if symbol not in black_symbol_list]  # 过滤黑名

            # =====并行获取所有币种的1小时K线
            symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time)
            print(len(symbol_candle_data))
            print(symbol_candle_data['BTCUSDT'].tail(2), '\n')

            current_file = __file__
            allsave_path = os.path.join(os.path.abspath(os.path.join(current_file, os.pardir, os.pardir)), 'data', 'k_line', '_'.join(['binance_select_coin_kline_all', '.pkl']))
            with open(allsave_path, 'wb') as f:
                pickle.dump(symbol_candle_data, f, pickle.HIGHEST_PROTOCOL)

            # 生成data_ready文件
            t = datetime.now()
            ready_path = os.path.join(os.path.abspath(os.path.join(current_file, os.pardir, os.pardir)), 'data', 'k_line', '_'.join(
                ['binance_select_coin_kline_all', str(run_time).replace(':', '-'), '.csv']))
            pd.DataFrame(columns=[t]).to_csv(ready_path)
            print('生成ready文件')

            if debug == 1:
                # 调试模式，直接进入下一个循环
                sleep(3)
            else:
                sleep(60)
            # 删除之前的ready文件
            os.remove(ready_path)
            print('删除ready文件')

            if debug == 1:
                exit()

        except Exception as e:
            print('主k线获取程序报错中断，报错内容：', str(e), '暂停一段时间后重启，sleep时间：', '3')
            send_dingding_msg('k线获取程序报错中断，报错内容：' + str(e), dingding_id, dingding_secret)
            time.sleep(3)
            pass
