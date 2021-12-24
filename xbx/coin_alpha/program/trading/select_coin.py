"""
邢不行-2020策略分享会
邢不行微信：xbx9025
多空中性选币策略实盘
"""
from xbx.coin_alpha.program.trading.Signals import *
from xbx.coin_alpha.program.trading.Functions import *
from xbx.coin_alpha.program.trading.Config import *
pd.options.mode.chained_assignment = None
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def main():

    # =====获取账户信息
    # 获取账户的实际持仓
    symbol_info = update_symbol_info(exchange, symbol_list)
    print('当前持仓量\n', symbol_info[symbol_info['当前持仓量'] != 0].T, '\n')

    # 计算新的和旧的trade_usdt
    trade_usdt_old, trade_usdt_new = cal_old_and_new_trade_usdt()

    # 计算每个策略分配的交易资金
    strategy_trade_usdt = cal_strategy_trade_usdt(stratagy_list, trade_usdt_new, trade_usdt_old)
    print(strategy_trade_usdt)

    # =====开始每小时的循环
    while True:

        # =====sleep直到下一个整点小时
        run_time = sleep_until_run_time('1h', if_sleep=False)

        # =====并行获取所有币种的1小时K线
        symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time)
        print(symbol_candle_data['BTCUSDT'].tail(2), '\n')

        # =====选币数据整理 & 选币
        select_coin = cal_factor_and_select_coin(stratagy_list, symbol_candle_data, run_time)
        # print(select_coin[['key', 's_time', 'symbol', '方向']], '\n')

        # =====计算选中币种的实际下单量
        symbol_info = cal_order_amount(symbol_info, select_coin, strategy_trade_usdt, run_time)
        symbol_amount = symbol_info
        print('实际下单量：\n', symbol_info, '\n')

        # =====逐个下单
        symbol_last_price = fetch_binance_ticker_data(exchange)  # 获取币种的最新价格
        place_order(symbol_info, symbol_last_price)  # 下单
        time.sleep(short_sleep_time)  # 下单之后休息一段时间

        # =====看是否需要更新trade_usdt
        if (run_time - datetime(2000, 1, 1)).days % Update_Day == 0 and run_time.hour == Update_Hour:
            print('开始更新trade_usdt')
            # 更新trade_usdt_old，trade_usdt_new
            trade_usdt_old = trade_usdt_new  # 更新trade_usdt_old
            trade_usdt_new = fetch_binance_swap_equity(exchange)  # 使用最新的账户净值，更新trade_usdt_new
            print('trade_usdt_old：', trade_usdt_old, 'trade_usdt_new：', trade_usdt_new, '\n')
            # 更新strategy_trade_usdt
            strategy_trade_usdt = cal_strategy_trade_usdt(stratagy_list, trade_usdt_new, trade_usdt_old)
            print(strategy_trade_usdt, '\n')
            # 净值保存到本地
            store_trade_usdt_history(trade_usdt_new, run_time, File_Name)

        # =====数据更新、整理、监测
        # 更新账户的实际持仓
        time.sleep(short_sleep_time)
        symbol_info = update_symbol_info(exchange, symbol_list)

        # 数据监测
        equity = fetch_binance_swap_equity(exchange)  # 更新账户净值
        # 账户净值、持仓、下单信息等发送钉钉
        send_dingding_msg_every_loop(equity, select_coin, symbol_info, symbol_amount, symbol_last_price)

        # 本次循环结束
        print('\n', '-' * 20, '本次循环结束，%f秒后进入下一次循环' % long_sleep_time, '-' * 20, '\n\n')
        time.sleep(long_sleep_time)


if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            print('程序报错中断，报错内容：', str(e), '暂停一段时间后重启，sleep时间：', long_sleep_time)
            send_dingding_msg('程序报错中断，报错内容：' + str(e), dingding_id, dingding_secret)
            time.sleep(long_sleep_time)
