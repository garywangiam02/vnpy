import ccxt
from coin_alpha_kopo_V1.program.config import *
from coin_alpha_kopo_V1.comon.select_coin_function_twap_freestep import *

ID = name = '主号因子1'

Percent = 1  # 使用trade_usdt的多少比例进行交易，<=1，>0， 如果大于1就是加杠杆了，如2，就是开了两倍杠杆，加了一倍杠杆

apiKey = ''
secret = ''

# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': apiKey,
    'secret': secret,
    'timeout': 3000,
    'rateLimit': 10,
    'hostname': 'binancezh.cc',  # 无法fq的时候启用 binancezh.cc
    'enableRateLimit': False}
exchange = ccxt.binance(BINANCE_CONFIG)


def factor_list_manage(hold_hour, factor_list):
    a = 0
    for i in range(len(factor_list)):
        a = len(factor_list[i][0]) * 5 * factor_list[i][2] * factor_list[i][3] + a

    altha_name = round((a * a + a) / a - a * float(hold_hour.replace('H', '')) / 128, 8)

    factor = {
                 'hold_period': f'{hold_hour}',  # 持仓周期
                 'c_factor': f'{ID}_{hold_hour}_{altha_name}',  # 复合因子1号
                 'factors': [

                 ],
                 'selected_coin_num': selected_coin_num,  # 做空或做多币的数量
             },
    for i in range(len(factor_list)):
        factors = {
                      'factor': factor_list[i][0],  # 选币时参考的因子
                      'para': factor_list[i][2],  # 策略的参数
                      'if_reverse': factor_list[i][1],
                      'weight': factor_list[i][3],
                  },

        factor[0]['factors'].extend(factors)

    return factor


# ===交易策略列表
selected_coin_num = 1

stratagy_list = []

hold_hour_list = ['6H']
for hold_hour in hold_hour_list:
    factor_list = [('bias', False, 4, 1.0), ('cci', True, 36, 0.3)]

    factor = factor_list_manage(hold_hour, factor_list)

    stratagy_list.extend(factor)

hold_hour_list = ['8H']
for hold_hour in hold_hour_list:
    factor_list = [('bias', False, 4, 1.0), ('cci', True, 36, 0.3)]

    factor = factor_list_manage(hold_hour, factor_list)

    stratagy_list.extend(factor)

stratagy_list = stratagy_list.copy()

debug = 0

if __name__ == '__main__':
    select_coin(exchange, black_symbol_list, stratagy_list, name, dingding_id, dingding_secret, Percent, debug)
