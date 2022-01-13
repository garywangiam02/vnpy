# https://bbs.quantclass.cn/thread/3769
import ccxt
import numpy as np
import time
from datetime import datetime, timedelta


def transfer_usdt(deposit_or_withdrawal, transfer_amount):
    while True:
        try:
            info = exchange.sapiPostFuturesTransfer(
                params={
                    'type': deposit_or_withdrawal,  # 1：现货至u本位合约；2：u本位合约至现货
                    'asset': 'USDT',
                    'amount': transfer_amount,
                }
            )
            print(f'划转成功：{info}，划转数量：{transfer_amount}，时间：{datetime.now()}')
            break
        except Exception as e:
            print(f'划转出错：{e}，60秒后重试。{datetime.now()}')
            time.sleep(60)


def timed_transfer_usdt(transfer_count, deposit_or_withdrawal, transfer_amount):
    for i in range(transfer_count):
        transfer_usdt(deposit_or_withdrawal, transfer_amount)
        if (i + 1) == transfer_count:
            break
        print(f'{sleep_time}秒后进行第{i+2}次划转。')
        time.sleep(sleep_time)
    print(f'划转结束，时间{datetime.now()}')


def retra_add_money(retracement_ratio, base_add_amount, frequency):
    max_swap_usdt = 0

    while True:
        swap_usdt_amount = get_swap_coin('USDT')
        spot_usdt_amount = get_spot_coin('USDT')

        # 更新账户资产最高值
        if swap_usdt_amount > max_swap_usdt:
            max_swap_usdt = swap_usdt_amount

        # 计算现值相比最高值的回撤
        retracement = round((max_swap_usdt - swap_usdt_amount) / max_swap_usdt * 100, 2)

        if retracement > retracement_ratio:
            print(f'{datetime.now()} 资产最高值{max_swap_usdt}，当前值{swap_usdt_amount}，回撤{retracement}%，触发加仓。')
            add_amount = retracement * base_add_amount  # 计算加仓金额
            if add_amount < spot_usdt_amount:
                print(f'当前现货余额：{spot_usdt_amount}，加仓金额：{add_amount}。')
                transfer_usdt(1, add_amount)
                max_swap_usdt = swap_usdt_amount  # 重新设定最高值
            else:
                print(f'当前现货余额：{spot_usdt_amount}，余额不足，退出程序。\n')
                exit()
        else:
            print(f'{datetime.now()} 资产最高值{max_swap_usdt}，当前值{swap_usdt_amount}，回撤{retracement}%，不执行操作。')
        time.sleep(frequency)


def get_spot_coin(coin_name):
    while True:
        try:
            spot = exchange.private_get_account()
            for coin in spot['balances']:
                if coin['asset'] == coin_name:
                    return float(coin['free'])
        except Exception as e:
            print(f'获取现货出错，60秒后重试。{datetime.now()}')
            time.sleep(60)


def get_swap_coin(coin_name):
    while True:
        try:
            swap = exchange.fapiPrivate_get_balance()
            for coin in swap:
                if coin['asset'] == coin_name:
                    return float(coin['balance'])
        except Exception as e:
            print(f'获取合约出错，60秒后重试。{datetime.now()}')
            time.sleep(60)


# ===创建交易所
BINANCE_CONFIG = {
    'apiKey': '',
    'secret': '',
    'timeout': 30000,
    'rateLimit': 10,
    # 'hostname': 'binancezh.co',  # 无法fq的时候启用
    'enableRateLimit': False,
    'options': {
        'adjustForTimeDifference': True,  # ←---- resolves the timestamp
        'recvWindow': 10000,
    },
}
exchange = ccxt.binance(BINANCE_CONFIG)

# 存款提款参数设定
withdrawal_amount = 1000  # 如果是提款，请设置提款金额
transfer_count = 20  # 划转次数
sleep_time = 3600  # 间隔时间（s）

# 回撤加仓参数设定
retracement_ratio = 10  # 触发加仓的回撤幅度（%，正值）
base_add_amount = 50  # 基准加仓金额（实际加仓金额 = 基准加仓金额 * 回撤，回撤越大加仓越多）
frequency = 3600  # 监测频率（s）

# 获取现货和合约资产
swap_usdt_amount = get_swap_coin('USDT')
spot_usdt_amount = get_spot_coin('USDT')
print(f'现货账户USDT数量：{spot_usdt_amount}\n合约账户USDT数量：{swap_usdt_amount}')


mode = int(input('======= 模式设定 =======\n1 存款\n2 提款\n3 回撤加仓\n请输入对应数字并按回车：'))

if mode == 1:
    transfer_amount = np.floor(spot_usdt_amount / transfer_count * 100) / 100
    print(f'======================\n当前选择了【存款】模式')
    print(f'每次划转至合约账户数量：{transfer_amount}\n划转次数：{transfer_count}\n共计加仓：{transfer_amount*transfer_count}')
    print(f'划转间隔：{sleep_time}秒')
    print('20秒后开始……')
    time.sleep(20)
    timed_transfer_usdt(transfer_count, mode, transfer_amount)

elif mode == 2:
    transfer_amount = np.floor(withdrawal_amount / transfer_count * 100) / 100
    print('======================\n当前选择了【提款】模式')
    if withdrawal_amount > swap_usdt_amount:
        print('提款金额大于合约账户余额，请检查。')
        exit()
    print(f'每次划转至现货账户数量：{transfer_amount}\n划转次数：{transfer_count}\n共计提款：{withdrawal_amount}')
    print(f'划转间隔：{sleep_time}秒')
    print('20秒后开始……')
    time.sleep(20)
    timed_transfer_usdt(transfer_count, mode, transfer_amount)

elif mode == 3:
    print('======================\n当前选择了【回撤加仓】模式')
    print(f'监控开始，间隔时间{frequency}秒')
    retra_add_money(retracement_ratio, base_add_amount, frequency)
