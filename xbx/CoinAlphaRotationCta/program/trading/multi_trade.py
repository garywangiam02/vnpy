# -*- coding: utf-8 -*-
import datetime
import time
import os
import numpy as np
import pandas as pd
from trading.strategy import *
from trading.utility import *
from trading.functions import *
from trading.binance import Binance
import ccxt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler  # 后台定时器不能用时可以用阻塞版的


class trade():
    def __init__(self, apiKey, secret, config, notify_sender, posInfer=True, proxies=False, timeout=1, verify=False):
        self.scheduler = BackgroundScheduler()  # 创建定时器以定时执行任务
        # self.scheduler = BlockingScheduler()  # 后台定时器不能用时可以用阻塞版的
        self.exchange = Binance(apiKey=apiKey, secret=secret, notify_sender=notify_sender, proxies=proxies, timeout=timeout,
                                verify=verify)  # 创建交易所类与交易所通讯
        self.ccxt_exchange = ccxt.binance({"apiKey": apiKey, "secret": secret, "timeout": 30000})
        self.sender = notify_sender
        self.sender_warn = self.exchange.sender  # 借用交易所类的报错器发送紧急报错
        self.proxies = proxies  # bool型，有无挂代理
        self.config = config  # 读取各策略配置
        self.config_df = pd.DataFrame(config).T  # 用dataframe保存的各策略配置
        self.weight_sum = self.config_df['weight'].sum()  # 计算累计权重和
        for symbol in self.config_df.symbol.drop_duplicates():  # 调整有用到的各交易对的杠杆
            self.change_leverage(symbol=symbol, leverage=10)
        self.son = []  # 存放所有子策略类
        self.get_precision()  # 从交易所获取合约精度信息
        # 通过配置文件创建子策略实例
        for i in config:
            exec("self.%s=trade_son(config['%s'],self.exchange,'%s')" % (i, i, i))  # 通过策略配置创建子类
            exec("self.son.append(self.%s)" % i)  # 将子类保存在类变量son中
        for son in self.son:
            if son.time_interval.find('m') >= 0:  # 添加循环间隔是分钟的子类的定时任务
                self.scheduler.add_job(son.scheduler, trigger='cron', minute='*/' + son.time_interval.split('m')[0],
                                       misfire_grace_time=1, max_instances=3, id=son.name)
            elif son.time_interval.find('h') >= 0:  # 添加循环间隔是小时的子类的定时任务
                self.scheduler.add_job(son.scheduler, trigger='cron', hour='*/' + son.time_interval.split('h')[0],
                                       misfire_grace_time=1, max_instances=3, id=son.name)
            else:  # 注意暂时未判断按天的策略
                print(son.name, '时间间隔格式错误，请修改')
                raise ValueError
        # 创建母类的时候应更新当前合约账户可用资金和持仓信息和平衡各策略仓位
        self.rebalance_time = None  # 用于记录再平衡时间
        self.record_rebalance = False  # 是否需要更改再平衡时间的flag，不要修改
        self.rebalance()
        # 尝试推断开仓价格和数量
        if posInfer:
            for son in self.son:
                son.pos_infer()
        # 创建再平衡和定期校准的定时任务，默认再平衡时间间隔一个月，校准时间间隔为三天
        self.scheduler.add_job(self.rebalance, trigger='cron', month='*', misfire_grace_time=600, max_instances=3,
                               id='rebalance')
        self.scheduler.add_job(self.calibration, trigger='cron', day='*/3', misfire_grace_time=600, max_instances=3,
                               id='calibration')
        # 以最小时间间隔运行母类
        self.min_interval = get_min_interval(self.config_df)  # 获得最小时间间隔
        if self.min_interval.find('m') >= 0:
            self.scheduler.add_job(self.main, trigger='cron', minute='*/' + self.min_interval.split('m')[0],
                                   misfire_grace_time=10, max_instances=3, id='main')
        elif self.min_interval.find('h') >= 0:
            self.scheduler.add_job(self.main, trigger='cron', hour='*/' + self.min_interval.split('h')[0],
                                   misfire_grace_time=10, max_instances=3, id='main')
        else:
            print('最小时间间隔设置错误，请修改')
            raise ValueError
        # self.scheduler.start()  # 定时器开始工作

    def main(self):  # 母类在每个最小时间间隔都会执行的函数
        time.sleep(0.1)  # 等待其他子策略被调用，避免子类定时调用的延时情况
        self.time = datetime.datetime.now()  # 开始运行时的时间
        self.update_account()  # 更新帐户持仓信息和权益
        # 异步运行执行子类
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()  # 创建一个新的loop执行异步任务
        output = []
        hold_amt = []
        task = []
        for son in self.son:
            if son.to_run:  # 排除本周期无需运行的子类
                task += [son.run()]
        loop.run_until_complete(asyncio.gather(*task))
        print(datetime.datetime.now(), '所有子策略运行完毕')
        # 获得每个子类产生的信号的dataframe，格式：子类名称，标的，时间间隔，策略名，参数，时间，信号，信号价格，杠杆，策略可用资金，策略目标仓位，
        for son in self.son:
            if son.output is not None:  # 排除本周期没有运行的子类
                output.append(
                    [son.name, son.symbol, son.time_interval, son.strategy_name, son.para, son.time, son.signal,
                     son.price, son.signal_leverage, son.available_cash, son.target_amount, son.pricePrecision])
            if(~np.isnan(son.last_amt) and np.isnan(son.signal)):  # 有持仓的子类还需记录之前持有的数量
                hold_amt.append([son.symbol, son.last_amt])
            son.output = None
            son.signal = np.nan
        self.output = pd.DataFrame(output, dtype=float)  # .reset_index(drop=True)
        self.output.columns = ['name', 'symbol', 'time_interval', 'strategy_name', 'para', 'candle_begin_time',
                               'signal', 'price', 'leverage', 'available_cash', 'target_amount', 'pricePrecision']
        self.hold_amt = pd.DataFrame(hold_amt, dtype=float)
        if self.hold_amt.shape[0] > 0:
            self.hold_amt.columns = ['symbol', 'last_amt']
        # 记录再平衡时间
        if self.record_rebalance:
            self.rebalance_time = self.output.candle_begin_time.iloc[0]
            self.record_rebalance = False
        self.output['rebalance_time'] = self.rebalance_time
        # 保存子策略发出的信号
        # 若文件存在，则用追加模式mode='a'，且不写入列名header=False,若文件本身不存在，则用写入模式mode='w'，且需要写入列名header=True

        file_path = f'{path_root_out}/select_symbol_debug.csv'

        self.output.to_csv(file_path, mode='a', index=False, header=False) if os.path.exists(
            file_path) else self.output.to_csv(file_path, mode='w', index=False, header=True)
        # 将子类的信号进行加总和过滤
        self.order_amt = self.output[~np.isnan(self.output.target_amount)].groupby('symbol').agg(
            {'price': 'mean', 'target_amount': 'sum', 'pricePrecision': 'first'})  # 各合约目标持仓数量
        self.order_amt = self.order_amt.merge(self.position[['symbol', 'positionAmt']], how='left',
                                              on='symbol')  # 拼接合约当前仓位
        print("下单预处理信号" + str(self.order_amt))
        if self.hold_amt.shape[0] > 0:
            self.hold_amt = self.hold_amt[~np.isnan(self.hold_amt.last_amt)].groupby('symbol').agg(
                {'last_amt': 'sum'}).reset_index()
            print("hold_amt:" + str(self.hold_amt))
            self.order_amt = self.order_amt.merge(self.hold_amt, how='left', on='symbol')
            self.order_amt['last_amt'] = self.order_amt.last_amt.fillna(0)
            self.order_amt['orderAmt'] = self.order_amt['target_amount'] + self.order_amt['last_amt'] - self.order_amt[
                'positionAmt']  # 计算下单数量
        else:
            self.order_amt['orderAmt'] = self.order_amt['target_amount'] - self.order_amt['positionAmt']  # 计算下单数量
        self.order_amt = self.order_amt[
            0.975 * np.abs(self.order_amt['price'] * self.order_amt['orderAmt']) >= 5]  # 过滤名义价值小于五美元的订单
        self.order_amt['side'] = np.where(self.order_amt['orderAmt'] > 0, 'BUY', 'SELL')  # 得到订单的方向
        print("下单统计信号" + str(self.order_amt))
        if self.order_amt.shape[0] == 0:
            print(datetime.datetime.now(), '本周期无需交易')
        else:
            if (datetime.datetime.now() > self.time + datetime.timedelta(seconds=20)):
                # 为了以防万一，确保下单成功率，当要下单的时间距离主函数刚开始运行的时间已经超过20s，应先更新标的价格再进行下单
                print('延时过久，先更新标的价格')
                all_price = pd.DataFrame(self.exchange.get_f_price(), dtype=float).set_index('symbol')
                self.order_amt.set_index('symbol', inplace=True)
                self.order_amt['price'] = all_price['price']
                self.order_amt.reset_index(inplace=True)
            # 保存母类的下单信号方便复盘查看
            mom_signal_file_path = f'{path_root_out}/mom_signal.csv'
            self.order_amt.to_csv(mom_signal_file_path, mode='a', index=False, header=False) if os.path.exists(
                mom_signal_file_path) else self.order_amt.to_csv(mom_signal_file_path, mode='w', index=False, header=True)
            print(datetime.datetime.now(), '本周期需要进行交易')
            self.place_order()  # 下单并获得订单成交价格
            self.output = self.output.merge(self.order_amt[['symbol', 'avgPrice']], on='symbol', how='left')  # 拼接订单成交价格
            # 保存带有订单成交价格的子类信号以便复盘
            son_signal_file_path = f'{path_root_out}/son_signal_with_real_price.csv'
            self.output.to_csv(son_signal_file_path, mode='a', index=False, header=False) if os.path.exists(
                son_signal_file_path) else self.output.to_csv(son_signal_file_path, mode='w',
                                                              index=False, header=True)
            self.allocate_profit()  # 调整子策略可用资金
        time.sleep(3)
        self.profit_report()
        # self.bnb_burn()

    def bnb_burn(self):
        """
        bnb 燃烧相关函数
        """
        replenish_bnb(self.exchange, self.assets)

    def change_leverage(self, symbol, leverage):  # 调整合约杠杆
        try:
            ret = self.exchange.change_f_leverage(symbol=symbol, leverage=leverage)['leverage']
            print(symbol, '合约杠杠调整为', ret)
        except Exception:
            time.sleep(1)
            self.change_leverage(symbol, leverage)

    # 更新帐户持仓方向和可用资金
    def update_account(self, calibrate=True):
        account = self.exchange.f_account()
        try:
            self.available_cash = account['availableBalance']  #
        except Exception:
            print('更新帐户信息错误，1s后重试')
            time.sleep(1)
            return self.update_account()
        self.position = pd.DataFrame(account['positions'], dtype=float)  # 所有持仓信息
        self.position = self.position[self.position.symbol.isin(self.config_df.symbol)]  # 只保留策略有交易的合约的信息
        self.assets = pd.DataFrame(account['assets'], dtype=float)  # 帐户的总信息
        self.account_equity = self.assets[self.assets['asset'] == 'USDT']['marginBalance'].iloc[0] - self.assets[
            'unrealizedProfit'].sum()  # 帐户权益
        account_file_path = f'{path_root_out}/account_balance.txt'
        with open(account_file_path, 'a+') as f:
            _ = str(datetime.datetime.now()) + ' ' + str(self.account_equity) + '\n'
            f.write(_)
        if calibrate:
            self.calibration(check=True)  # 以检查模式运行校准

    # 定期持仓和收益率报告
    def profit_report(self):
        self.update_account(calibrate=False)
        pos = self.position[self.position.positionAmt != 0][['symbol', 'positionAmt', 'unrealizedProfit']]
        pos.columns = ['合约', '持仓数量', '未实现盈亏']
        _ = ['\n' + y.to_string() + '\n' for x, y in pos.iterrows()]
        text = ''
        text += '# =====持仓信息' + ''.join(_) + '\n\n'
        all_price = pd.DataFrame(self.exchange.get_f_price())
        _ = []
        account_initial_cash, account_available_cash, account_realize_profit, account_unrealize_profit = [0] * 4
        for son in self.son:
            unrealize_profit = np.nan
            unrealize_return = np.nan
            realize_profit = round(son.available_cash - son.initial_cash, 4)
            realize_return = str(round(realize_profit / son.initial_cash * 100, 2)) + '%'
            account_realize_profit += realize_profit
            account_initial_cash += son.initial_cash
            account_available_cash += son.available_cash
            if ~np.isnan(son.last_price):  # 上次是开仓的才有浮动盈亏
                unrealize_profit = round(
                    (float(all_price[all_price['symbol'] == son.symbol]['price']) - son.last_price) * son.last_amt, 4)
                unrealize_return = str(round(unrealize_profit / son.available_cash * 100, 2)) + '%'
                account_unrealize_profit += unrealize_profit
            _.append([son.name, son.initial_cash, son.available_cash, son.last_amt, realize_profit, unrealize_profit,
                      unrealize_return, realize_return])
        _ = pd.DataFrame(_)
        _.columns = ['子类名称', '初始资金', '当前资金', '当前仓位', '已实现利润', '未实现利润', '浮动收益率（当前）', '已实现收益率']
        _ = ['\n' + y.to_string() + '\n' for x, y in _.iterrows()]
        text += '# =====各策略类收益信息' + ''.join(_) + '\n\n'
        account_realize_return = str(round(100 * account_realize_profit / account_initial_cash, 2)) + '%'
        account_unrealize_return = str(round(100 * account_unrealize_profit / account_available_cash, 2)) + '%'
        account_info = pd.Series(
            [account_initial_cash, account_available_cash, account_realize_profit, account_unrealize_profit,
             account_unrealize_return, account_realize_return])
        account_info.index = ['帐户初始资金', '帐户当前资金', '帐户已实现利润', '帐户未实现利润', '帐户浮动收益率(当前)', '帐户已实现收益率']
        text += '# =====帐户收益信息' + '\n' + account_info.to_string() + '\n\n'
        self.sender.send_msg(text)  # 发送报告到微信
        print(text)

    def rebalance(self):
        # 子策略权重的重新平衡，回到最初的权重
        print(datetime.datetime.now(), '开始进行重新平衡')
        self.record_rebalance = True
        self.update_account(calibrate=False)  # 注意再平衡函数会调用更新帐户函数，小心重复更新
        for son in self.son:  # 计算每个子类的可支配资金
            son.available_cash = round(self.account_equity * son.weight, 2)
            son.initial_cash = son.available_cash  # 记录每个策略初始资金

    def calibration(self, check=False, cali_ratio=0.02):
        # 盈亏计算难免与实际有偏差，因此应定期对帐户权益进行校准
        if not check:  # 检查模式不用重新更新帐户信息
            print(datetime.datetime.now(), '开始进行校准')
            self.update_account()
        therotical_equity = 0
        for son in self.son:  # 加总获得理论权益
            therotical_equity += son.available_cash
        ratio = self.account_equity * self.weight_sum / therotical_equity  # 计算调整比例
        if check:  # 检查模式只在偏离过大才修正
            if abs(therotical_equity - self.account_equity * self.weight_sum) / (self.account_equity * self.weight_sum) >= cali_ratio:
                for son in self.son:  # 每个子策略等比例调整，不影响每个子策略的权重
                    son.available_cash = round(ratio * son.available_cash, 2)
        else:  # 强行修正
            for son in self.son:  # 每个子策略等比例调整，不影响每个子策略的权重
                son.available_cash = round(ratio * son.available_cash, 2)

    def get_precision(self):  # 从交易所获得合约的精度
        try:
            symbol_info = pd.DataFrame(self.exchange.get_f_exchangeinfo()['symbols'])
            self.symbol_info = symbol_info
        except Exception:
            print('获取合约精度失败0.5s后重试')
            time.sleep(0.5)
            return self.get_precision()
        for i in self.config:  # 获取精度成功后将精度写入配置
            _symbol_info = symbol_info[(symbol_info.symbol == self.config[i]['symbol'])]
            self.config[i]['pricePrecision'] = int(-np.log10(float(_symbol_info['filters'].iloc[0][0]['tickSize'])))  # 价格精度
            self.config[i]['quantityPrecision'] = _symbol_info['quantityPrecision'].iloc[0]  # 数量精度

    def get_limit_order_data(self, ratio=0.02):
        # ===为了达到成交的目的，计算实际委托价格会向上或者向下浮动一定比例默认为0.02
        order_data = []
        self.order_amt = self.order_amt.merge(self.symbol_info[['symbol', 'quantityPrecision']], how='left', on='symbol')
        for i in self.order_amt.iterrows():
            price = round(i[1]['price'] * (1 + ratio), int(i[1]['pricePrecision'])) if i[1]['side'] == 'BUY' else round(
                i[1]['price'] * (1 - ratio), int(i[1]['pricePrecision']))
            quantity = round(i[1]['orderAmt'], int(i[1]['quantityPrecision']))
            data = {'symbol': i[1]['symbol'], 'price': price, 'quantity': np.abs(quantity),
                    'side': i[1]['side'], 'type': 'LIMIT', 'timeInForce': 'GTC'}
            order_data.append(data)
        self.order_data = order_data  # 将需要操作的订单信息保存在类变量中

    def place_order(self):
        # 下单函数，创建异步下单任务
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        self.get_limit_order_data()
        self.all_order_info = []
        task = []
        for order_data in self.order_data:
            print(order_data)
            task += [self.send_order(order_data)]
        loop.run_until_complete(asyncio.gather(*task))
        time.sleep(10)  # 10s后查询订单成交情况
        self.order_check()

    async def send_order(self, data):
        # 实际下单的函数，对下单失败的情况增加1s后重试机制
        order_info = await self.exchange.asyncf_order(**data)
        if (type(order_info) is str):  # 下单出错
            print('下单失败，1s后重试')
            await asyncio.sleep(1)
            return self.send_order(data)
        else:  # 成功下单
            self.all_order_info += [order_info]

    @asyncrun
    async def get_order_info(self, symbol, order_id):  # 通过标的和订单号查询成交情况
        try:
            _info = await self.exchange.asyncget_f_order(symbol=symbol, orderId=order_id)
            # 将成交价格记录到下单数量的表格中以计算各个子策略的盈亏
            self.order_amt['avgPrice'] = np.where(self.order_amt['symbol'] == _info['symbol'], float(_info['avgPrice']), self.order_amt['avgPrice']
                                                  ) if 'avgPrice' in self.order_amt.columns else np.where(self.order_amt['symbol'] == _info['symbol'], float(_info['avgPrice']), np.nan)
            self.order_text += '\n交易对：' + str(_info['symbol']) + '\n订单号：' + str(_info['orderId']) + '\n原始委托数量：' + str(_info['origQty']) + '\n成交数量：' + str(_info['cumQuote']) + '\n原始委托价格：' + str(
                _info['price']) + '\n平均成交价：' + str(_info['avgPrice']) + '\n方向：' + str(_info['side']) + '，' + str(_info['positionSide']) + '\n订单状态：' + str(_info['status']) + '\n'

        except:
            _info = {}
            _info['avgPrice'] = np.nan
            _info['symbol'] = symbol
            self.order_amt['avgPrice'] = np.where(self.order_amt['symbol'] == _info['symbol'], float(_info['avgPrice']), self.order_amt['avgPrice']
                                                  ) if 'avgPrice' in self.order_amt.columns else np.where(self.order_amt['symbol'] == _info['symbol'], float(_info['avgPrice']), np.nan)
            print('\n订单查询出错，查询id：', order_id)
            self.order_text += '\n订单查询出错，查询id：' + str(order_id)

    # 订单的检查，发送订单信息的文本到微信
    def order_check(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        self.order_text = ''
        task = []
        for order_info in self.all_order_info:
            task += [self.get_order_info(order_info['symbol'], order_info['orderId'])]
        loop.run_until_complete(asyncio.gather(*task))
        self.sender.send_msg(self.order_text)  # 发送订单交易信息
        print(self.order_text)

    def allocate_profit(self, c_rate=0.0004):
        # 通过利润调节子类的可用资金
        for son in self.son:
            _row = self.output[self.output.name == son.name]  # 定位到该策略所属行
            if _row.shape[0] == 0:  # 该策略本周期没有执行，跳过
                continue
            elif np.isnan(_row.iloc[0, :]['signal']):  # 策略无信号，跳过
                continue
            _row = _row.iloc[0, :]
            # 如果平均成交价格是空说明策略间互相抵消，并没有实际下单，以信号价格作为价格锚点
            _row['avgPrice'] = _row['price'] if np.isnan(_row['avgPrice']) else _row['avgPrice']
            if ~np.isnan(son.last_price):  # 如果有上一次的开仓记录且上一次操作不是平仓（平仓后的上次开仓价格会设置为nan)
                if (np.sign(son.last_amt) == np.sign(_row['target_amount'])):  # 同向信号
                    if (np.abs(_row['target_amount']) > np.abs(
                            son.last_amt)):  # 加仓的情况需要更新开仓均价和数量
                        son.available_cash -= np.abs(
                            _row['avgPrice'] * (_row['target_amount'] - son.last_amt)) * c_rate  # 扣除加仓的手续费
                        son.last_price = (np.abs(son.last_price * son.last_amt) + np.abs(
                            _row['avgPrice'] * (_row['target_amount'] - son.last_amt))) / np.abs(
                            _row['target_amount'])  # 更新当前仓位的平均价格
                        son.last_amt = _row['target_amount']
                    elif (np.abs(_row['target_amount']) < np.abs(son.last_amt)):  # 减仓的情况结算利润，更新可用资金
                        son.available_cash += (_row['avgPrice'] - son.last_price) * (
                            son.last_amt - _row['target_amount']) - np.abs(
                            _row['avgPrice'] * np.abs(son.last_amt - _row['target_amount'])) * c_rate
                        son.last_amt = _row['target_amount']
                    else:
                        # 仓位不变，不作调整
                        pass
                elif (np.sign(son.last_amt) * np.sign(_row['target_amount']) == -1):  # 反向信号需要先平仓结算然后重新记录开仓数量和价格
                    son.available_cash += (_row['avgPrice'] - son.last_price) * son.last_amt - _row[
                        'avgPrice'] * np.abs(_row['target_amount'] - son.last_amt) * c_rate  # 这里扣除的手续费是平仓和开仓手续费之和
                    son.last_price = _row['avgPrice']
                    son.last_amt = _row['target_amount']
                else:  # 之前开仓，现在平仓，那么结算收益且记录
                    son.available_cash += (_row['avgPrice'] - son.last_price) * son.last_amt - np.abs(son.last_amt) * \
                        _row['avgPrice'] * c_rate
                    son.last_amt = 0
                    son.last_price = np.nan
            elif np.sign(_row['target_amount']) != 0:  # 如果是上次无记录或者上次平仓，现在开仓，那么需要记录开仓数量和价格
                son.available_cash -= np.abs(_row['avgPrice'] * _row['target_amount']) * c_rate  # 扣除手续费
                son.last_price = _row['avgPrice']
                son.last_amt = _row['target_amount']
            else:  # 如果没有上一次记录或者上次是平仓，且当前是平仓，那么无法调整可用资金，只记录价格数量
                son.last_price = np.nan
                son.last_amt = 0

    def adjust_strategys(self, operation, configs):
        """
        调整待执行的策略列表
         operation
           add : 新增策略
           update : 更新策略
           delete : 删除策略
           unset: 重设策略
        configs:
           {
               "symbol#strategy_name#time_interval":{
                     'symbol': 'ETHUSDT',  # 交易标的
                     'strategy_name': 'real_signal_random',  # 你的策略函数名称
                     'para': [500, 2],  # 参数
                     'data_num': 1000,  # 策略函数需要多少根k线
                     'time_interval': '1m',  # 策略的时间周期
                     'leverage': 2,  # 策略基础杠杆
                     'weight': 0.5,  # 策略分配的资金权重
               },
               "key2":{

               }
           }
        """
        if operation == 'add':
            self.add_strategeys(configs)
        elif operation == 'delete':
            self.delete_strategys(configs)
        elif operation == 'unset':
            self.unset_strategys(configs)

        # 判断是否需要调整母类运行时间间隔
        before_min_interval = self.min_interval
        self.min_interval = get_min_interval(self.config_df)  # 获得最小时间间隔
        if before_min_interval != self.min_interval:
            self.scheduler.remove_job('main')
            if self.min_interval.find('m') >= 0:
                self.scheduler.add_job(self.main, trigger='cron', minute='*/' + self.min_interval.split('m')[0],
                                       misfire_grace_time=10, max_instances=3, id='main')
            elif self.min_interval.find('h') >= 0:
                self.scheduler.add_job(self.main, trigger='cron', hour='*/' + self.min_interval.split('h')[0],
                                       misfire_grace_time=10, max_instances=3, id='main')

    def add_strategeys(self, configs):
        """
        新增策略 并合并已有仓位
        """
        # 去重
        current_strategys = list(self.config_df.index)
        operating_son = []
        filtered_configs = {k: v for k, v in configs.items() if k not in current_strategys}
        if not filtered_configs:
            return
        self.config.update(filtered_configs)  # 更新配置,有则覆盖,无则添加
        self.config_df = pd.DataFrame(self.config).T
        self.weight_sum = self.config_df['weight'].sum()  # 计算累计权重和
        self.get_precision()  # 更新精度

        for strategy_name, config in filtered_configs.items():
            # 添加子类定时调度
            exec("self.%s=trade_son(config,self.exchange,'%s')" % (strategy_name, strategy_name))  # 通过策略配置创建子类
            exec("self.son.append(self.%s)" % strategy_name)   # 将子类保存在类变量son中
            exec("operating_son.append(self.%s)" % strategy_name)  # 将子类保存在类变量operating_son中

        self.rebalance()  # rebalance
        for add_son in operating_son:
            self.change_leverage(symbol=add_son.symbol, leverage=10)  # 调整杠杆
            if add_son.time_interval.find('m') >= 0:  # 添加循环间隔是分钟的子类的定时任务
                self.scheduler.add_job(add_son.scheduler, trigger='cron', minute='*/' + add_son.time_interval.split('m')[0],
                                       misfire_grace_time=1, max_instances=3, id=add_son.name)
            elif add_son.time_interval.find('h') >= 0:  # 添加循环间隔是小时的子类的定时任务
                self.scheduler.add_job(add_son.scheduler, trigger='cron', hour='*/' + add_son.time_interval.split('h')[0],
                                       misfire_grace_time=1, max_instances=3, id=add_son.name)

        # 执行已有仓位合并
        account = self.exchange.f_account()
        position = pd.DataFrame(account['positions'], dtype=float)  # 所有持仓信息
        position.set_index(['symbol'], drop=False, inplace=True)
        filter_config_symbols = [config['symbol'] for config in filtered_configs.values()]
        position = position[position.symbol.isin(filter_config_symbols) & position.positionAmt != 0]  # 只保留被新增策略管理的币种所持有的合约信息
        symbol_set = set(position.symbol).intersection(set(filter_config_symbols))  # 取交集
        for symbol in symbol_set:
            accorded_son = [son for son in self.son if son.symbol == symbol]
            if accorded_son:
                for son in accorded_son:
                    son.last_price = position.at[symbol, 'entryPrice']  # 上次的开仓价格
                    son.last_amt = position.at[symbol, 'positionAmt'] / len(accorded_son)  # 用于记录上次的开仓的方向和数量（卖空的情况为负数）

    def delete_strategys(self, configs):
        """
        删除策略
        """
        if not configs:
            return
        current_strategys = list(self.config_df.index)
        filtered_son = [k for k in configs.keys() if k in current_strategys]
        clear_symbols = [x.split('_')[0] for x in filtered_son]
        for strategy_name in filtered_son:
            # 从子类中删除
            self.scheduler.remove_job(strategy_name)
            self.clear_strategy_position(strategy_name)
            self.config.pop(strategy_name)  # 更新配置信息
            self.config_df.drop(index=strategy_name, inplace=True)
            for item in self.son[:]:
                if item.name == strategy_name:
                    self.son.remove(item)
                    break

        self.weight_sum = self.config_df['weight'].sum()  # 计算累计权重和
        self.rebalance()

    def clear_symbol_position(self, clear_symbol_name, position_amount):
        """
        币种平指定仓位
        clear_symbol_name : 币种
        position_amount : 待平仓位
        """
        if position_amount == 0:
            return
        self.all_order_info = []
        direction = "BUY" if position_amount < 0 else "SELL"
        param = {"symbol": clear_symbol_name, "side": direction, "type": "MARKET", "quantity": abs(position_amount)}
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.send_order(param))

    def clear_strategy_position(self, strategy_name):
        """
        以市价对指定策略清仓
        """
        for son in self.son:
            if(son.name == strategy_name):
                self.clear_symbol_position(son.symbol, son.last_amt)
                return

    def unset_strategys(self, configs):
        """
        重置持仓
        """
        current_strategys = set(self.config_df.index)
        expected_strategys = set(configs.keys())
        new_position_strategy = expected_strategys - current_strategys
        old_clear_strategy = current_strategys - expected_strategys
        add_config = {key: value for key, value in configs.items() if key in new_position_strategy}
        delete_config = {key: value for key, value in self.config.items() if key in old_clear_strategy}
        self.add_strategeys(add_config)
        self.delete_strategys(delete_config)


class trade_son():
    # 子类只负责计算信号
    def __init__(self, config, exchange, name, df=None, cash_percent=1, test=False):
        # 读取该策略配置
        self.exchange = exchange  # 从母类继承交易所实例
        self.name = name  # 子类名称，用于区分
        self.last_price = np.nan  # 用于记录上次的开仓价格，上次平仓的情况价格应为np.nan
        self.last_amt = 0  # 用于记录上次的开仓的方向和数量（卖空的情况为负数）
        for i in config:  # 读入配置
            exec("self.%s=config['%s']" % (i, i))
        self.df = self.get_data_init(
            num=self.data_num) if df is None else df  # 初始化数据，达到要求的长度，此处留下优化空间，创建实例时如果有传入数据那么将不再重新获得初始数据
        self.to_run = False  # 标记本周期是否该执行的flag，供母类判断
        self.output = None  # 子类本周期运行策略返回的结果
        self.signal = np.nan

    def scheduler(self):  # 供定时器调用，定时将需运行的flag改为True
        self.to_run = True

    def __repr__(self):  # 打印子类信息
        _ret = pd.Series(
            [self.name, self.initial_cash, self.available_cash, self.last_price, self.last_amt, self.symbol,
             self.strategy_name, self.para, self.time_interval])
        _ret.index = ['子类名称', '初始资金', '当前资金', '上次开仓价格', '上次持仓数量', '合约', '策略名', '参数', '时间间隔']
        return _ret.to_string()

    def pos_infer(self, volatility_ratio=0.98):
        # 推断上次开仓价格和数量（注意如果之前是分多次开仓的话开仓价格会不准确）
        output = eval(self.strategy_name)(self.df.copy(), self.para)  # 策略运行，获得返回数据
        output = output[(~np.isnan(output.signal)) & (output.signal != None)]
        if output.shape[0] == 0:
            return
        if output.iloc[-1].signal != 0:
            last_open = output[output.signal != 0]
            if last_open.shape[0] > 0:
                last_open = last_open.iloc[-1]
                self.last_price = float(last_open['close'])
                e = float(self.available_cash)  # 帐户可支配资金
                l = last_open['leverage'] if 'leverage' in last_open.index else self.leverage
                size = float(e * l * volatility_ratio * last_open['signal'] / self.last_price)  # 计算可开的数量
                self.last_amt = float(
                    str(size).split('.')[0] + '.' + str(size).split('.')[1][:self.quantityPrecision]) if str(size).find(
                    '.') >= 0 else size  # 调整精度

    @asyncrun
    async def run(self):  # 供母类调用的运行函数，返回策略信号
        # 计算本周期的交易信号
        self.df = await self.get_data()  # 异步更新数据
        self.output = eval(self.strategy_name)(self.df.copy(), self.para)  # 策略运行，获得返回数据
        print(self.__repr__())
        print(self.output.tail(10))
        self.cal_order_size()  # 计算本周期目标仓位
        self.to_run = False  # 将需运行的flag改为False

    # ===计算目标持仓数量
    def cal_order_size(self, volatility_ratio=0.98, price=None):
        # 通过信号返回本周期的目标仓位
        son_output = self.output
        signal = son_output.signal.iloc[-1]  # 当期信号
        price = float(son_output.close.iloc[-1]) if price is None else price  # 当期信号价格或者按最新价格计算
        e = float(self.available_cash)  # 帐户可支配资金
        # 不超过账户最大杠杆
        l = son_output.leverage.iloc[-1] if 'leverage' in son_output.columns else self.leverage
        self.signal, self.price, self.signal_leverage, self.time = signal, price, l, son_output.candle_begin_time.iloc[
            -1]
        if signal is None:
            self.signal = np.nan
            self.target_amount = np.nan
        elif np.isnan(signal):
            self.target_amount = np.nan
        else:
            size = float(e * l * volatility_ratio / price)  # 计算可开的数量
            size = float(str(size).split('.')[0] + '.' + str(size).split('.')[1][:self.quantityPrecision]) if str(
                size).find(
                '.') >= 0 else size  # 调整精度
            self.target_amount = size * signal

    def get_data_init(self, num=10000, max_len=1000):
        # 为了避免权重超过币安上限，子策略在获取数据的时候暂不采用异步方式
        # 初始化数据，获得足够的数据长度
        # print(str(datetime.datetime.now()),'开始获取数据')
        if self.time_interval.find('m') >= 0:
            self.timedelta = datetime.timedelta(minutes=int(self.time_interval.split('m')[0]))
            start = datetime.datetime.now() - num * self.timedelta
        elif self.time_interval.find('h') >= 0:
            self.timedelta = datetime.timedelta(hours=int(self.time_interval.split('h')[0]))
            start = datetime.datetime.now() - num * self.timedelta
        else:
            print('获取数据暂不支持分钟和小时以外的其他类型')
            return
        symbol_data = []
        earliest = None

        def _get_data(end, max_len=1000):
            time_interval = self.time_interval
            nonlocal symbol_data, start, earliest
            temp_data = self.exchange.get_f_history(symbol=self.symbol, interval=time_interval, endTime=end,
                                                    limit=max_len)
            if len(temp_data) == 0:  # 获取数据的异常处理
                time.sleep(1)
                return _get_data(end, max_len=max_len)
            symbol_data = temp_data + symbol_data
            temp_time = pd.to_datetime(temp_data[0][0], unit='ms') + pd.DateOffset(hours=8)
            print(self.symbol, time_interval, '当前获取到', temp_time)
            if (temp_time < start or temp_time == earliest):
                print(self.symbol, time_interval, '初始化数据完成')
            else:
                earliest = temp_time
                time.sleep(0.2)  # 一秒拿5000根，权重不超过1500
                _get_data(temp_data[0][0], max_len=max_len)

        _get_data(self.exchange.get_timestamp(), max_len=max_len)
        # 注意这里扔掉了最后一根k线
        symbol_data = pd.DataFrame(symbol_data,
                                   columns=['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'end_time',
                                            'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                                            'taker_buy_quote_asset_volume', 'redundant'], dtype=float).iloc[:, :-1]
        symbol_data['candle_begin_time'] = pd.to_datetime(symbol_data['candle_begin_time'], unit='ms') + pd.DateOffset(
            hours=8)
        symbol_data['end_time'] = pd.to_datetime(symbol_data['end_time'], unit='ms') + pd.DateOffset(hours=8)
        if (symbol_data['candle_begin_time'].iloc[-1] + self.timedelta > datetime.datetime.now()):  # K线未结束
            symbol_data = symbol_data.iloc[:-1, :]
        symbol_data.sort_values(by=['candle_begin_time'], inplace=True)
        symbol_data.drop_duplicates(subset=['candle_begin_time'], inplace=True)
        symbol_data.reset_index(inplace=True, drop=True)
        # self.df = symbol_data
        return symbol_data

    # 异步更新1000根k线数据
    @asyncrun
    async def get_data(self, max_len=1000):
        # 异步更新数据
        time = self.exchange.get_timestamp()
        temp = await self.exchange.asyncget_f_history(symbol=self.symbol, interval=self.time_interval, endTime=time,
                                                      limit=max_len)
        # 注意这里扔掉了最后一根k线
        temp = pd.DataFrame(temp, columns=['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'end_time',
                                           'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                                           'taker_buy_quote_asset_volume', 'redundant'], dtype=float).iloc[:, :-1]
        temp['candle_begin_time'] = pd.to_datetime(temp['candle_begin_time'], unit='ms') + pd.DateOffset(hours=8)
        temp['end_time'] = pd.to_datetime(temp['end_time'], unit='ms') + pd.DateOffset(hours=8)
        # 错误排除与重试
        if temp.shape[0] == 0:
            print(str(datetime.datetime.now()), self.symbol, self.time_interval, '获取数据失败,1s后重试')
            await asyncio.sleep(1)
            return self.get_data(max_len=max_len)  # 重新迭代，获取数据
        if (temp['candle_begin_time'].iloc[-1] + self.timedelta > datetime.datetime.now()):  # K线未结束
            temp = temp.iloc[:-1, :]
        df = pd.concat([self.df, temp], axis=0)
        # 数据清洗、去重、除去旧数据
        df.sort_values(by=['candle_begin_time'], inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df = df.iloc[df.shape[0] - self.data_num:, :]  # 只保留data_num行数据
        df.reset_index(inplace=True, drop=True)
        print(str(datetime.datetime.now()), self.symbol, self.time_interval, '最新数据更新成功')
        # self.df = df # 获取成功，更新实例数据库df
        return df
