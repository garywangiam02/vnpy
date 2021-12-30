import ccxt
import pandas as pd
import numpy as np
import time
from datetime import datetime
from pandas.core.frame import DataFrame
from lovekun_1_lib import *

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 180) # 设置打印宽度(**重要**)

SIGNAL_MAP = {
    '多': 1,
    '平': 0,
    '空': -1
}

class LoveKun(StrategyBase):
    """
    坤大费率实盘
    
    赚钱如呼吸般轻松-监控费率及量比和标准差
    https://bbs.quantclass.cn/thread/7093

    """

    name = "坤大费率实盘"

    def prepare_loop(self):
        # 每隔固定时间运行
        print(f'\n❤  LoveKun 坤大费率实盘启动  ❤')
        print(f'  === 策略模式: {self.config.strategy_mode} ===')
        print(f'  === 自动下单: {self.config.enable_place_order} ===')
        print(f'  === 最大杠杆: {self.config.max_leverage} ===')
        print(f'  === 布林极限: {self.config.parameter_width_m} ===')
        print(f'  === 止损比例: {self.config.stop_loss_ratio} ===')
        if self.config.strategy_mode == 'all':
            print(f'  === 黑名单币种: {self.config.strategy_mode_all_black_list} ===')
        if self.config.strategy_mode == 'whitelist':
            symbols = list(self.config.strategy_mode_whitelist_symbols.keys())
            print(f'  === 白名单币种: {symbols} ===')
        print('\n')

        # sleep至目标时间
        run_time, prev_run_time = sleep_by_time_interval(self.config.time_interval, self.config.debug_mode, 0)
        self.run_time = run_time
        if self.config.debug_mode:  # debug模式，不在整点跑，使用上一个整点作为run_time，这样就会删除当前小时没跑完的k线
            self.run_time = prev_run_time

    def execute(self):
        print(f'\n ❤  LoveKun 坤大费率实盘开始执行，计划工作时间: {self.run_time}，当前时间: {datetime.now()}')

        exchange =  ccxt.binance({
            'apiKey': self.config.binance_api_key,
            'secret': self.config.binance_api_secret,
            'timeout': 8000,
            'rateLimit': 10, 
            'verbose': False,
            'hostname': self.config.binance_api_host_name,
            'enableRateLimit': False,
        })

        # 并行获取所有币种K线
        exchange_info = binance_u_furture_get_exchangeinfo(exchange)
        symbol_list = list(filter(lambda s: ((datetime.now().timestamp() * 1000 - int(s['onboardDate'])) / 1000 / 86400) >= self.config.u_future_min_days and s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL', exchange_info['symbols']))
        symbol_list = [s['symbol'] for s in symbol_list]

        # 获取币种最小交易量和下单精度  
        min_qty_dict, price_precision_dict = binance_u_furture_get_trade_rules_from_exchangeinfo(exchange_info) 

        all_klines_df = binance_u_furture_fetch_all_swap_candle_data(exchange, symbol_list, '1h', self.run_time, self.config.kline_num, use_thread=self.config.use_threed)

        print(all_klines_df['BTCUSDT'][['symbol', 'candle_begin_time', 'close']].tail(3))

        # 获取所有U本位资金费率
        premium_info_list = exchange.fapiPublic_get_premiumindex()
        premium_info_dict = {}  # key = symbol, value = lastFundingRate
        for pi in premium_info_list:
            premium_info_dict[pi['symbol']] = pi

        # 获取上一次资金费率
        print(f'读取上一次资金费率文件: {self.config.prev_premium_file}')
        prev_premium_info = pd.DataFrame()
        if os.path.isfile(self.config.prev_premium_file):
            prev_premium_info = pd.read_json(self.config.prev_premium_file)

        # 保存这一次资金费率
        pd.DataFrame(premium_info_dict).to_json(self.config.prev_premium_file)

        # 获取当前仓位
        user_uf_position = binance_u_furture_get_position(exchange, symbol_list)
        print('当前真实仓位\n', user_uf_position)

        # 获取当前虚拟仓位（因为最大杠杆限制未下单的仓位）
        user_virtual_postion = get_all_virtual_pos()
        user_virtual_postion.set_index('symbol', drop=True, inplace=True)
        print('当前虚拟仓位\n', user_virtual_postion)

        volumn_list = []
        rate_list   = []

        # 遍历币种进行分析
        for symbol in symbol_list:
            symbol_df = all_klines_df[symbol].copy()

            # 全币种模式黑名单过滤
            if self.config.strategy_mode == 'all' and symbol in self.config.strategy_mode_all_black_list:
                print(f'{symbol} 在全币种模式黑名单中，跳过')
                continue

            # 获取币种实时费率
            if symbol not in premium_info_dict:
                print(f'币种 {symbol} 资金费率信息未找到，跳过')
                continue

            premium = float(premium_info_dict[symbol]['lastFundingRate'])

            # 获取币种上一次保存的费率
            prev_premium = None  
            if symbol in prev_premium_info:
                prev_premium_time = prev_premium_info[symbol]['time']
                time_diff_hours =  (self.run_time.timestamp() - float(prev_premium_time) / 1000) / 3600
                
                # print(f'{symbol} 保存费率小时差: {time_diff_hours}')

                # 上一次保存的费率与现在时间相差不超过1.1个小时，才算有效
                if time_diff_hours < 1.1:
                    prev_premium = float(prev_premium_info[symbol]['lastFundingRate'])
                    
            # print(f'{symbol} 上一次费率: {prev_premium}, 当前费率: {premium}')

            # 计算各种指标，源自坤大和Ryan萨博帖子
            df1 = pd.DataFrame(columns=['symbol', 'volume', 'WIDTH_m', 'Annualized'], index=['0'])
            dm1 = pd.DataFrame(columns=['symbol', 'Rate', 'volume', 'WIDTH_m'], index=['0'])
            n = 50
            m = 2
            w = 20

            # ===计算指标
            # 计算均线
            symbol_df['volume_m'] = symbol_df['volume'].rolling(176, min_periods=1).mean()
            symbol_df['volume_2'] = symbol_df['volume'] / symbol_df['volume_m']
            volume_2 = symbol_df.iloc[-1]['volume_2']
            df1['volume_m'] = volume_2
            symbol_df['median'] = symbol_df['close'].rolling(n, min_periods=1).mean()
            # 计算上轨、下轨道
            symbol_df['std'] = symbol_df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
            symbol_df['WIDTH'] = 2 * m * symbol_df['std'] / symbol_df['median']
            # df['WIDTH2'] = df['WIDTH'].rolling(w, min_periods=1).mean()
            symbol_df['WIDTHm'] = symbol_df['WIDTH'].rolling(900, min_periods=1).min()
            symbol_df['WIDTH2'] = symbol_df['WIDTH'] / symbol_df['WIDTHm']
            WIDTH_m = symbol_df.iloc[-1]['WIDTH2']
            symbol_df["change_2h"] = symbol_df["close"].pct_change(2)  #获取2h涨跌幅

            lastFundingRate = round(premium * 100, 3)
            Annualized =  str(round(premium * 100*3*360, 1))+"%"  #  年化
            annualization = str(round(premium * 100*3*7, 1))+"%"  #  七年化

            df1['symbol'] = symbol
            df1['WIDTH_m'] = WIDTH_m
            df1['Annualized'] = Annualized

            dm1['symbol'] = symbol
            dm1['volume_m'] = volume_2
            dm1['WIDTH_m'] = WIDTH_m
            dm1['Annualized'] = Annualized
            dm1['Rate'] = lastFundingRate

            # 计算EMA均线
            # EMA（today）=α * Price（today） + ( 1 - α ) * EMA（yesterday）
            # 坤大使用aicoin的计算方式，即 α = 2/(N+1)
            # pandas计算ema参考文档：https://blog.csdn.net/weixin_41494909/article/details/99670246
            symbol_df : DataFrame = symbol_df
            symbol_df['ma8'] = symbol_df['close'].ewm(alpha=2/(8 + 1), adjust=False).mean()
            symbol_df['ma14'] = symbol_df['close'].ewm(alpha=2/(14 + 1), adjust=False).mean()
            symbol_df['ma21'] = symbol_df['close'].ewm(alpha=2/(21 + 1), adjust=False).mean()
            symbol_df['ma55'] = symbol_df['close'].ewm(alpha=2/(55 + 1), adjust=False).mean()
            symbol_df['ma480'] = symbol_df['close'].ewm(alpha=2 / (480 + 1), adjust=False).mean()  # 快牛熊均线
            #  -------------------------------------------------------------------------------------------
            #  翻转y坐标计算做空曲线
            symbol_df['-ma8'] = 0-symbol_df['close'].ewm(alpha=2/(8 + 1), adjust=False).mean()
            symbol_df['-ma14'] = 0-symbol_df['close'].ewm(alpha=2/(14 + 1), adjust=False).mean()
            symbol_df['-ma21'] = 0-symbol_df['close'].ewm(alpha=2/(21 + 1), adjust=False).mean()
            symbol_df['-ma55'] = 0-symbol_df['close'].ewm(alpha=2/(55 + 1), adjust=False).mean()
            #  -------------------------------------------------------------------------------------------

            last_ma8  = symbol_df['ma8'].iloc[-1]
            last_ma14 = symbol_df['ma14'].iloc[-1]
            last_ma21 = symbol_df['ma21'].iloc[-1]
            last_ma55 = symbol_df['ma55'].iloc[-1]
            last_ma480 = symbol_df['ma480'].iloc[-1]
            last_close  = symbol_df['close'].iloc[-1]
            #  -------------------------------------------------------------------------------------------
            last_ma8_ = symbol_df['-ma8'].iloc[-1]
            last_ma14_ = symbol_df['-ma14'].iloc[-1]
            last_ma21_ = symbol_df['-ma21'].iloc[-1]
            last_ma55_ = symbol_df['-ma55'].iloc[-1]
            last_close_ = 0-symbol_df['close'].iloc[-1]
            #  -------------------------------------------------------------------------------------------

            last_change2h = symbol_df.iloc[-1]['change_2h']
            curr_pos    = 0     # 当前持仓方向
            entry_price = 0     # 开仓均价
            mark_price  = 0     # 标记价格
            curr_amt = 0        # 当前持仓量
            is_virtual_pos = 0  # 是否是虚拟仓位

            # 根据真实仓位获取仓位信息
            if symbol in user_uf_position.index:
                symbol_pos = user_uf_position.loc[symbol]
                curr_amt = symbol_pos['当前持仓量']
                if curr_amt > 0:
                    curr_pos = 1
                elif curr_amt < 0:
                    curr_pos = -1
                entry_price = symbol_pos['当前持仓均价']
                mark_price = symbol_pos['当前标记价格']
            
            # 根据虚拟仓位获取仓位信息
            if curr_amt == 0 and symbol in user_virtual_postion.index:
                # 根据虚拟仓位获取仓位信息
                symbol_pos = user_virtual_postion.loc[symbol]
                curr_pos = symbol_pos['pos']
                entry_price = symbol_pos['price']
                mark_price = binance_u_furture_fetch_ticker_price(exchange, pd.DataFrame({}, index=[symbol])).loc[symbol]
                is_virtual_pos = 1

            # print(f'虚拟={is_virtual_pos}, 持仓均价={round(entry_price, 2)}\t标记价格={round(mark_price, 2)}\tEMA(8)={round(last_ma8, 2)}\tEMA(14)={round(last_ma14, 2)}\tEMA(21)={round(last_ma21, 2)}\tEMA(55)={round(last_ma55, 2)}\tEMA(2880)={round(last_ma2880, 2)}\tClose={round(last_close, 2)}\t{symbol}')

            signal = ''     # 产生的信号
            # 1. 当前为空仓
            # 1.0 判断牛熊线,牛线之上只做多,牛线之下反手做空.
            # 1.01判断涨跌幅，前几根k线涨跌大于0.25，不参与多单，跌幅大于-0.25，不参与空单
            # 1.1 费率<0 & 布林极限>3 & MA8>MA14 & MA21>MA55 & 突破MA14 => 做多 （下跌反弹过程不做多，趋势稳定才上车）
            # 1.2 费率<0 & 布林极限>3 & abs(费率)<abs(上一次费率) & 跌破MA21 => 做空
            # print(symbol, last_change2h) # 打印2h涨跌幅

            if curr_pos == 0:
                if last_ma480 < last_close:
                    if lastFundingRate > -0.74:  # 避免踩坑(列如要空投,解锁等币种),此类币种拉砸都非常突然
                        if last_change2h < self.config.price_change_pct_filter_long:
                            if (lastFundingRate < 0) and (WIDTH_m > self.config.parameter_width_m) and (last_ma8 > last_ma14) and (last_ma21 > last_ma55) and (last_close > last_ma14):
                                signal = '多'
                elif last_ma480 >= last_close:
                    if lastFundingRate > -0.74:   # 避免踩坑(列如要空投,解锁等币种),此类币种拉砸都非常突然
                        if last_change2h < self.config.price_change_pct_filter_long:
                            if (lastFundingRate < 0) and (WIDTH_m > self.config.parameter_width_m) and (last_ma8_ > last_ma14_) and (last_ma21_ > last_ma55_) and (last_close_ > last_ma14_):
                                signal = '空'

            # 2. 当前为做多
            # 2.1 固定比例止损: 标记价格<开仓均价*(1-止损参数) => 平仓
            # 2.2 MA55>MA21 & 判断牛熊
            #   2.2.1 费率<0 & abs(费率)<abs(上一次费率) & 跌破MA21 => 做空
            #   2.3.2  费率>0 & 布林极限>15 & close<MA14 => 平仓
            #   2.3.3  费率>0 & 布林极限<15 & close<MA21 => 平仓
            #   2.3.4  费率<=0 & 布林极限>15 & close<MA14 => 平仓
            #   2.3.5  费率<=0 & 布林极限>5 & close<MA21 => 平仓
            #   2.3.6  费率<=0 & 布林极限<5 & close<MA55 => 平仓
            #   2.3.7  费率<=0 & 布林极限>5 & close>MA21 => 观望
            #   2.3.8  费率<=0 & 布林极限<5 & close>MA55 => 观望
            #   2.3.9  费率>0 & 布林极限>5 & close>MA21 => 观望
            #   2.3.10 费率>0 & 布林极限<5 & close>MA55 => 观望
            elif curr_pos == 1:
                if mark_price < entry_price * (1 - self.config.stop_loss_ratio):
                    signal = '平'
                if last_ma55 > last_ma21 and last_ma480 >= last_close:
                    if (lastFundingRate < 0) and (prev_premium is not None) and (abs(lastFundingRate) < abs(prev_premium)) and (last_close < last_ma21_):
                        signal = '空'
                elif (WIDTH_m > 10) and (last_close < last_ma14):
                    signal = '平'
                elif (WIDTH_m >= 3) and (WIDTH_m <= 10) and (last_close < last_ma21):
                    signal = '平'
                elif (WIDTH_m < 3) and (last_close < last_ma55):
                    signal = '平'
                elif signal == '' and (lastFundingRate <= 0) and (WIDTH_m >= 3) and (last_close > last_ma21):
                    signal = '观望'
                elif signal == '' and (lastFundingRate <= 0) and (WIDTH_m <= 3) and (last_close > last_ma55):
                    signal = '观望'
                elif signal == '' and (lastFundingRate > 0) and (WIDTH_m >= 3) and (last_close > last_ma21):
                    signal = '观望'
                elif signal == '' and (lastFundingRate > 0) and (WIDTH_m <= 3) and (last_close > last_ma55):
                    signal = '观望'

            # 3. 当前为做空
            # 3.1 固定比例止损: 标记价格>开仓均价*(1+止损参数) => 平仓
            # 3.2 MA55<21& 判断牛熊,牛熊线以上
            #   3.2.1 费率<0 & -MA8>-MA14 & 突破MA14 => 做多
            #   3.3.2 费率>0 & 布林极限>5 & close>MA21 => 平仓
            #   3.3.3 费率>0 & 布林极限<5 & close>MA55 => 平仓
            #   3.3.4 费率<=0 & 布林极限>15 & close>MA14 => 平仓
            #   3.3.5 费率<=0 & 布林极限<15 & close>MA21 => 平仓
            #   3.3.6 费率>=0 & 布林极限>5 & close<MA21 => 观望
            #   3.3.7 费率>=0 & 布林极限<5 & close<MA55 => 观望
            #   3.3.8 费率<0 & 布林极限>5 & close<MA21 => 观望
            #   3.3.9 费率<0 & 布林极限<5 & close<MA55 => 观望
            elif curr_pos == -1:
                if mark_price > entry_price * (1 + self.config.stop_loss_ratio):
                    signal = '平'
                if last_ma55 < last_ma21 and last_ma480 < last_close:
                    if (lastFundingRate < 0) and (last_ma8 > last_ma14) and (last_close > last_ma14):
                        signal = '多'
                elif (WIDTH_m >= 3) and (WIDTH_m <= 10)(last_close_ < last_ma21_):
                    signal = '平'
                elif (WIDTH_m < 3)(last_close_ < last_ma55_):
                    signal = '平'
                elif (WIDTH_m > 10)(last_close_ < last_ma14_):
                    signal = '平'
                elif signal == '' and (lastFundingRate >= 0) and (WIDTH_m >= 3) and (last_close_ > last_ma21_):
                    signal = '观望'
                elif signal == '' and (lastFundingRate >= 0) and (WIDTH_m <= 3) and (last_close_ > last_ma55_):
                    signal = '观望'
                elif signal == '' and (lastFundingRate < 0) and (WIDTH_m >= 3) and (last_close_ > last_ma21_):
                    signal = '观望'
                elif signal == '' and (lastFundingRate < 0) and (WIDTH_m <= 3) and (last_close_ > last_ma55_):
                    signal = '观望'
            # print(f'EMA(8)={round(last_ma8, 3)}\tEMA(14)={round(last_ma14, 3)}\tEMA(21)={round(last_ma21, 3)}\tEMA(55)={round(last_ma55, 3)}\t{symbol}    Close={round(last_close, 3)}')
            # print(f'EMA(8)={round(last_ma8_, 3)}\tEMA(14)={round(last_ma14_, 3)}\tEMA(21)={round(last_ma21_, 3)}\tEMA(55)={round(last_ma55_, 3)}\t{symbol}    Close={round(last_close_, 3)}')
            # print(f'EMA(8)={round(last_ma8, 3)}\tEMA(14)={round(last_ma14, 3)}\tEMA(21)={round(last_ma21, 3)}\tEMA(55)={round(last_ma55, 3)}\t{symbol}    Close={round(last_close, 3)}')
            # print(last_ma2880, last_close)
            dm1['信号'] = signal
            dm1['Close'] = last_close
            # print(f'{symbol} curr_pos={curr_pos}, signal={signal}')
            volumn_df = df1[['symbol', 'volume_m', 'WIDTH_m', 'Annualized']]
            volumn_list.append(volumn_df)
            rate_df = dm1[['symbol', 'Rate', 'volume_m', 'WIDTH_m', 'Close', '信号']]
            rate_list.append(rate_df)

        all_volumn_df = pd.concat(volumn_list, ignore_index=True)
        all_volumn_df.sort_values(by='symbol', inplace=False)
        all_volumn_df.reset_index(drop=False, inplace=False)
        all_volumn_df = all_volumn_df.sort_values(by=['volume_m', 'WIDTH_m', 'Annualized'], ascending=False)

        all_rate_df = pd.concat(rate_list, ignore_index=True)
        all_rate_df.sort_values(by='symbol', inplace=False)
        all_rate_df.reset_index(drop=False, inplace=False)
        all_rate_df = all_rate_df.sort_values(by=['Rate', 'volume_m', 'WIDTH_m'], ascending=True)
        
        min_rate_df = all_rate_df.head(20)          # 查看费率最小的前10
        max_vol_df  = all_volumn_df.head(20)        # 查看量比最大的前10

        print('--- 按负费率排名 ---')
        print(min_rate_df, '\n')

        print('--- 按量比排名 ---')
        print(max_vol_df, '\n')

        print('--- 有信号的币种 ---')
        has_signal_rate_df = all_rate_df[all_rate_df['信号'] != '']
        print(has_signal_rate_df)

        msg = ""

        # -- 产生信号的币种输出钉钉
        if len(has_signal_rate_df) > 0:
            signal_df = has_signal_rate_df.copy()
            signal_df['symbol'] = signal_df['symbol'].str.replace('USDT', '')
            signal_df.set_index('symbol', drop=True, inplace=True)
            signal_df.rename(columns={ 'Rate': '费率', 'volume_m': '量比', 'WIDTH_m': '布林极限', 'Close': '价格' }, inplace=True)  
            signal_df['费率'] = round(signal_df['费率'], 3)
            signal_df['量比'] = round(signal_df['量比'], 3)
            signal_df['布林极限'] = round(signal_df['布林极限'], 3)
            signal_df['价格'] = round(signal_df['价格'], 3)
            msg += (signal_df.to_string() + '\n\n')

        # -- 费率最低的币种输出钉钉
        if len(min_rate_df[min_rate_df['Rate'] < 0]) > 0:
            min_rate_df : pd.DataFrame = min_rate_df[min_rate_df['Rate'] < 0].copy()
            min_rate_df['symbol'] = min_rate_df['symbol'].str.replace('USDT', '')
            min_rate_df.set_index('symbol', drop=True, inplace=True)
            min_rate_df.rename(columns={ 'Rate': '费率', 'volume_m': '量比', 'WIDTH_m': '布林极限', 'Close': '价格' }, inplace=True)  
            min_rate_df['费率'] = round(min_rate_df['费率'], 3)
            min_rate_df['量比'] = round(min_rate_df['量比'], 3)
            min_rate_df['布林极限'] = round(min_rate_df['布林极限'], 3)
            min_rate_df['价格'] = round(min_rate_df['价格'], 3)
            msg += (min_rate_df.to_string() + '\n\n')

        # if len(max_vol_df[max_vol_df['volume_m'] > 10]) > 0 or len(max_vol_df[max_vol_df['WIDTH_m']> 10]) > 0:
        #     max_vol_df = max_vol_df.copy()
        #     max_vol_df['symbol'] = max_vol_df['symbol'].str.replace('USDT', '')
        #     max_vol_df.set_index('symbol', drop=True, inplace=True)
        #     max_vol_df.rename(columns={ 'volume_m': '量比', 'WIDTH_m': '布林极限', 'Annualized': '年化' }, inplace=True) 
        #     max_vol_df['量比'] = round(max_vol_df['量比'], 3)
        #     max_vol_df['布林极限'] = round(max_vol_df['布林极限'], 3)
        #     msg += (max_vol_df.to_string())

        # 钉钉推送(无下单时)
        if len(msg) > 0 and self.config.dd_enable and not self.config.enable_place_order:
            send_dingding_msg('=== 坤大赚钱如呼吸策略 ===\n\n' + msg, self.config.dd_root_id, self.config.dd_secret)

        # ============================
        # 根据信号自动下单
        # ===========================
        print(f'是否开启下单: {self.config.enable_place_order}')

        # 查询账户余额
        balance_usdt = 0
        balance_unPnl = 0
        balance_total = 0

        if self.config.enable_place_order:
            balance = retry_wrapper(exchange.fapiPrivateV2_get_balance, act_name='获取U本位合约账户余额')
            balance = pd.DataFrame(balance)
            balance = balance[balance['asset'] == 'USDT']
            balance_usdt = round(float(balance['balance'].iloc[0]))
            balance_unPnl = round(float(balance['crossUnPnl'].iloc[0]))
            balance_total = balance_usdt + balance_unPnl
            print(f'-- 账户余额: ${balance_usdt}(${balance_unPnl}) = ${balance_total}')

        # debug不下单模式，如果资产为0，则设置一个假余额，方便测试
        if self.config.debug_mode and balance_total == 0 and not self.config.enable_place_order:
            balance_total = 1000

        signal_orders = []
        for _, row in has_signal_rate_df.iterrows():
            symbol = row['symbol']
            signal = row['信号']
            signal_val = None

            if signal in SIGNAL_MAP:
                signal_val = SIGNAL_MAP[signal]
                if self.config.strategy_mode == 'all': 
                    o = {'symbol': symbol, 'signal': signal_val, 'one_order_usdt': self.config.strategy_mode_all_one_order_usdt, 'slippage': self.config.strategy_mode_all_slippage}
                    o['order_usdt'] = float(math.floor(balance_total * self.config.strategy_mode_all_balace_ratio))
                    signal_orders.append(o)
                if self.config.strategy_mode == 'whitelist' and symbol in self.config.strategy_mode_whitelist_symbols: 
                    o = self.config.strategy_mode_whitelist_symbols[symbol].copy()
                    o['symbol'] = symbol
                    o['signal'] = signal_val
                    o['order_usdt'] = float(math.floor(balance_total * o['balance_ratio']))
                    signal_orders.append(o)
        
        # 下单前判断是否要跳过资金费率结算点
        # 注意：运行环境timezone需要是utc+8的倍数
        sleep_if_in_special_hours()

        # 下单
        batch_orders, failed_orders = place_order(run_time=self.run_time, exchange=exchange, signals=signal_orders, user_uf_position=user_uf_position, user_virtual_position=user_virtual_postion, min_qty=min_qty_dict, price_precision=price_precision_dict, balance_total=balance_total, max_leverage=self.config.max_leverage, is_real_place_order=self.config.enable_place_order)            

        # 钉钉推送(有下单时)
        if len(msg) > 0 and self.config.dd_enable: #and self.config.enable_place_order:
            if len(batch_orders) > 0 and len(batch_orders[0]) > 0:
                msg += f'\n\n--- 下单 ---\n'
                order_total = 0
                for bo in batch_orders:
                    for o in bo:
                        order_usdt = abs(round(o['实际下单资金']))
                        order_total += order_usdt
                        msg += f"[{o['批次'].split('-')[0]}] {o['symbol'].replace('USDT', '')}  {o['side']}  ${order_usdt}\n"
                msg += f'\n总计: ${order_total}\n'

            if len(failed_orders) > 0:
                msg += '\n*** 失败订单 ***\n'
                for fo in failed_orders:
                    msg += f"{fo['symbol']} {fo['失败原因']}\n"

            msg += f'\n\n账户当前净值：${balance_usdt} (${balance_unPnl}) = ${round(balance_usdt + balance_unPnl)}\n'

            # 更新后的持仓
            user_uf_position = binance_u_furture_get_position(exchange, symbol_list)
            user_uf_position = user_uf_position[user_uf_position['当前持仓量'] != 0]

            if len(user_uf_position) > 0:
                msg += '\n最新持仓: \n'
                for symbol, row in user_uf_position.iterrows():
                    direction = "多" if row["当前持仓量"] > 0 else "空"
                    msg += f'{symbol} ${round(row["当前持仓价值"], 2)} {direction}\n'

            # 更新后的虚拟持仓
            user_virtual_postion = get_all_virtual_pos()
            if len(user_virtual_postion) > 0:
                msg += '\n最新虚拟持仓: \n'
                for _, row in user_virtual_postion.iterrows():
                    msg += f'{row["run_time"]} {row["symbol"]} {row["pos"]} {row["price"]}\n'

            send_dingding_msg('=== 坤大赚钱如呼吸策略 ===\n\n' + msg, self.config.dd_root_id, self.config.dd_secret)

    def end(self):
        """
        每轮执行策略后的收尾工作
        """
        if self.config.debug_mode:
            print('--- 调试模式, 10s 后退出')
            time.sleep(10)
            exit()

if __name__ == '__main__':
    runner = StrategyRunner()
    runner.run(LoveKun())
