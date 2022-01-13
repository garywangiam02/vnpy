


from datetime import timedelta
from multiprocessing import Pool, cpu_count
from datetime import datetime
import Signals
from Position import *
from Evaluate import *
from Function import *
from Statistics import *
from functools import partial

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数

# =====回测固定参数
tag = '20210601'  # 本次回测标记

leverage_rate = 2
slippage = 1 / 1000  # 滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓


drop_days = 0  # 币种刚刚上线10天内不交易

# 基于币安回测 或 基于OKEx回测（OKEx为邢大原版）
binance_or_okex = 'binance' # or okex

begin_time = '2020-01-01'


# =====批量遍历策略参数
# ===单次循环
def calculate_by_one_loop(para, df, signal_name, symbol, rule_type):
    _df = df.copy()
    # 计算交易信号
    _df = getattr(Signals, signal_name)(_df, para=para)
    # 计算实际持仓
    if binance_or_okex == 'okex':
        _df = position_for_OKEx_future(_df) # 邢大原版
    else:
        _df = position_for_binance_future(_df)# 币安

    # 币种上线10天之后的日期
    t = _df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
    _df = _df[_df['candle_begin_time'] > t]
    # 计算资金曲线
    if binance_or_okex == 'okex':
        face_value = get_symbol_face_value(symbol)
        _df = equity_curve_for_OKEx_USDT_future_next_open(_df, slippage=slippage, c_rate=c_rate,
                                                          leverage_rate=leverage_rate,
                                                          face_value=face_value,
                                                          min_margin_ratio=min_margin_ratio)
    else:
        quantity_precision = get_symbol_quantity_precision(symbol)
        _df = equity_curve_for_Binance_USDT_future_next_open(_df, slippage=slippage, c_rate=c_rate,
                                                             leverage_rate=leverage_rate,
                                                             quantity_precision=quantity_precision,
                                                             min_margin_ratio=min_margin_ratio)

    # 保存策略收益
    rtn = pd.DataFrame()
    rtn.loc[0, 'para'] = str(para)
    r = _df.iloc[-1]['equity_curve']
    rtn.loc[0, '累计净值'] = r  # 最终收益
    rtn.loc[0, '年化收益'], rtn.loc[0, '最大回撤'], rtn.loc[0, '年化收益回撤比'] = return_drawdown_ratio(_df)
    print(signal_name, symbol, rule_type, para, '策略收益：', r)
    return rtn





if __name__ == '__main__':
    
    for signal_name in ['adaptboll']:
        for symbol in ['ETH-USDT', 'BTC-USDT', 'ETH-BTC']:
            for rule_type in ['4H', '2H', '1H', '30T', '15T']:
            # for rule_type in ['1H']:
                print(signal_name, symbol, rule_type)
                print('开始遍历该策略参数：', signal_name, symbol, rule_type)

                # ===读入数据
                # df = pd.read_pickle(root_path + '/data/%s-USDT_5m.pkl' % symbol)
                df = pd.read_pickle(root_path + '/data/pickle_data/spot/%s_5m.pkl' % symbol)
                # 任何原始数据读入都进行一下排序、去重，以防万一
                df.sort_values(by=['candle_begin_time'], inplace=True)
                df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
                df.reset_index(inplace=True, drop=True)

                # =====转换为其他分钟数据
                period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
                    {'open': 'first',
                     'high': 'max',
                     'low': 'min',
                     'close': 'last',
                     'volume': 'sum',
                     # 'quote_volume': 'sum',
                     # 'trade_num': 'sum',
                     # 'taker_buy_base_asset_volume': 'sum',
                     # 'taker_buy_quote_asset_volume': 'sum',
                     })
                period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
                period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
                period_df.reset_index(inplace=True)
                # df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
                #                 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']]
                df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
                df = df[df['candle_begin_time'] >= pd.to_datetime(begin_time)]
                df.reset_index(inplace=True, drop=True)

                # ===获取策略参数组合
                para_list = getattr(Signals, signal_name+'_para_list')()

                # ===并行回测
                start_time = datetime.now()  # 标记开始时间

                # 利用partial指定参数值
                part = partial(calculate_by_one_loop, df=df, signal_name=signal_name, symbol=symbol, rule_type=rule_type)

                with Pool(max(cpu_count() - 1, 1)) as pool:
                    # 使用并行批量获得data frame的一个列表
                    df_list = pool.map(part, para_list)
                    print('读入完成, 开始合并', datetime.now() - start_time)
                    # 合并为一个大的DataFrame
                    para_curve_df = pd.concat(df_list, ignore_index=True)

                # ===输出
                para_curve_df.sort_values(by='年化收益回撤比', ascending=False, inplace=True)
                print(para_curve_df.head(10))

                # ===存储参数数据
                p = root_path + '/data/output/para/%s-%s-%s-%s-%s.csv' % (signal_name, symbol.replace('-', '_'), leverage_rate, rule_type, tag)
                if not os.path.exists(root_path + '/data/output/para/'):
                    os.makedirs(root_path + '/data/output/para/')

                pd.DataFrame().to_csv(p, index=False)
                para_curve_df.to_csv(p, index=False, mode='a')



