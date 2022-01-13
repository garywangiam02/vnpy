'''
此处参考邢大择时魔改小组的 1_查看单个策略结果.py
修改：
1.symbol_face_value - binance用下单量精度修改后代替
2.position_for_OKEx_future - 计算实际持仓，需要改为binance版本
3.equity_curve_for_OKEx_USDT_future_next_open - 资金曲线计算资金曲线，需要改为binance版本
'''

from datetime import timedelta
import Signals
from Position import *
from Evaluate import *
from Function import *
from Statistics import *
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数


# =====手工设定策略参数
symbol = 'ETH-BTC'
para = [240, 0.05]
signal_name = 'adaptboll'
rule_type = '1H'


c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
slippage = 1 / 1000  # 滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
leverage_rate = 2
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓
# drop_days = 10  # 币种刚刚上线10天内不交易

# 获取对应币种的数量精度，用于计算下单量，最终计算资金曲线
quantity_precision = get_symbol_quantity_precision(symbol)

# 注意如果是ETH/BTC这样的币对，face_value是不太准确的，统一设为1
face_value = get_symbol_face_value(symbol)


# 读取数据
df = pd.read_pickle(root_path + '/data/pickle_data/spot/%s_5m.pkl' % symbol)

# 任何原始数据读入都进行一下排序、去重，以防万一
df.sort_values(by=['candle_begin_time'], inplace=True)
df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
df.reset_index(inplace=True, drop=True)
# print(df)

# =====转换为其他分钟数据
period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
    {'open': 'first',
     'high': 'max',
     'low': 'min',
     'close': 'last',
     # 目前仅保留ohlc数据
     # 'volume': 'sum',
     # 'quote_volume': 'sum',
     # 'trade_num': 'sum',
     # 'taker_buy_base_asset_volume': 'sum',
     # 'taker_buy_quote_asset_volume': 'sum',
     })

period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
# period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
period_df.reset_index(inplace=True)
df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close']]
df = df[df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
df.reset_index(inplace=True, drop=True)



# =====计算交易信号
df = getattr(Signals, signal_name)(df, para=para)

# =====计算实际持仓
df = position_for_binance_future(df) # 邢大原OKEx框架调用的是position_for_OKEx_future()

# =====计算资金曲线
# 选取相关时间。币种上线10天之后的日期
# t = df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
# df = df[df['candle_begin_time'] > t]
# df = df[df['candle_begin_time'] >= pd.to_datetime('2018-01-01')]


# 计算资金曲线函数，邢大原版为OKEx框架，根据合约面值计算下单量，币安根据下单量精度计算
df_binance = equity_curve_for_Binance_USDT_future_next_open(df.copy(), slippage=slippage, c_rate=c_rate,
                                                    leverage_rate=leverage_rate,
                                                    quantity_precision=quantity_precision,
                                                    min_margin_ratio=min_margin_ratio)


# 邢大原版OKEx框架计算的资金曲线，可用于做对比，结果差不多
df_okex = equity_curve_for_OKEx_USDT_future_next_open(df.copy(), slippage=slippage, c_rate=c_rate, leverage_rate=leverage_rate,
                                                      face_value=face_value, min_margin_ratio=min_margin_ratio)




print(f'策略 {signal_name} 在 Binanace 上的最终收益：', df_binance.iloc[-1]['equity_curve'].round(2),
      f' ** {symbol} 自身收益：', round(df_binance.iloc[-1]['close'] / df_binance.iloc[0]['close'], 2))

print(f'策略 {signal_name} 在 OKEx 上的最终收益：', df_okex.iloc[-1]['equity_curve'].round(2),
      f' ** {symbol} 自身收益：', round(df_okex.iloc[-1]['close'] / df_okex.iloc[0]['close'], 2))
