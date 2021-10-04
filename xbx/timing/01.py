import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from xbx.utils import DATA_PATH

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

stock_code = 'sz000002'
df = pd.read_csv('{}/择时策略-回测/{}.csv'.format(DATA_PATH, stock_code),
                 encoding='gbk', skiprows=1, parse_dates=['交易日期'])

# 任何原始数据读入都进行一下排序、去重，以防万一
df.sort_values(by=['交易日期'], inplace=True)
df.drop_duplicates(subset=['交易日期'], inplace=True)
df.reset_index(inplace=True, drop=True)


# =====计算后复权价
df['涨跌幅'] = df['收盘价'] / df['前收盘价'] - 1
df['复权因子'] = (1 + df['涨跌幅']).cumprod()
df['收盘价_复权'] = df['复权因子'] * (df.iloc[0]['收盘价'] / df.iloc[0]['复权因子'])
df['开盘价_复权'] = df['开盘价'] / df['收盘价'] * df['收盘价_复权']
df['最高价_复权'] = df['最高价'] / df['收盘价'] * df['收盘价_复权']
df['最低价_复权'] = df['最低价'] / df['收盘价'] * df['收盘价_复权']
df.drop(['复权因子'], axis=1, inplace=True)


# =====计算涨跌停价格
df['涨停价'] = df['前收盘价'] * 1.1
df['跌停价'] = df['前收盘价'] * 0.9
# 四舍五入
# print(round(3.5), round(4.5))  # 银行家舍入法：四舍六进，五，奇进偶不进
df['涨停价'] = df['涨停价'].apply(lambda x: float(Decimal(x * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 100))
df['跌停价'] = df['跌停价'].apply(lambda x: float(Decimal(x * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 100))


# =====计算移动平均线策略的交易信号

# ===策略参数
para_list = [10, 90]
ma_short = para_list[0]  # 短期均线。ma代表：moving_average
ma_long = para_list[1]  # 长期均线

# ===计算均线。所有的指标，都要使用复权价格进行计算。
df['ma_short'] = df['收盘价_复权'].rolling(ma_short, min_periods=1).mean()
df['ma_long'] = df['收盘价_复权'].rolling(ma_long, min_periods=1).mean()

# ===找出做多信号
condition1 = df['ma_short'] > df['ma_long']  # 短期均线 > 长期均线
condition2 = df['ma_short'].shift(1) <= df['ma_long'].shift(1)  # 上一周期的短期均线 <= 长期均线
df.loc[condition1 & condition2, 'signal'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

# ===找出做多平仓信号
condition1 = df['ma_short'] < df['ma_long']  # 短期均线 < 长期均线
condition2 = df['ma_short'].shift(1) >= df['ma_long'].shift(1)  # 上一周期的短期均线 >= 长期均线
df.loc[condition1 & condition2, 'signal'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

# ===删除无关中间变量
df.drop(['ma_short', 'ma_long'], axis=1, inplace=True)

# =====将数据存入hdf文件中
df.to_hdf('{}/择时策略-回测/signals.h5'.format(DATA_PATH), key='df', mode='w')

# 案例_产生实际持仓
# ===导入数据
df = pd.read_hdf('{}/择时策略-回测/signals.h5'.format(DATA_PATH), key='df')


# ===由signal计算出实际的每天持有仓位
# 在产生signal的k线结束的时候，进行买入
df['signal'].fillna(method='ffill', inplace=True)
df['signal'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
df['pos'] = df['signal'].shift()
df['pos'].fillna(value=0, inplace=True)  # 将初始行数的pos补全为0

# ===对涨跌停无法买卖做出相关处理。
# 找出收盘价无法买入的K线
cannot_buy_condition = df['收盘价'] >= df['涨停价']
# 将找出上一周期无法买入的K线、并且signal为1时，的'pos'设置为空值
df.loc[cannot_buy_condition.shift() & (df['signal'].shift() == 1), 'pos'] = None  # 2010-12-22

# 找出收盘价无法卖出的K线
cannot_sell_condition = df['收盘价'] <= df['跌停价']
# 将找出上一周期无法卖出的K线、并且signal为0时的'pos'设置为空值
df.loc[cannot_sell_condition.shift() & (df['signal'].shift() == 0), 'pos'] = None

# pos为空的时，不能买卖，只能和前一周期保持一致。
df['pos'].fillna(method='ffill', inplace=True)


# ===如果是分钟级别的数据，还需要对t+1交易制度进行处理，在之后案例中进行演示


# ===删除无关中间变量
df.drop(['signal'], axis=1, inplace=True)


# ===将数据存入hdf文件中
df.to_hdf('{}/择时策略-回测/pos.h5'.format(DATA_PATH), key='df', mode='w')
print(df)
