# -*- coding: utf-8 -*-
"""
提示：
    1、代码注释部分，有“修改”、“不需修改”字样的，如果是小白，建议按注释执行。大佬就随意了。
    2、作图的数据源的df_draw，字段必须是这样，signal是图上出信号用的，那三个就是三条线，如果你增加字段就可以是四、五、六条了，字段名无所谓
    ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'median', 'upper', 'lower', 'signal']
    其中，df_draw的前几个字段必须是'candle_begin_time', 'open', 'high', 'low', 'close', 'volume'，顺序不要改变；
    signal字段必须有，最好放在最后。
    其它的字段，就是画图上的线。上面的是三条线：上中下轨。增加一个字段，就会增加一条线，数量可增加很多。
    函数执行完后，会生成一个echarts.html文件，用浏览器打开这个文件即可
"""
import numpy as np
import pandas as pd


def to_ymd(time):
    return pd.Timestamp(time).strftime("%Y%m%d")


pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: f'{x:.2f}')

coin_value_table = {
    'BTC-USDT': 0.01,
    'ETH-USDT': 0.1,
    'LTC-USDT': 1,
    'EOS-USDT': 10,
    'BCH-USDT': 0.1,
    'XRP-USDT': 100,
    'ETC-USDT': 10,
    'BSV-USDT': 1,
    'TRX-USDT': 1000,
}

# --------------根据要画图的策略和参数进行修改，开始处---------------------------------------------
exchange = 'binance'  # 只用于显示，可以是任意字符串
symbol = 'ETH-USDT'  # 只用于显示，可以是任意字符串
rule_type = '15T'  # 策略K线时间周期

start_date = '2017-10-01'  # 起始日期
end_date = '2020-12-31'  # 终止日期

signal_name = 'signal_simple_bolling'  # 策略函数名称
para = [360, 2.0]  # 策略参数，根据实际策略需要更改，可以多参数

data_path = 'ETH-USDT_5m.h5'  # 数据文件的位置
drop_days = 0

subplot_list_list = [['%B'], ['BBW']]  # 在此添加与删除副图指标，注意这是一个list套list的结构，放在同一张副图里的指标需要放在同一个子list里，如果不需要副图就设置成空list。

initial_cash = 10000  # 初始资金，默认为10000元
c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
slippage = 1 / 1000  # 滑点，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
leverage_rate = 3  # 杠杆倍数
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓
face_value = coin_value_table[symbol]  # 一张合约的面值

# ----------以下参数可以修改，但在了解其效果之前不建议修改----------
width = 1525  # 图表宽度
height = 715  # 图表高度

plot_height = 120  # 主图高度
subplot_height = 80  # 副图高度
interval = 40  # 副图之间垂直间隔高度
bottom = 80  # 底部保留的高度

price_precision = 2  # 价格精度
volume_precision = 5  # 成交量精度
index_precision = 10  # 指标精度
curve_precision = 5  # 最后一张图中的价格曲线精度


# ----------以上参数可以修改，但在了解其效果之前不建议修改----------


# 策略函数，根据自己魔改的实际策略函数进行粘贴即可
def signal_simple_bolling(df, para):
    """
    传统布林策略
    """
    n = int(para[0])
    m = para[1]
    # ===计算指标
    # 计算均线
    df['median'] = df['close'].rolling(window=n, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std'] = df['close'].rolling(window=n, min_periods=1).std(ddof=0)
    df['upper'] = df['median'] + df['std'] * m
    df['lower'] = df['median'] - df['std'] * m

    df['%B'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])
    df['BBW'] = (df['upper'] - df['lower']) / df['median']

    df.drop(['std'], axis=1, inplace=True)

    # ===找出做多信号
    condition1 = df['close'] > df['upper']  # 当天的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 昨天的收盘价 <= 上轨
    df.loc[condition1 & condition2, 'signal_long'] = 1  # 将买入信号当天的signal设置为1

    # ===找出做空信号
    condition1 = df['close'] < df['lower']  # 当天的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 昨天的收盘价 >= 下轨
    df.loc[condition1 & condition2, 'signal_short'] = -1  # 将卖出信号当天的signal设置为-1

    # ===找出做多平仓
    condition1 = df['close'] < df['median']  # 当天的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 昨天的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将卖出信号当天的signal设置为0

    # ===找出做空平仓
    condition1 = df['close'] > df['median']  # 当天的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 昨天的收盘价 <= 中轨
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将卖出信号当天的signal设置为0

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)
    temp = df[df['signal'].notna()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    df.drop(['signal_long', 'signal_short'], axis=1, inplace=True)

    return df


# -------------根据要画图的策略和参数进行修改，结束处---------------------------------------------


# ========不需修改部分，开始处====================================================================
# 由交易信号产生实际持仓
def position_for_okex_future(df):
    """
    根据signal产生实际持仓。不考虑各种不能买入卖出的情况。
    所有的交易都是发生在产生信号的K线结束时。
    :param df:
    :type df: pd.DataFrame
    :return:
    :rtype: pd.DataFrame
    """

    # ===由signal计算出实际的每天持有仓位
    # 在产生signal的k线结束的时候，进行买入
    df['signal_copy'] = df['signal'].copy()
    df['signal_copy'].fillna(method='ffill', inplace=True)
    df['signal_copy'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
    df['pos'] = df['signal_copy'].shift(1, fill_value=0)  # 将初始行数的pos补全为0

    df.drop(['signal_copy'], axis=1, inplace=True)

    return df


def transfer_to_period_data(df, rule_type):
    # =====转换为其他分钟数据
    period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
        {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }
    )
    period_df.dropna(subset=['open', 'high', 'low', 'close', 'volume'], inplace=True)  # 去除一天都没有交易的周期
    period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
    period_df.reset_index(inplace=True)
    df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
    return df


# 将资金曲线数据，转化为交易数据
def transfer_equity_curve_to_trade(equity_curve):
    """
    将资金曲线数据，转化为一笔一笔的交易
    :param equity_curve: 资金曲线函数计算好的结果，必须包含pos
    :type equity_curve: pd.DataFrame
    :return:
    :rtype: pd.DataFrame
    """
    interval = equity_curve['candle_begin_time'].iloc[1] - equity_curve['candle_begin_time'].iloc[0]
    # =选取开仓、平仓条件
    condition1 = equity_curve['pos'] != 0
    condition2 = equity_curve['pos'] != equity_curve['pos'].shift(1)
    open_pos_condition = condition1 & condition2

    # =计算每笔交易的start_time
    if 'start_time' not in equity_curve.columns:
        equity_curve.loc[open_pos_condition, 'start_time'] = equity_curve['candle_begin_time']
        equity_curve['start_time'].fillna(method='ffill', inplace=True)
        equity_curve.loc[equity_curve['pos'] == 0, 'start_time'] = pd.NaT

    # =对每次交易进行分组，遍历每笔交易
    trade = pd.DataFrame()  # 计算结果放在trade变量中

    for index, group in equity_curve.groupby('start_time'):

        # 记录每笔交易
        # 本次交易结束那根K线的开始时间
        trade.loc[index, 'end_time'] = group.iloc[-1]['candle_begin_time']

        # 本次交易方向
        trade.loc[index, 'signal'] = group['pos'].iloc[0]

        # 本次交易杠杆倍数
        if 'leverage_rate' in group:
            trade.loc[index, 'leverage_rate'] = group['leverage_rate'].iloc[0]

        # 开仓信号的价格
        trade.loc[index, 'start_price'] = group.iloc[0]['open']
        # 平仓信号的价格
        trade.loc[index, 'end_price'] = group.iloc[-1]['close']
        # 持仓K线数量
        trade.loc[index, 'bar_number'] = group.shape[0]
        # 本次交易结束时资金曲线
        trade.loc[index, 'end_equity'] = group.iloc[-1]['equity_curve']
        # 本次交易中资金曲线最低值
        trade.loc[index, 'min_equity'] = group['equity_curve'].min()
        # 本次交易中资金曲线最高值
        trade.loc[index, 'max_equity'] = group['equity_curve'].max()

    trade.index.name = 'start_time'
    trade.reset_index(inplace=True)
    trade.insert(loc=2, column='hold_time', value=trade['end_time'] - trade['start_time'] + interval)
    trade.insert(loc=7, column='start_equity', value=trade['end_equity'].shift(1, fill_value=1))
    trade['min_equity'] = trade[['start_equity', 'min_equity']].min(axis=1)
    trade['max_equity'] = trade[['start_equity', 'max_equity']].max(axis=1)

    trade['end_ratio'] = trade['end_equity'] / trade['start_equity'] - 1
    trade['min_ratio'] = trade['min_equity'] / trade['start_equity'] - 1
    trade['max_ratio'] = trade['max_equity'] / trade['start_equity'] - 1

    return trade


# okex交割合约（usdt本位）资金曲线
def equity_curve_for_okex_usdt_future_next_open(df, initial_cash, slippage, c_rate, leverage_rate, face_value, min_margin_ratio):
    """
    okex交割合约（usdt本位）资金曲线
    开仓价格是下根K线的开盘价，可以是其他的形式
    相比之前杠杆交易的资金曲线函数，逻辑简单很多：手续费的处理、爆仓的处理等。
    在策略中增加滑点。滑点的处理和手续费是不同的。
    :param df:
    :type df: pd.DataFrame
    :param initial_cash: 初始资金
    :type initial_cash: int
    :param slippage: 滑点，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
    :type slippage: float
    :param c_rate: 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
    :type c_rate: float
    :param leverage_rate: 杠杆倍数
    :type leverage_rate: float
    :param face_value: 一张合约的面值
    :type face_value: float
    :param min_margin_ratio: 最低保证金率，低于就会爆仓
    :type min_margin_ratio: float
    :return:
    :rtype: pd.DataFrame
    """
    # =====下根k线开盘价
    next_open = df['open'].shift(-1).fillna(value=df['close']).values  # 下根K线的开盘价

    pos = df['pos'].values
    pos_condition = pos != 0  # 当前周期不为空仓
    # =====找出开仓、平仓的k线
    open_condition = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = pos_condition & open_condition

    close_condition = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = pos_condition & close_condition

    # =====开始计算资金曲线
    # ===在开仓时
    # 在open_pos_condition的K线，以开盘价计算买入合约的数量。（当资金量大的时候，可以用5分钟均价）
    contract_num = np.where(open_pos_condition, np.floor((initial_cash * leverage_rate) / (face_value * df['open'].values)), np.nan)  # 对合约张数向下取整
    # 开仓价格：理论开盘价加上相应滑点
    open_pos_price = np.where(open_pos_condition, df['open'].values * (1 + slippage * pos), np.nan)
    # 开仓之后剩余的钱，扣除手续费
    cash = initial_cash - open_pos_price * contract_num * face_value * c_rate  # 即保证金

    # ===开仓之后每根K线结束时
    # 买入之后cash，contract_num，open_pos_price不再发生变动
    contract_num = np.where(pos_condition, pd.Series(contract_num).fillna(method='ffill').values, np.nan)
    open_pos_price = np.where(pos_condition, pd.Series(open_pos_price).fillna(method='ffill').values, np.nan)
    cash = np.where(pos_condition, pd.Series(cash).fillna(method='ffill').values, np.nan)

    # ===在平仓时
    # 平仓价格
    close_pos_price = np.where(close_pos_condition, next_open * (1 - slippage * pos), np.nan)
    # 平仓之后剩余的钱，扣除手续费
    close_pos_fee = np.where(close_pos_condition, close_pos_price * face_value * contract_num * c_rate, np.nan)

    # ===计算利润
    # 开仓至今持仓盈亏
    # 平仓时利润额外处理
    profit = np.where(close_pos_condition, face_value * contract_num * (close_pos_price - open_pos_price) * pos, face_value * contract_num * (df['close'].values - open_pos_price) * pos)
    # 账户净值
    net_value = cash + profit

    # ===计算爆仓
    # 至今持仓盈亏最小值
    price_min = np.where(pos == -1, df['high'].values, np.where(pos == 1, df['low'].values, np.nan))
    profit_min = face_value * contract_num * (price_min - open_pos_price) * pos
    # 账户净值最小值
    net_value_min = cash + profit_min
    # 计算保证金率
    margin_ratio = net_value_min / (face_value * contract_num * price_min)
    # 计算是否爆仓
    liquidation = np.where(margin_ratio <= (min_margin_ratio + c_rate), 1, np.nan)

    # ===平仓时扣除手续费
    net_value -= np.where(close_pos_condition, close_pos_fee, 0)
    # 应对偶然情况：下一根K线开盘价格价格突变，在平仓的时候爆仓。此处处理有省略，不够精确。
    liquidation = np.where(close_pos_condition & (net_value < 0), 1, liquidation)

    # ===对爆仓进行处理
    liquidation = pd.Series(liquidation).fillna(method='ffill').values
    net_value = np.where(liquidation == 1, 0, net_value)

    # =====计算资金曲线
    df['equity_change'] = np.where(open_pos_condition, net_value / initial_cash - 1, pd.Series(net_value).pct_change().values)  # 开仓日的收益率
    df['equity_change'].fillna(value=0, inplace=True)
    df['equity_curve'] = (1 + df['equity_change']).values.cumprod()

    return df


def get_echart_html(info, ohlcv_data, line_data, curve_data, drawdown_data, subplot_data_list, trade_data, trade_time_data, signal_data):
    _df_ohlcv_list = np.array(ohlcv_data).tolist()  # 把_df_ohlcv转成list
    _df_line_list = np.array(line_data).transpose().tolist()  # 把_df_line转成list
    _df_curve_list = np.array(curve_data).transpose().tolist()  # 把_df_curve转成list
    _df_drawdown_list = np.array(drawdown_data).transpose().tolist()  # 把_df_drawdown转成list
    _df_subplot_list_list = [np.array(subplot_data).transpose().tolist() for subplot_data in subplot_data_list]  # 把_df_subplot转成list

    _df_trade_list = np.array(trade_data).transpose().tolist()  # 把_df_trade转成list
    _df_trade_time_list = np.array(trade_time_data).transpose().tolist()  # 把_df_trade_time转成list

    title = '_'.join(info)
    print(title)
    chart_title = r'\n'.join(info)
    line_list = ['K线'] + line_data.columns.to_list()

    offset = 1  # 常数，表示在所有副图之前只有主图一张图表
    plot_number = len(subplot_data_list) + offset + 1  # 图表的总数

    subplot_space = subplot_height + interval  # 副图需要占用的总高度
    extra_height = subplot_space * len(subplot_data_list)  # 由于副图的存在需要多保留的高度
    bottom_height = bottom + extra_height  # 主图底部总共需要保留的高度

    echarts_data = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <!-- 引入 echarts.js -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/echarts/5.0.2/echarts.min.js"></script>
</head>
<body>
    <!-- 为ECharts准备一个具备大小（宽高）的dom -->
    <div id="klineChart" style="width:{width}px;height:{height + extra_height}px;"></div>
    <div id="tradeChart" style="width:{width}px;height:{height}px;"></div>
    <div id="curveChart" style="width:{width}px;height:{height}px;"></div>
    <script type="text/javascript">

        function splitData(rawData) {{
            let categoryData = [];
            let values = [];
            let volumes = [];
            rawData.forEach((data, i) => {{
                categoryData.push(data.splice(0, 1)[0]);
                values.push(data);
                volumes.push([i, data[4], data[0] > data[1] ? 1 : -1]);
            }});
            return {{
                categoryData: categoryData,
                values: values,
                volumes: volumes,
            }};
        }}

        <!-- 将小数转化为百分数 -->
        function toPercent(value) {{
            return (value * 100).toFixed(2) + ' %';
        }}

        const ohlcv_data = splitData({_df_ohlcv_list});
        const line_data = {_df_line_list};
        const subplot_data_list = {_df_subplot_list_list};
        const trade_data = {_df_trade_list};
        const trade_time_data = {_df_trade_time_list};
        const curve_data = {_df_curve_list};
        const drawdown_data = {_df_drawdown_list};

        const upColor = '#47b262';
        const upBorderColor = '#47b262';
        const downColor = '#eb5454';
        const downBorderColor = '#eb5454';

        const volumeIndex = {plot_number - 1};
        const xAxisArray = {list(range(plot_number))};

        // 基于准备好的dom，初始化echarts实例
        const klineChart = echarts.init(document.getElementById('klineChart'));
        const klineOption = {{
            animation: false,
            title: {{
                text: '{chart_title}',
                left: 0,
                textStyle: {{
                    fontSize: 15,
                }},
            }},
            tooltip: {{
                trigger: 'axis',
                axisPointer: {{
                    type: 'cross',
                }},
                position: function (pos, params, el, elRect, size) {{
                    let obj = {{top: 10}};
                    obj[['left', 'right'][+(pos[0] < size.viewSize[0] / 2)]] = 30;
                    return obj;
                }},
                formatter: function (params) {{
                    const name = ['open', 'close', 'lowest', 'highest'];
                    let result = params[0].axisValue + '<br>';
                    params.forEach(param => {{
                        if (param.seriesName === 'K线') {{
                            result += param.marker + param.seriesName + ': <br>';
                            param.data.slice(1, 5).forEach((data, index) => {{
                                result += name[index] + ': ' + data.toFixed({price_precision}) + '<br>';
                            }});
                        }} else if (param.seriesName === 'volume') {{
                            result += param.marker + param.seriesName + ': ' + param.data[1].toFixed({volume_precision}) + '<br>';
                        }} else {{
                            result += param.marker + param.seriesName + ': ' + param.data.toFixed({index_precision}) + '<br>';
                        }}
                    }});
                    return result;
                }},
            }},
            axisPointer: {{
                link: {{
                    xAxisIndex: 'all',
                }},
            }},
            legend: {{
                data: {line_list},
            }},
            visualMap: {{
                show: false,
                seriesIndex: {len(line_data.columns) + sum(len(subplot_data.columns) for subplot_data in subplot_data_list) + 1},
                dimension: 2,
                pieces: [
                    {{
                        value: 1,
                        color: downColor,
                    }},
                    {{
                        value: -1,
                        color: upColor,
                    }},
                ],
            }},
            grid: [
                {{
                    left: '10%',
                    right: '10%',
                    bottom: {plot_height + bottom_height},
                }},
                {{
                    left: '10%',
                    right: '10%',
                    height: {subplot_height},
                    bottom: {bottom_height},
                }},"""

    for i in range(0, len(subplot_data_list)):
        echarts_data += f"""
                {{
                    left: '10%',
                    right: '10%',
                    height: {subplot_height},
                    bottom: {bottom_height - subplot_space * (i + 1)},
                }},"""

    echarts_data += f"""
            ],
            toolbox: {{
                feature: {{
                    dataZoom: {{
                        yAxisIndex: 'none'
                    }},
                    restore: {{}},
                    saveAsImage: {{}},
                }},
                right: 20,
            }},
            xAxis: [
                {{
                    gridIndex: 0,
                    type: 'category',
                    data: ohlcv_data.categoryData,
                    scale: true,
                    boundaryGap: false,
                    axisLine: {{
                        onZero: false,
                    }},
                    splitLine: {{
                        show: false,
                    }},
                }},"""

    for i in range(0, len(subplot_data_list)):
        echarts_data += f"""
                {{
                    gridIndex: {i + offset},
                    type: 'category',
                    data: ohlcv_data.categoryData,
                    scale: true,
                    boundaryGap: false,
                    axisLine: {{
                        onZero: false,
                    }},
                    splitLine: {{
                        show: false,
                    }},
                }},"""

    echarts_data += f"""
                {{
                    gridIndex: volumeIndex,
                    type: 'category',
                    data: ohlcv_data.categoryData,
                    scale: true,
                    boundaryGap: false,
                    axisLine: {{
                        onZero: false,
                    }},
                    splitLine: {{
                        show: false,
                    }},
                }},
            ],
            yAxis: [
                {{
                    gridIndex: 0,
                    scale: true,
                }},"""

    for i in range(0, len(subplot_data_list)):
        echarts_data += f"""
                {{
                    gridIndex: {i + offset},
                    scale: true,
                    splitLine: {{
                        lineStyle: {{
                            width: 0.5,
                        }},
                    }},
                }},"""

    echarts_data += f"""
                {{
                    gridIndex: volumeIndex,
                    scale: true,
                    splitNumber: 2,
                    splitLine: {{
                        lineStyle: {{
                            width: 0.5,
                        }},
                    }},
                }},
            ],
            dataZoom: [
                {{
                    type: 'inside',
                    xAxisIndex: xAxisArray,
                    start: 0,
                    end: 100,
                    throttle: 0,
                }},
                {{
                    type: 'slider',
                    show: true,
                    xAxisIndex: xAxisArray,
                    start: 0,
                    end: 100,
                    throttle: 0,
                }},
            ],
            series: [
                {{
                    name: 'K线',
                    type: 'candlestick',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: ohlcv_data.values,
                    large: true,
                    largeThreshold: 1000,
                    itemStyle: {{
                        color: upColor,
                        color0: downColor,
                        borderColor: upBorderColor,
                        borderColor0: downBorderColor,
                    }},
                    markPoint: {{
                        label: {{
                            show: true,
                        }},
                        data: ["""

    echarts_data += signal_data

    echarts_data += f"""
                        ],
                    }},
                }},"""

    for i in range(0, len(line_data.columns)):
        echarts_data += f"""
                {{
                    name: '{line_data.columns[i]}',
                    type: 'line',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: line_data[{i}],
                    large: true,
                    smooth: true,
                    lineStyle: {{
                        opacity: 0.5,
                    }},
                    showSymbol: false,
                    emphasis: {{
                        lineStyle: {{
                            width: 2,
                        }},
                    }},
                }},"""

    for i in range(0, len(subplot_data_list)):
        for j in range(0, len(subplot_data_list[i].columns)):
            echarts_data += f"""
                {{
                    name: '{subplot_data_list[i].columns[j]}',
                    type: 'line',
                    xAxisIndex: {i + offset},
                    yAxisIndex: {i + offset},
                    data: subplot_data_list[{i}][{j}],
                    large: true,
                    smooth: true,
                    lineStyle: {{
                        opacity: 0.5,
                    }},
                    showSymbol: false,
                    emphasis: {{
                        lineStyle: {{
                            width: 2,
                        }},
                    }},
                }},"""

    echarts_data += f"""
                {{
                    name: 'volume',
                    type: 'bar',
                    xAxisIndex: volumeIndex,
                    yAxisIndex: volumeIndex,
                    data: ohlcv_data.volumes,
                    large: true,
                    largeThreshold: 2000,
                }},
            ],
        }};
        // 使用指定的配置项和数据显示图表
        klineChart.setOption(klineOption);

        const tradeChart = echarts.init(document.getElementById('tradeChart'));
        const tradeOption = {{
            animation: false,
            tooltip: {{
                trigger: 'axis',
                axisPointer: {{
                    type: 'cross',
                }},
                position: function (pos, params, el, elRect, size) {{
                    let obj = {{top: 10}};
                    obj[['left', 'right'][+(pos[0] < size.viewSize[0] / 2)]] = 30;
                    return obj;
                }},
                formatter: function (params) {{
                    const index = params[0].dataIndex;
                    let result = params[0].axisValue + '<br>';
                    result += 'start_time: ' + trade_time_data[0][index] + '<br>';
                    result += 'end_time: ' + trade_time_data[1][index] + '<br>';
                    result += 'hold_time: ' + trade_time_data[2][index] + '<br>';
                    result += 'signal: ' + trade_time_data[3][index] + '<br>';
                    params.forEach(param => {{
                        result += param.marker + param.seriesName + ': ' + toPercent(param.data) + '<br>';
                    }});
                    return result;
                }},
            }},
            legend: {{
                show: true,
            }},
            grid: {{
                left: '10%',
                right: '10%',
                bottom: 80,
            }},
            toolbox: {{
                feature: {{
                    dataZoom: {{
                        yAxisIndex: 'none'
                    }},
                    restore: {{}},
                    saveAsImage: {{}},
                }},
                right: 20,
            }},
            xAxis: {{
                type: 'category',
                data: {list(range(1, len(trade_data) + 1))},
                boundaryGap: true,
                axisLine: {{
                    onZero: false,
                }},
            }},
            yAxis: [
                {{
                    name: 'equity_ratio',
                    type: 'value',
                    axisLabel: {{
                        formatter: toPercent,
                    }},
                    axisPointer: {{
                        label: {{
                            formatter: params => toPercent(params.value),
                        }},
                    }},
                }},
            ],
            dataZoom: [
                {{
                    type: 'inside',
                    start: 0,
                    end: 100,
                    throttle: 0,
                }},
                {{
                    type: 'slider',
                    show: true,
                    start: 0,
                    end: 100,
                    throttle: 0,
                }},
            ],
            series: ["""

    for i in range(0, len(trade_data.columns)):
        echarts_data += f"""
                {{
                    name: '{trade_data.columns[i]}',
                    type: 'bar',
                    data: trade_data[{i}],
                    stack: '{trade_data.columns[i][0]}',
                    large: true,
                    itemStyle: {{
                        opacity: 0.6,
                    }},
                }},"""

    echarts_data += f"""
            ],
        }};
        tradeChart.setOption(tradeOption);

        tradeChart.on('click', function (params) {{
            const index = params.dataIndex;
            klineChart.dispatchAction({{
                type: 'dataZoom',
                startValue: trade_time_data[0][index],
                endValue: trade_time_data[1][index],
            }});
        }});

        const curveChart = echarts.init(document.getElementById('curveChart'));
        const curveOption = {{
            animation: false,
            tooltip: {{
                trigger: 'axis',
                axisPointer: {{
                    type: 'cross',
                }},
                position: function (pos, params, el, elRect, size) {{
                    let obj = {{top: 10}};
                    obj[['left', 'right'][+(pos[0] < size.viewSize[0] / 2)]] = 30;
                    return obj;
                }},
                formatter: function (params) {{
                    let result = params[0].axisValue + '<br>';
                    params.forEach(param => {{
                        result += param.marker + param.seriesName + ': ' + param.data.toFixed({curve_precision}) + '<br>';
                    }});
                    return result;
                }},
            }},
            legend: {{
                show: true,
            }},
            grid: {{
                left: '10%',
                right: '10%',
                bottom: 80,
            }},
            toolbox: {{
                feature: {{
                    dataZoom: {{
                        yAxisIndex: 'none',
                    }},
                    restore: {{}},
                    saveAsImage: {{}},
                }},
                right: 20,
            }},
            xAxis: {{
                type: 'category',
                data: ohlcv_data.categoryData,
                boundaryGap: false,
                axisLine: {{
                    onZero: false,
                }},
            }},
            yAxis: [
                {{
                    name: 'equity',
                    type: 'log',
                }},
                {{
                    name: 'drawdown',
                    nameLocation: 'start',
                    min: 0,
                    max: 1,
                    type: 'value',
                    inverse: true,
                    splitLine: {{
                        show: false,
                    }},
                }},
            ],
            dataZoom: [
                {{
                    type: 'inside',
                    start: 0,
                    end: 100,
                    throttle: 0,
                }},
                {{
                    type: 'slider',
                    show: true,
                    start: 0,
                    end: 100,
                    throttle: 0,
                }},
            ],
            series: ["""

    for i in range(0, len(curve_data.columns)):
        echarts_data += f"""
                {{
                    name: '{curve_data.columns[i]}',
                    type: 'line',
                    yAxisIndex: 0,
                    data: curve_data[{i}],
                    large: true,
                    smooth: true,
                    lineStyle: {{
                        opacity: 0.5,
                    }},
                    showSymbol: false,
                    emphasis: {{
                        lineStyle: {{
                            width: 2,
                        }},
                    }},
                }},"""

    for i in range(0, len(drawdown_data.columns)):
        echarts_data += f"""
                {{
                    name: '{drawdown_data.columns[i]}',
                    type: 'line',
                    yAxisIndex: 1,
                    data: drawdown_data[{i}],
                    large: true,
                    smooth: true,
                    lineStyle: {{
                        opacity: 0.3,
                    }},
                    showSymbol: false,
                    emphasis: {{
                        lineStyle: {{
                            width: 2,
                        }},
                    }},
                }},"""

    echarts_data += f"""
            ],
        }};
        curveChart.setOption(curveOption);
        window.addEventListener('resize', () => {{
            klineChart.resize();
            tradeChart.resize();
            curveChart.resize();
        }});
    </script>
</body>
</html>
"""

    return echarts_data


def echarts(exchange, symbol, rule_type, signal_name, para, df):
    info = [f'{exchange.upper()}_{symbol}_{rule_type}', f'{signal_name}_{para}', f'{to_ymd(start_date)}-{to_ymd(end_date)}']

    trade = transfer_equity_curve_to_trade(df.copy())
    trade['equity_change'] = trade['end_ratio']
    df['price_curve'] = df['close'] / df.iloc[0]['close']
    df['max_equity_curve'] = df['equity_curve'].expanding().max()
    df['drawdown'] = 1 - df['equity_curve'] / df['max_equity_curve']
    df.drop(['pos', 'equity_change'], axis=1, inplace=True)
    df = pd.merge(df, trade[['end_time', 'equity_change']], left_on='candle_begin_time', right_on='end_time', how='left')
    df.drop(['end_time'], axis=1, inplace=True)

    # 把除了空值外为nan的去除画图时才不会出错
    remove_list = ['signal', 'equity_change']
    column_list = df.columns.to_list()
    for column in remove_list:
        column_list.remove(column)
    df.dropna(axis=0, how='any', subset=column_list, inplace=True)

    df['candle_begin_time'] = df['candle_begin_time'].apply(str)

    trade_list = ['end_ratio', 'min_ratio', 'max_ratio']
    _df_trade = trade[trade_list].copy()

    trade_time_list = ['start_time', 'end_time', 'hold_time', 'signal']
    _df_trade_time = trade[trade_time_list].copy()
    for column in trade_time_list[:-1]:
        _df_trade_time[column] = _df_trade_time[column].apply(str)

    ohlcv_list = ['candle_begin_time', 'open', 'close', 'low', 'high', 'volume']
    subplot_flatten_list = sum(subplot_list_list, [])
    curve_list = ['price_curve', 'equity_curve', 'max_equity_curve']
    drawdown_list = ['drawdown']
    _df_ohlcv = df[ohlcv_list]  # 取得开高低收+成交量的DataFrame
    _df_subplot_list = [df[subplot_list] for subplot_list in subplot_list_list]  # 取得副图指标的DataFrame
    _df_curve = df[curve_list]  # 取得资金曲线的DataFrame
    _df_drawdown = df[drawdown_list]  # 取得回撤的DataFrame
    _df_line = df.drop(ohlcv_list + subplot_flatten_list + curve_list + drawdown_list + remove_list, axis=1)  # 取得指标的DataFrame
    # 把有信号的点标记出来
    signal_str = ''
    if 'signal' in df.columns.tolist():
        _df_signal = df[df['signal'].notna()].copy()
        _df_signal['equity_change'] = _df_signal['equity_change'].shift(-1)
        color = {
            1: '#91cc75',
            -1: '#fac858',
            0: '#5470c6',
        }
        # 有信号的时间之list
        # 有信号的最高价之list，标记在最高价才不会挡到K线
        for x_coord, y_coord, signal, equity_change in _df_signal[['candle_begin_time', 'high', 'signal', 'equity_change']].values:
            word = {
                1: rf'买\n{equity_change:.2%}',
                -1: rf'卖\n{equity_change:.2%}',
                0: '平仓',
            }
            signal_str += f"""
                            {{
                                coord: ['{x_coord}', {y_coord}],
                                label: {{
                                    formatter: function (params) {{
                                        return '{word[signal]}';
                                    }},
                                    color: '#ffffff',
                                    textBorderColor: '{color[signal]}',
                                    textBorderWidth: 1,
                                    fontSize: 10,
                                }},
                                itemStyle: {{
                                    color: '{color[signal]}',
                                }},
                            }},"""

    html_str = get_echart_html(info, _df_ohlcv, _df_line, _df_curve, _df_drawdown, _df_subplot_list, _df_trade, _df_trade_time, signal_str)

    with open('echarts.html', 'w', encoding='utf-8') as f:
        f.write(html_str)
    return html_str


def get_df_with_signal(df, rule_type, signal_name, para):
    df = df.sort_values(by='candle_begin_time', ascending=True)
    df.reset_index(drop=True, inplace=True)
    # 先把资料库的data转成实盘rule_type的分钟K线
    df = transfer_to_period_data(df, rule_type)
    # 因為resample过，所以最后一根不要取，会不完整
    df = df[:-1]
    # 原始数据是utc的话，要加这一行，加8个小时
    # df['candle_begin_time'] = df['candle_begin_time'] + pd.Timedelta(hours=8)
    df = globals()[signal_name](df.copy(), para)
    # 去除时间范围之外的candle
    df = df[df['candle_begin_time'] >= pd.Timestamp(start_date)]
    df = df[df['candle_begin_time'] >= (df.iloc[0]['candle_begin_time'] + pd.Timedelta(days=drop_days))]
    df = df[df['candle_begin_time'] <= pd.Timestamp(end_date)]
    df = position_for_okex_future(df)

    df = equity_curve_for_okex_usdt_future_next_open(df, initial_cash, slippage, c_rate, leverage_rate, face_value, min_margin_ratio)

    return df


# ========不需修改部分，结束处====================================================================


if __name__ == '__main__':
    df_data = pd.DataFrame(pd.read_hdf(data_path, key='df'))  # 读取数据文件，根据实际文件类型，可改为读csv、pkl类型的数据，可修改
    df_draw = get_df_with_signal(df_data.copy(), rule_type, signal_name, para)  # 生成用于画图、带signal和上中下等多轨的df
    # ----可将修改df_draw字段和字段顺序的代码放在这里（也可以直接放在策略函数中），开始处-----------------------
    # ----可将修改df_draw字段和字段顺序的代码放在这里（也可以直接放在策略函数中），结束处-----------------------
    echarts(exchange, symbol, rule_type, signal_name, para, df_draw.copy())  # 画图主函数，生成html文件
