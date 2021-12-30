# -*- coding: utf-8 -*-
import os

from pyecharts import options as opts
from pyecharts.charts import Kline, Line, Grid
from teamon.multi_trade import trade_son

if not os.path.exists('kline_chart'):
    os.mkdir('kline_chart')


def get_son_name(mom):
    ret = []
    for son in mom.son:
        ret.append(son.name)
    return ret


def draw_charts(df):
    '''
    Parameters
    ----------
    df : DataFrame or trade_son
    '''
    if isinstance(df, trade_son):
        title = df.name + ' ' + df.symbol + ' ' + df.time_interval + ' ' + 'K线图'
        name = df.name
        strategy = eval(df.strategy_name)(df.df.copy())  # 初始化策略实例
        df = strategy.run(df.para)  # 策略运行，获得返回数据
        del strategy
    else:
        name = ''
        title = 'K线图'
    df['candle_begin_time'] = df['candle_begin_time'].apply(str)
    signal_data = df[~np.isnan(df['signal'])][['candle_begin_time', 'high', 'signal']]

    def get_act(x):  # 通过signal判断仓位方向
        if x > 0:
            return {'formatter': '多'}
        elif x < 0:
            return {'formatter': '空'}
        else:
            return {'formatter': '平'}

    def set_color(x):  # 设置不同操作的颜色
        if x > 0:
            return {'color': 'rgb(214,18,165)'}
        elif x < 0:
            return {'color': 'rgb(0,0,255)'}
        else:
            return {'color': 'rgb(224,136,11)'}

    signal_data['label'] = np.vectorize(get_act)(signal_data['signal'])
    signal_data['itemStyle'] = np.vectorize(set_color)(signal_data['signal'])
    del signal_data['signal']
    signal_data.columns = ['xAxis', 'yAxis', 'label', 'itemStyle']
    signal_data = signal_data.to_dict('records')
    kline = (
        Kline()
            .add_xaxis(xaxis_data=df['candle_begin_time'].values.tolist())
            .add_yaxis(
            series_name="KlineData",
            y_axis=df[['open', 'close', 'low', 'high']].values.tolist(),
            itemstyle_opts=opts.ItemStyleOpts(color="#ec0000", color0="#00da3c"),
            markpoint_opts=opts.MarkPointOpts(
                data=signal_data,
            ),
        )
            .set_global_opts(
            xaxis_opts=opts.AxisOpts(
                type_="category",
                is_scale=True,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                split_number=20,
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
                background_color="rgba(245, 245, 245, 0.8)",
                border_width=1,
                border_color="#ccc",
                textstyle_opts=opts.TextStyleOpts(color="#000"),
            ),
            brush_opts=opts.BrushOpts(
                x_axis_index="all",
                brush_link="all",
                out_of_brush={"colorAlpha": 0.1},
                brush_type="lineX",
            ),
            datazoom_opts=[opts.DataZoomOpts(type_="inside", range_start=98, range_end=100),
                           opts.DataZoomOpts(type_='slider', range_start=98, range_end=100)],
            title_opts=opts.TitleOpts(title=title),
        )
    )
    if 'upper' in df.columns:  # 布林类，画上下轨
        kline_line = (
            Line()
                .add_xaxis(xaxis_data=df['candle_begin_time'].values.tolist())
                .add_yaxis(
                series_name='Upper',
                y_axis=df['upper'].values.tolist(),
                is_smooth=True,
                is_hover_animation=False,
                linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
                label_opts=opts.LabelOpts(is_show=False),
            )
                .add_yaxis(
                series_name='Median',
                y_axis=df['median'].values.tolist(),
                is_smooth=True,
                is_hover_animation=False,
                linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
                label_opts=opts.LabelOpts(is_show=False),
            )
                .add_yaxis(
                series_name='Lower',
                y_axis=df['lower'].values.tolist(),
                is_smooth=True,
                is_hover_animation=False,
                linestyle_opts=opts.LineStyleOpts(width=3, opacity=0.5),
                label_opts=opts.LabelOpts(is_show=False),
            )
                .set_global_opts(xaxis_opts=opts.AxisOpts(type_="category"))
        )
        kline = kline.overlap(kline_line)
    grid_chart = (
        Grid()
            .add(
            kline,
            grid_opts=opts.GridOpts(pos_left="3%", pos_right="1%", height="80%"),
        )
            # .add(
            #     bar_volume,
            #     grid_opts=opts.GridOpts(
            #         pos_left="3%", pos_right="1%", pos_top="82%", height="14%"
            #     ),
            # )
            .render("kline_chart/" + name + "_candlestick.html")
    )
