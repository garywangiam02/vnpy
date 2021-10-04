import timeit
import os
import cProfile as profile
import pstats
import numpy as np
import pandas as pd

try:
    from pyChanlun import ChanGraph, ChanLibrary
except ImportError:
    from .pyChanlun import ChanGraph, ChanLibrary

chan_lib = ChanLibrary(bi_style=0)


def get_bar_data():
    if os.path.exists("hs300.csv"):
        print("加载测试数据...")
        df = pd.read_csv("hs300.csv", index_col=0)
    else:
        import tushare as ts
        print("开始获取测试数据，测试数据为HS300...")
        df = ts.get_k_data("000300", index=True, start="2014-01-01", ktype="D")
        df.to_csv("hs300.csv")

    print("测试数据长度:{length}, 起始时间{start}, 结束时间{end}".format(
        length=len(df), start=df["date"].iloc[0], end=df["date"].iloc[-1]))
    return df


def test_single_graph(data, print_result=False):
    graph = ChanGraph(chan_lib, data["date"].values, data["high"].values, data["low"].values)

    if print_result:
        fx_list = graph.fenxing_list
        print("找到{n}个分型：".format(n=len(fx_list)))
        for i, fx in enumerate(fx_list):
            print(
                "第{i}个分型，从{start}到{end}，中心索引为{idx}，最高价{high:.2f}，最低价{low:.2f}".
                format(
                    i=i + 1,
                    # 对于顶（底）分型来说，index是最高（低）价所在的那一根Bar的位置
                    start=fx.start,  # 起点是指分型第一根原始Bar的索引
                    end=fx.end,  # 终点是分型最后一根原始Bar的索引
                    idx=fx.index,  # 这个index是从上面传入的data.index中得到的
                    high=fx.high,  # 对于底分型，其最高点为：按合并后的K线，分型左右两根Bar的最高点中较低者
                    low=fx.low,  # 对于顶分型，其最低点为：按合并后的K线，分型左右两根Bar的最低点中较高者
                ))

        bi_list = graph.bi_list
        print("找到{n}个笔：".format(n=len(bi_list)))
        for i, bi in enumerate(bi_list):
            print("第{i}个笔，从{start}到{end}，长度{length}，最高价{high:.2f}，最低价{low:.2f}".format(
                i=i + 1,
                # 只有分型具有index属性，其他缠论元素只有start和end
                start=bi.start,  # 起点是指笔的第一根原始Bar的索引
                end=bi.end,  # 终点是笔的最后一根原始Bar的索引
                length=len(bi),
                high=bi.high,  # 对于上升（下降）笔，最高点为笔终点（起点）的顶分型的最高点
                low=bi.low,  # 对于上升（下降）笔，最低点为笔起点（终点）的底分型的最低点
            ))

        zs_list = graph.bi_zhongshu_list
        print("找到{n}个笔中枢：".format(n=len(zs_list)))
        for i, zs in enumerate(zs_list):
            print("第{i}个笔中枢，从{start}到{end}，长度{length}，最高价{high:.2f}，最低价{low:.2f}".format(
                i=i,
                start=zs.start,
                end=zs.end,
                length=len(zs),
                high=zs.high,  # 中枢的高点即中枢的上轨
                low=zs.low,  # 低点即下轨
            ))
            for j, bi in enumerate(zs.bi_list):
                print("第{j}个笔，{start}到{end}".format(
                    j=j+1,
                    start=bi.start,
                    end=bi.end,
                ))

        duan_list = graph.duan_list
        print("找到{n}个线段：".format(n=len(duan_list)))
        for i, duan in enumerate(duan_list):
            print("第{i}个线段，从{start}到{end}，长度{length}，最高价{high:.2f}，最低价{low:.2f}".format(
                i=i,
                ## 线段是笔的递归，所以这里不重复写注释了
                start=duan.start,
                end=duan.end,
                length=len(duan),
                high=duan.high,
                low=duan.low,
            ))

        zs_list = graph.duan_zhongshu_list
        print("找到{n}个段中枢：".format(n=len(zs_list)))
        for i, zs in enumerate(zs_list):
            print("第{i}个段中枢，从{start}到{end}，长度{length}，最高价{high:.2f}，最低价{low:.2f}".format(
                i=i,
                start=zs.start,
                end=zs.end,
                length=len(zs),
                high=zs.high,  # 中枢的高点即中枢的上轨
                low=zs.low,  # 低点即下轨
            ))


def test_ctypes_library(data):
    high = data["high"].values.astype(np.float32)
    low = data["low"].values.astype(np.float32)

    _, _, chan_k_high, chan_k_low = chan_lib.chanK(high, low)
    bi = chan_lib.chanBi(chan_k_high, chan_k_low)
    duan = chan_lib.chanDuan(bi, chan_k_high, chan_k_low)
    _, _, _, _ = chan_lib.chanZhongShu(duan, chan_k_high, chan_k_low)

    # if bi:
    #     return


def timeit_single_graph(data):
    print("开始测试单一缠论图...")
    N = 100

    t = timeit.timeit(
        "test_single_graph(data)",
        number=N,
        globals={
            "test_single_graph": test_single_graph,
            "data": data,
        })
    print("循环{N}次，总用时{t:.4f}秒，平均每次用时{avg:.4f}秒".format(N=N, t=t, avg=t / N))
    print("单一缠论图测试结束.")


def timeit_ctypes_library(data):
    print("开始测试缠论DLL...")
    N = 1000

    t = timeit.timeit(
        "test_ctypes_library(data)",
        number=N,
        globals={
            "test_ctypes_library": test_ctypes_library,
            "data": data,
        })
    print("循环{N}次，总用时{t:.4f}秒，平均每次用时{avg:.4f}秒".format(N=N, t=t, avg=t / N))
    print("缠论DLL测试结束.")


def profile_chan_graph(data):
    print("开始进行性能分析...")
    globals()["data"] = data
    profile.run("test_single_graph(data)", "/tmp/graph.profile")
    p = pstats.Stats('/tmp/graph.profile')
    p.strip_dirs().sort_stats('cumulative').print_stats()
    print("分析结束.")


def main():
    data = get_bar_data()
    print("************")
    timeit_ctypes_library(data)
    print("************")
    timeit_single_graph(data)
    print("************")
    test_single_graph(data, True)

if __name__ == "__main__":
    main()
    if chan_lib:
        ChanLibrary._free_library()

