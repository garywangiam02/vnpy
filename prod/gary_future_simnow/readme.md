策略：
- StrategyGridTradeFutureV3
- StrategyGridTradeFutureV4
- StrategyGridTradeFutureV5
- Strategy_TripleMa
- Strategy_TripleMa_v2
- Strategy151DualMaGroupV1
- Strategy151DualMaGroupV2
- Strategy152_Chan_First_V1
- Strategy153_Chan_Three_V1

# 资金相关
    max_invest_rate = 0.1  # 最大仓位(0~1)
    max_invest_margin = 0  # 资金上限 0，不限制
    max_invest_pos = 0  # 单向头寸数量上限 0，不限制

# StrategyGridTradeFutureV3
    期货网格交易策略
    # v1：移植股票网格=》期货，按照当前价，往下n%开始布网格
    # 当创新高，没有网格时，重新布
    # v2， 支持自定义套利对（包括负数价格）,支持双向网格
    # v3,
    #   增加缠论线段，避免急速下跌是被套
    #   增加每个格子的执行计数器，如果执行计数结束，就自动清除；当更低一级网格被执行，也自动被清除

    #  单向网格做多配置方式:
    "Future_grid_LONG_pg": {
            "class_name": "StrategyGridTradeFutureV3",
            "vt_symbol": "pg2011.DCE",
            "auto_init": true,
            "auto_start": true,
            "setting": {
                "backtesting": false,
                "idx_symbol": "pg2011.DCE",
                "max_invest_rate": 0.05,
                "grid_height_percent": 0.4,
                "max_invest_pos": 15,
                "grid_lots": 15，
                "active_long_grid":true,
                "active_short_grid":false
            }
        }
    # 交易所标准套利合约配置方式：
    "Future_grid_LONG_I0901": {
        "class_name": "StrategyGridTradeFutureV3",
        "vt_symbol": "SP i2009&i2101.DCE",
        "auto_init": true,
        "auto_start": true,
        "setting": {
            "backtesting": false,
            "idx_symbol": "SP i2009&i2101.DCE",
            "max_invest_rate": 0.05,
            "grid_height_percent": 10,
            "grid_height_pips": 10,   每个格是10个跳，
            "max_invest_pos": 15,     最大投入15手，平均到每一个网格就是1手
            "grid_lots": 15,          一共做15格
            "active_long_grid":true,  这里只做正套
            "fixed_highest_price": 100 这里限制了价差最高只能100，高于100就不做正套
        }
    },

# StrategyGridTradeFutureV4
    v4, 增加开仓当前分笔开始时间，如果相同分笔内再次开仓，必须相隔两个格子及以上才开仓，
    修改is_chanlun_fit, 增加过滤条件，如，单笔线段不做；

    #  单向网格做多配置方式:
    "Future_grid_LONG_pg": {
            "class_name": "StrategyGridTradeFutureV4",
            "vt_symbol": "pg2011.DCE",
            "auto_init": true,
            "auto_start": true,
            "setting": {
                "backtesting": false,
                "idx_symbol": "pg2011.DCE",
                "max_invest_rate": 0.05,
                "grid_height_percent": 0.4,
                "max_invest_pos": 15,
                "grid_lots": 15，
                "active_long_grid":true,
                "active_short_grid":false
            }
        }
    # 交易所标准套利合约配置方式：
    "Future_grid_LONG_I0901": {
        "class_name": "StrategyGridTradeFutureV4",
        "vt_symbol": "SP i2009&i2101.DCE",
        "auto_init": true,
        "auto_start": true,
        "setting": {
            "backtesting": false,
            "idx_symbol": "SP i2009&i2101.DCE",
            "max_invest_rate": 0.05,
            "grid_height_percent": 10,
            "grid_height_pips": 10,   每个格是10个跳，
            "max_invest_pos": 15,     最大投入15手，平均到每一个网格就是1手
            "grid_lots": 15,          一共做15格
            "active_long_grid":true,  这里只做正套
            "fixed_highest_price": 100 这里限制了价差最高只能100，高于100就不做正套
        }
    },
# StrategyGridTradeFutureV5
  v5， 增加主动离场： 只有一格持仓时，原有线段被破坏，出现反弹没力时，主动离场
# Strategy_TripleMa
 """螺纹钢、5分钟级别、三均线策略
    策略：
    10，20，120均线，120均线做多空过滤
    MA120之上
        MA10 上穿 MA20，金叉，做多
        MA10 下穿 MA20，死叉，平多
    MA120之下
        MA10 下穿 MA20，死叉，做空
        MA10 上穿 MA20，金叉，平空

    # 回测要求：
    使用1分钟数据回测
    #实盘要求：
    使用tick行情
# Strategy_TripleMa_v2
"""15分钟级别、三均线策略
    策略：
    10，20，120均线，120均线做多空过滤
    MA120之上
        MA10 上穿 MA20，金叉，做多
        MA10 下穿 MA20，死叉，平多
    MA120之下
        MA10 下穿 MA20，死叉，做空
        MA10 上穿 MA20，金叉，平空

    # 回测要求：
    使用1分钟数据回测
    # 实盘要求：
    使用tick行情

    V2：
    使用增强版策略模板
    使用指数行情，主力合约交易
    使用网格保存持仓
# Strategy151DualMaGroupV1
CTA 双均线 组合轧差策略
    原始版本：
        金叉做多；死叉做空
        轧差
    v1版本：
        金叉时，生成突破线 = 取前n根bar的最高价，乘以1.03，或者加上1个ATR,或者两个缠论分笔高度。
        发生金叉后的m根bar内，如果价格触碰突破线，则开多；
        持仓期间，如果触碰x根bar的前低，离场
        离场后，
# Strategy151DualMaGroupV2
 """CTA 双均线 组合轧差策略
    原始版本：
        金叉做多；死叉做空
        轧差
    v1版本：
        金叉时，生成突破线 = 取前n根bar的最高价，乘以1.03，或者加上1个ATR,或者两个缠论分笔高度。
        发生金叉后的m根bar内，如果价格触碰突破线，则开多；
        如果出现下跌线段，在底分型逆势进场

        持仓期间，在出现顶背驰信后，触碰x根bar的前低，离场
        离场后，允许再次进场

    v2版本：
        增加开仓时的亏损保护，减少回撤
        缠论开仓时，如果开仓位置在长均线之下，止损位置为一个缠论分笔平均高度
        缠论开仓时，如果开场位置在长均线之上，止损位置为长均线
        突破开仓时，价格为inited的价格

        增加保护跟涨止盈
        如果最后一笔的高度，大于2个平均分笔，启动跟涨止盈保护

    """
# Strategy152_Chan_First_V1
"""
    缠论策略系列-1买、1卖策略
    一买信号：盘整背驰1买，趋势背驰1买，三买后盘整背驰，区间套1买
    离场信号：前低止损，或者次级别出现顶背驰信号

    """
# Strategy153_Chan_Three_V1
 """
    缠论策略系列-3买、3卖策略
    三买信号：中枢突破后的三买三卖
    离场信号：有效下破中枢，或者次级别出现顶背驰信号或三卖信号

    """
