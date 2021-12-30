class Config():
    '''
    配置
    '''

    # 是否是调试模式，在调试模式下会跳过sleep，实盘请设置为False
    debug_mode = True

    # 程序每间隔多久运行，建议 1h （K线固定1h）
    time_interval = '15m'

    # 币安API相关配置
    binance_api_key = ''
    binance_api_secret = ''
    binance_api_host_name = 'fapi.binance.com'

    # 保存上一次费率的文件路径（JSON格式）
    prev_premium_file = 'lovekun_premium.json'
    
    # 全币种获取K线数量
    kline_num = 999
    
    # 并行获取K线使用多进程还是多线程，默认建议False（多进程），有些内存小的破服务器要改成True，不然可能会卡住
    use_threed = False

    # symbol上架U本位合约多少天后才会被本策略监控
    u_future_min_days = 60

    # 钉钉配置
    dd_enable = True        # 是否启用钉钉推送
    dd_root_id = ''
    dd_secret  = ''

    # 是否自动下单
    enable_place_order = False

    # 最大杠杆：每个币种开仓前会判断开仓后是否会超过最大杠杆数，超过就不开了
    max_leverage = 2

    # 固定止损比例：请参阅策略逻辑注释中的2.1.1和3.1.1
    # 坤大推荐0.05或0.08
    stop_loss_ratio = 0.05

    # 2h涨跌幅过滤
    price_change_pct_filter_long    = 0.5    # 0.5 = 2h内涨幅>50%不开多
    price_change_pct_filter_short   = 0.25   # 0.5 = 2h内跌幅<25%不开空

    # 布林极限参数--过滤低波动币种（数值越大，开单频率限制币种数量越低. 1.5~2快，3~5适中，6~10慢）
    parameter_width_m = 3 

    # ===========================================================
    # 策略模式：全币种模式 或者 白名单模式
    # 全币种模式：所有符合条件的币种都会下单，下单金额统一
    # 白名单模式：只会对指定币种进行下单，监控仍然是全币种
    # ===========================================================

    strategy_mode = 'all'   # all = 全币种模式，whitelist = 白名单模式

    # === 全币种模式配置 ===
    strategy_mode_all_balace_ratio      = 0.2    # 全币种模式下，每个币种下单金额占账户资金的百分比，0.2 = 20%
    strategy_mode_all_one_order_usdt    = 1000   # 全币种模式下，拆单时每单的最大金额
    strategy_mode_all_slippage          = 0.02   # 全币种模式下，滑点
    strategy_mode_all_black_list        = ['BTCUSDT', 'ETHUSDT']    # 全币种模式下，黑名单币种（不予监控和下单）

    # === 白名单模式配置 ===
    # 白名单模式需要配置的币种信息
    #   - key = symbol
    #   - balance_ratio = 下单金额占账户资金的百分比
    #   - one_order_usdt = 拆单时每单的最大金额
    #   - slippage = 滑点
    strategy_mode_whitelist_symbols = {
        'OMGUSDT': {
            'balance_ratio': 0.2,      
            'one_order_usdt': 1000,
            'slippage': 0.02
        },
        'AXSUSDT': {
            'balance_ratio': 0.2,      
            'one_order_usdt': 1000,
            'slippage': 0.02
        }
    }


