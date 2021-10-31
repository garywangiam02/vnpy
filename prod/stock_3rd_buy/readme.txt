回测脚本：
• 单股票： tests/stock_S153_3rd/run_single_test_s153_3rd_buy_group.py
• 多股票： tests/stock_S153_3rd/ run_hs300_test_S153_3rd_buy.py
回测引擎
• 使用股票Portfolio组合回测引擎
回测数据：
• 使用1分钟bar数据 bar_data/交易所/股票代码_1m.csv
• 如果还没有数据，使用prod/jobs/refill_tdx_stock_bars.py下载
• 如果没有复权数据，使用第一节课的复权数据下载
