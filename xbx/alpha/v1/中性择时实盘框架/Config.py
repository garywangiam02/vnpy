"""
《邢不行-2020新版|Python数字货币量化投资课程》
无需编程基础，助教答疑服务，专属策略网站，一旦加入，永续更新。
课程详细介绍：https://quantclass.cn/crypto/class
邢不行微信: xbx9025
本程序作者: 邢不行

# 课程内容
币安u本位择时策略实盘框架需要配置的全局变量
"""

import configparser
config = configparser.ConfigParser()
config.read('config.ini')

# sleep时间配置
short_sleep_time = 1  # 用于和交易所交互时比较紧急的时间sleep，例如获取数据、下单
medium_sleep_time = 2  # 用于和交易所交互时不是很紧急的时间sleep，例如获取持仓
long_sleep_time = 10  # 用于较长的时间sleep

# timeout时间
exchange_timeout = 3000  # 3s

# 订单对照表
binance_order_type = {
    '开多': 'BUY',
    '开空': 'SELL',
    '平多': 'SELL',
    '平空': 'BUY',
    '平空开多': 'BUY',
    '平多开空': 'SELL',
}





# 钉钉api
dingding_api = {
    'robot_id': config['dingding']['robot_id'],
    'secret': config['dingding']['secret'],
}
# 钉钉报错api，与监控api分开
dingding_error_api = {
    'robot_id': config['dingding_err']['robot_id'],
    'secret': config['dingding_err']['secret'],
}

# 微信api
wx_api = {
    'agentid': config['wx']['agentid'],
    'secret': config['wx']['secret'],
    'corpid': config['wx']['corpid'],
}
# 微信报错api，与监控api分开
wx_error_api = {
    'agentid': config['wx_err']['agentid'],
    'secret': config['wx_err']['secret'],
    'corpid': config['wx_err']['corpid'],
    }

# signal信号保存路径
signal_file = 'signal_history.csv'

# 资金曲线保存路径
equity_history_file = 'equity_history.txt'


