import ccxt
from Function import *
import configparser

# 从config.ini读取配置信息
config = configparser.ConfigParser()
config.read('config.ini')

# 交易标的
symbol = str(config['default']['symbol'])

# 每次下单量（注意每个币种交易所的要求不一样，需要修改config.ini）
quantity = round(float(config['default']['quantity']), None)
quantity=str(quantity)

# 价格精度
pricePrecision = int(config['default']['pricePrecision'])

# 网格间距，每下跌多少%，买quantity个
gap_percent = float(config['default']['gap_percent'])

# 单边最大挂单数
max_orders = int(config['default']['max_orders'])

# 网格程序运行时间间隔，如每30s运行一次
time_interval = int(config['default']['time_interval'])

# 网格的上下限，破网就撤掉所有挂单（注意，只撤单，不会平掉已有仓位，等到价格回到网内，继续执行网格交易）
grid_up = float(config['default']['grid_up'])
grid_down = float(config['default']['grid_down'])


# =交易所配置
BINANCE_CONFIG = {
    'apiKey': config['binance_api']['apiKey'],
    'secret': config['binance_api']['secret'],
    'timeout': 3000,
    'rateLimit': 10,
    'verbose': False,
    'hostname': 'dapi.binance.com',
    'enableRateLimit': False,
    'options': {
            'adjustForTimeDifference': True,
            'recvWindow': 10000,
        },
}

exchange = ccxt.binance(BINANCE_CONFIG)  # 交易所api




if __name__ == '__main__':
    # print(exchange.dapiPublicGetExchangeInfo())
    # print(dir(exchange))
    # 每次运行前先撤掉当下该symbol的所有挂单
    cancel_open_orders(exchange, symbol)
    # exit()
    # print(exchange.dapiPublic_get_exchangeinfo())
    # 初始化buy_orders, sell_orders
    # 这两个了list用于存放已在交易所挂单的订单信息
    buy_orders = []
    sell_orders = []

    while True:

        print('-'*100)
        
        buy_orders, sell_orders = grid(exchange, symbol, gap_percent, quantity, pricePrecision, max_orders, grid_up, grid_down, buy_orders, sell_orders)
        
        # 每30s运行一次
        time.sleep(time_interval)




