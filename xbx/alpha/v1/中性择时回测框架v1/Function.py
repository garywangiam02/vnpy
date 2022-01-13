import os
import requests
import math
import pandas as pd

# 获取项目根目录
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径

root_path = os.path.abspath(os.path.join(_))  # 返回根目录文件夹

# print(root_path)
# /Users/mac/Desktop/cta_backtest



from ast import literal_eval

# 将不同时间级别策略的参数转换为1h的等效参数
# 方便查看参数是否集中
# 如15m的参数40 与 1h的参数10 等效
def para_to_1h(para, time_interval):
    # para = [360]
    # time_interval = '15T'

    para_0 = literal_eval(para)[0]

    if time_interval == '15T':
        para_0 = para_0 / 4
    elif time_interval == '30T':
        para_0 = para_0 / 2
    elif time_interval == '1H':
        para_0 = para_0
    elif time_interval == '2H':
        para_0 = para_0 * 2
    elif time_interval == '4H':
        para_0 = para_0 * 4
    else:
        raise ValueError('time_interval出错！')
    return para_0





def get_quantity_precision():
    base_url = 'https://fapi.binance.com'
    path = '/fapi/v1/exchangeInfo'
    url = base_url + path
    # 此处暂时没有加容错
    response_data = requests.get(url, timeout=5).json()
    # print(response_data)

    df = pd.DataFrame(response_data['symbols'])

    df['tickSize'] = df['filters'].apply(lambda x: math.log(1 / float(x[0]['tickSize']), 10))
    df['stepSize'] = df['filters'].apply(lambda x: math.log(1 / float(x[1]['stepSize']), 10))
    df = df[['symbol', 'pricePrecision', 'quantityPrecision', 'tickSize', 'stepSize']]

    # {'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, ...... }
    return df.set_index('symbol')['quantityPrecision'].to_dict()





def get_face_value_from_OKEx():
    base_url = 'https://www.okex.com'
    # path = '/api/futures/v3/instruments' # 交割合约
    path = '/api/swap/v3/instruments' # 永续合约
    url = base_url + path
    # 此处暂时没有加容错
    response_data = requests.get(url, timeout=5).json()

    # 获取合约id与面值
    df = pd.DataFrame(response_data)[['instrument_id', 'contract_val']]
    # 仅保留USDT合约的instrument_id
    df = df[df['instrument_id'].apply(lambda x: x.split('-')[1] == 'USDT')]
    # BTC-USDT -> BTC
    df['instrument_id'] = df['instrument_id'].apply(lambda x: x.split('-')[0])
    df['contract_val'] = df['contract_val'].astype(float)

    df = df.drop_duplicates().set_index('instrument_id')
    return df.to_dict()['contract_val']





# 邢大原版为OKEx框架，计算下单数量用的是合约面值
# symbol_face_value = get_face_value_from_OKEx() # 最新合约面值可通过该函数获取
symbol_face_value = {'OMG': 1.0, 'WAVES': 1.0, 'BAT': 10.0, 'NEO': 1.0, 'LINK': 1.0, 'DASH': 0.1, 'ADA': 100.0, 'ZEC': 0.1, 'XTZ': 1.0, 'ONT': 10.0, 'ATOM': 1.0, 'QTUM': 1.0, 'XLM': 100.0, 'XMR': 0.1, 'IOTA': 10.0, 'ALGO': 10.0, 'IOST': 1000.0, 'THETA': 10.0, 'KNC': 1.0, 'COMP': 0.1, 'DOGE': 1000.0, 'FIL': 0.1, 'SRM': 1.0, 'SNX': 1.0, 'ANT': 1.0, 'ZRX': 10.0, 'MKR': 0.01, 'DOT': 1.0, 'JST': 100.0, 'REN': 10.0, 'KSM': 0.1, 'TRB': 0.1, 'RSR': 100.0, 'BAL': 0.1, 'STORJ': 10.0, 'BTM': 100.0, 'LRC': 10.0, 'BTC': 0.01, 'LTC': 1.0, 'ETH': 0.1, 'TRX': 1000.0, 'BCH': 0.1, 'BSV': 1.0, 'EOS': 10.0, 'XRP': 100.0, 'ETC': 10.0, 'YFI': 0.0001, 'YFII': 0.001, 'SUSHI': 1.0, 'CRV': 1.0, 'UMA': 0.1, 'BAND': 1.0, 'WNXM': 0.1, 'ZIL': 100.0, 'BTT': 10000.0, 'SWRV': 1.0, 'SUN': 0.1, 'UNI': 1.0, 'AVAX': 1.0, 'FLM': 10.0, 'ZEN': 1.0, 'AAVE': 0.1, 'CVC': 100.0, 'GRT': 10.0, 'NEAR': 10.0, 'EGLD': 0.1, 'BNT': 10.0, '1INCH': 1.0, 'SOL': 1.0, 'LON': 1.0, 'BADGER': 0.1, 'MIR': 1.0, 'TORN': 0.01, 'MASK': 1.0, 'CFX': 10.0, 'CHZ': 10.0, 'MANA': 10.0, 'ALPHA': 1.0, 'LUNA': 0.1, 'FTM': 10.0, 'CONV': 10.0, 'DORA': 0.1, 'ENJ': 1.0, 'SAND': 10.0, 'PERP': 1.0, 'ANC': 1.0, 'SC': 100.0, 'CRO': 10.0, 'XEM': 10.0, 'RVN': 10.0, 'LPT': 0.1, 'MATIC': 10.0, 'XCH': 0.01, 'SHIB': 1000000.0, 'ICP': 0.01, 'CSPR': 1.0, 'LAT': 10.0}

# Binance用数量精度确定下单数量
# symbol_quantity_precision = get_quantity_precision() # 最新数量精度信息可以通过该函数获得
symbol_quantity_precision = {'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0, 'ETCUSDT': 2, 'LINKUSDT': 2, 'XLMUSDT': 0, 'ADAUSDT': 0, 'XMRUSDT': 3, 'DASHUSDT': 3, 'ZECUSDT': 3, 'XTZUSDT': 1, 'BNBUSDT': 2, 'ATOMUSDT': 2, 'ONTUSDT': 1, 'IOTAUSDT': 1, 'BATUSDT': 1, 'VETUSDT': 0, 'NEOUSDT': 2, 'QTUMUSDT': 1, 'IOSTUSDT': 0, 'THETAUSDT': 1, 'ALGOUSDT': 1, 'ZILUSDT': 0, 'KNCUSDT': 0, 'ZRXUSDT': 1, 'COMPUSDT': 3, 'OMGUSDT': 1, 'DOGEUSDT': 0, 'SXPUSDT': 1, 'KAVAUSDT': 1, 'BANDUSDT': 1, 'RLCUSDT': 1, 'WAVESUSDT': 1, 'MKRUSDT': 3, 'SNXUSDT': 1, 'DOTUSDT': 1, 'DEFIUSDT': 3, 'YFIUSDT': 3, 'BALUSDT': 1, 'CRVUSDT': 1, 'TRBUSDT': 1, 'YFIIUSDT': 3, 'RUNEUSDT': 0, 'SUSHIUSDT': 0, 'SRMUSDT': 0, 'BZRXUSDT': 0, 'EGLDUSDT': 1, 'SOLUSDT': 0, 'ICXUSDT': 0, 'STORJUSDT': 0, 'BLZUSDT': 0, 'UNIUSDT': 0, 'AVAXUSDT': 0, 'FTMUSDT': 0, 'HNTUSDT': 0, 'ENJUSDT': 0, 'FLMUSDT': 0, 'TOMOUSDT': 0, 'RENUSDT': 0, 'KSMUSDT': 1, 'NEARUSDT': 0, 'AAVEUSDT': 1, 'FILUSDT': 1, 'RSRUSDT': 0, 'LRCUSDT': 0, 'MATICUSDT': 0, 'OCEANUSDT': 0, 'CVCUSDT': 0, 'BELUSDT': 0, 'CTKUSDT': 0, 'AXSUSDT': 0, 'ALPHAUSDT': 0, 'ZENUSDT': 1, 'SKLUSDT': 0, 'GRTUSDT': 0, '1INCHUSDT': 0, 'BTCBUSD': 3, 'AKROUSDT': 0, 'CHZUSDT': 0, 'SANDUSDT': 0, 'ANKRUSDT': 0, 'LUNAUSDT': 0, 'BTSUSDT': 0, 'LITUSDT': 1, 'UNFIUSDT': 1, 'DODOUSDT': 1, 'REEFUSDT': 0, 'RVNUSDT': 0, 'SFPUSDT': 0, 'XEMUSDT': 0, 'BTCSTUSDT': 1, 'COTIUSDT': 0, 'CHRUSDT': 0, 'MANAUSDT': 0, 'ALICEUSDT': 1, 'BTCUSDT_210625': 3, 'ETHUSDT_210625': 3, 'HBARUSDT': 0, 'ONEUSDT': 0, 'LINAUSDT': 0, 'STMXUSDT': 0, 'DENTUSDT': 0, 'CELRUSDT': 0, 'HOTUSDT': 0, 'MTLUSDT': 0, 'OGNUSDT': 0, 'BTTUSDT': 0, 'NKNUSDT': 0, 'SCUSDT': 0, 'DGBUSDT': 0, '1000SHIBUSDT': 0, 'ICPUSDT': 2, 'BAKEUSDT': 0}




def get_symbol_quantity_precision(symbol):

    # quantity_precision默认为3
    quantity_precision = 3

    if symbol.split('-')[0] + symbol.split('-')[1] in list(symbol_quantity_precision.keys()):
        quantity_precision = symbol_quantity_precision[symbol.split('-')[0] + symbol.split('-')[1]]


    return quantity_precision

def get_symbol_face_value(symbol):

    # 注意如果是ETH/BTC这样的币对，face_value是不太准确的，统一设为1
    face_value = 1

    if symbol.split('-')[1] == 'USDT' and symbol.split('-')[0] in list(symbol_face_value.keys()):
        face_value = symbol_face_value[symbol.split('-')[0]]

    return face_value


