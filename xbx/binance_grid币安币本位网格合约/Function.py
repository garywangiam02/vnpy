import ccxt
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
import time
import math
import hmac
import hashlib
import base64
from urllib import parse
import os
import sys
import configparser

# 读取config.ini信息
config = configparser.ConfigParser()
config.read('config.ini')

# 获取本程序标识，以便发送报错时知道是由哪个程序发出的
name = config['default']['name']


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



# ===重试机制
def retry_wrapper(func, params={}, act_name='', sleep_seconds=10, retry_times=100):
    """
    需要在出错时不断重试的函数，例如和交易所交互，可以使用本函数调用。
    :param func: 需要重试的函数名
    :param params: func的参数
    :param act_name: 本次动作的名称
    :param sleep_seconds: 报错后的sleep时间
    :param retry_times: 为最大的出错重试次数
    :return:
    """

    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result
        except Exception as e:
            print(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            time.sleep(sleep_seconds)
    else:
        output_info = act_name + '报错重试次数超过上限，程序退出。\n'
        # send_wx_error_msg(output_info) 
        # send_dingding_error_msg(output_info)
        raise ValueError(output_info)




# ===发送钉钉相关函数
# 计算钉钉时间戳
def cal_timestamp_sign(secret):
    # 根据钉钉开发文档，修改推送消息的安全设置https://ding-doc.dingtalk.com/doc#/serverapi2/qf2nxq
    # 也就是根据这个方法，不只是要有robot_id，还要有secret
    # 当前时间戳，单位是毫秒，与请求调用时间误差不能超过1小时
    # python3用int取整
    timestamp = int(round(time.time() * 1000))
    # 密钥，机器人安全设置页面，加签一栏下面显示的SEC开头的字符串
    secret_enc = bytes(secret.encode('utf-8'))
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = bytes(string_to_sign.encode('utf-8'))
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    # 得到最终的签名值
    sign = parse.quote_plus(base64.b64encode(hmac_code))
    return str(timestamp), str(sign)

def loadJson(filename):
    with open(filename, 'r', encoding='UTF-8') as f:
        data = json.load(f)
        return data


def saveJson(filename, data):
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)



def send_dingding_msg(content, dingding_api):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。
                        例如如下地址：https://oapi.dingtalk.com/robot/send?access_token=81a0e96814b4c8c3132445f529fbffd4bcce66
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """

    robot_id = dingding_api['robot_id']
    secret = dingding_api['secret']

    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}
        headers = {"Content-Type": "application/json;charset=utf-8"}
        # https://oapi.dingtalk.com/robot/send?access_token=XXXXXX&timestamp=XXX&sign=XXX
        timestamp, sign_str = cal_timestamp_sign(secret)
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id + \
              '&timestamp=' + timestamp + '&sign=' + sign_str
        body = json.dumps(msg)
        requests.post(url, data=body, headers=headers, timeout=10)
        print('成功发送钉钉', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        print("发送钉钉失败:", e, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))



# 报错机器人用专门的api，不与其他机器人混用
def send_dingding_error_msg(content):
    # print(content)
    # 报错机器人专用api
    send_dingding_msg(name + content, dingding_error_api)






# 发送微信
def send_wx_message(content, wx_api):

    corpid = wx_api['corpid']
    secret = wx_api['secret']
    agentid = wx_api['agentid']

    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    data = {
        "corpid": corpid,
        "corpsecret": secret
    }

    try:  # 容错
        r = requests.get(url=url, params=data)
    except Exception:
        print("send_message()的requests.get()失败，请检查网络连接。")
    # print(r.json())
    # exit()
    token = r.json()['access_token']
    # Token是服务端生成的一串字符串，以作客户端进行请求的一个令牌
    # 当第一次登录后，服务器生成一个Token便将此Token返回给客户端
    # 以后客户端只需带上这个Token前来请求数据即可，无需再次带上用户名和密码
    url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(token)
    data = {
        "toparty": "1",
        "msgtype": "text",
        "agentid": agentid,
        "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")},
        "safe": "0"
    }

    try:  # 容错
        result = requests.post(url=url, data=json.dumps(data))
        print('成功发送微信', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        print("send_message()的requests.post()失败，请检查网络连接。", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return result.text


# 报错机器人用专门的api，不与其他机器人混用
def send_wx_error_msg(content):
    print(content)
    # 报错机器人专用api：wx_error_api
    send_wx_message(name + content, wx_error_api)







# 通过orderId获取订单信息
def get_order_info(exchange, symbol, orderId):

    order_info = retry_wrapper(exchange.dapiPrivate_get_order,
                                act_name=f'查看{symbol}当前挂单{orderId}',
                                params={'symbol': symbol,
                                        'orderId': orderId})

    time.sleep(1)
    print("成功获取订单信息")
    return order_info


# 通过orderId取消订单
def cancel_order(exchange, symbol, orderId):

    # order_info = retry_wrapper( exchange.fapiPrivate_delete_order, 
    #                             act_name=f'{symbol}撤单',
    #                             params={'symbol': symbol,
    #                                     'orderId': orderId})

    try:
        params={'symbol': symbol, 'orderId': orderId}
        order_info = exchange.dapiPrivate_delete_order(params=params)
    except Exception as e:
        print('撤销订单报错：', e)
        order_info = {}
    
    return order_info



# 查询某个symbol当下的所有挂单
def get_open_orders(exchange, symbol=None):

    if symbol:
        open_orders = retry_wrapper(exchange.dapiPrivate_get_openorders,
                                    act_name=f'查看{symbol}当前全部挂单',
                                    params={'symbol': symbol})
    else:
        open_orders = retry_wrapper(exchange.dapiPrivate_get_openorders,
                                    act_name=f'查看当前全部挂单')

    return open_orders


# 撤销某个symbol当下的所有挂单
def cancel_open_orders(exchange, symbol):

    cancel_orders = retry_wrapper(exchange.dapiPrivate_delete_allopenorders,
                                    act_name=f'撤销{symbol}当前全部挂单',
                                    params={'symbol': symbol})
    print("已撤销")
    return cancel_orders



# 获取当前某个symbol的tick数据
def get_bid_ask_price(exchange, symbol):

    ticker = retry_wrapper(exchange.dapiPublic_get_ticker_bookticker,
                           act_name=f'获取{symbol}当前tick数据',
                            params={'symbol': symbol})
    # print(ticker)
    ticker=ticker[0]
    # print(ticker)
    bid_price = 0.0
    ask_price = 0.0
    if ticker:
        bid_price = float(ticker.get('bidPrice', 0))
        ask_price = float(ticker.get('askPrice', 0))

    return bid_price, ask_price




# 下单
def place_order(exchange, symbol, side, quantity, price):
    print("正在下单")
    print(quantity)
    print(side)
    params = {
        'symbol': symbol,
        'side': side,
        'type': 'LIMIT',
        'quantity': quantity,
        'price': price,
        'timeInForce': 'GTC',
    }

    buy_order_info = exchange.dapiPrivate_post_order(params)
    # 容错
    # buy_order_info = retry_wrapper(exchange.dapiPrivate_post_order,
    #                                 act_name=f'下单函数',
    #                                 params=params)
    
    return buy_order_info






# 网格主逻辑
def grid(exchange, symbol, gap_percent, quantity, pricePrecision, max_orders, grid_up, grid_down, buy_orders, sell_orders):

    # 用于存放已在交易所挂单的订单信息
    buy_orders = buy_orders
    sell_orders = sell_orders

    # 获取当前买一卖一价
    bid_price, ask_price = get_bid_ask_price(exchange, symbol)
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 当前ask_price: {ask_price} bid_price: {bid_price}')

    # 判断是否还在网内，如果破网则撤单，不执行网格逻辑，直到价格回到网内
    if bid_price > grid_up or ask_price < grid_down:
        
        # 撤掉目前所有挂单
        cancel_open_orders(exchange, symbol)
        buy_orders = []
        sell_orders = []
        print('破网！已撤销所有挂单，等待价格回归至网内继续交易。')
        print(f'grid_up: {grid_up}      grid_down: {grid_down}')
        print(f'ask_price: {ask_price}  bid_price: {bid_price}')

        return buy_orders, sell_orders

    buy_orders.sort(key=lambda x: float(x['price']), reverse=True) # 按价格高到底排序（高价更容易成交）
    sell_orders.sort(key=lambda x: float(x['price']), reverse=False) # 按价格低到高排序（低价更容易成交）
    # print(buy_orders)
    # print(sell_orders)
    # 若buy_orders已记录有买单，逐个检查是否有最新成交
    for buy_order_info in buy_orders:
        
        # 获取该订单当前最新状态
        order_info = get_order_info(exchange, symbol, buy_order_info.get('orderId'))
        
        # 若订单当下已成交
        if order_info.get('status') == 'FILLED':
            final_price = float(order_info.get('price'))
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {symbol}买单成交了，买单价为{final_price}')

            # 成交后，从buy_orders中删除
            # 注意remove的是buy_order_info而不是order_info
            # order_info是成交后的buy_order_info
            buy_orders.remove(buy_order_info)

            # 买单成交，挂上一格的卖单
            sell_price = final_price / (1 - gap_percent)
            sell_price = round(sell_price, pricePrecision) # 处理价格精度

            # 防止新计算的sell_price比当前ask低
            # （如果网格运行时间间隔较长会出现该情况，因为新挂单价是由上一个成交价决定的，从成交到现在价格可能已经大幅波动了）
            # if sell_price < ask_price:
            #     # 如果新计算的sell_price比当前ask低，以ask挂卖单，防止挂单变吃单
            #     sell_price = round(ask_price, pricePrecision)

            # 挂新卖单
            new_sell_order_info = place_order(exchange, symbol, 'SELL', quantity, sell_price)
            # 挂单成功后
            if new_sell_order_info.get('orderId'):
                # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单成交，根据成交价挂上一格卖单: {new_sell_order_info.get("symbol")} | price: {new_sell_order_info.get("price")} | qty: {new_sell_order_info.get("origQty")} | orderId: {new_sell_order_info.get("orderId")}')
                print_order(new_sell_order_info, '买单成交，根据成交价挂上一格卖单')
                # 新的卖单加入到sell_orders中
                sell_orders.append(new_sell_order_info)
            #买单成交，查一下买单个数，不到max的凑够max
            for i in range(1,max_orders+1):
                if i>len(buy_orders):
                    buy_price = round(final_price * ((1 - gap_percent)**i), pricePrecision)
                    new_buy_order_info = place_order(exchange, symbol, 'BUY', quantity, buy_price)
                    # 挂单成功后
                    if new_buy_order_info.get('orderId'):
                        # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单成交，根据成交价挂下一格买单: {new_buy_order_info.get("symbol")} | price: {new_buy_order_info.get("price")} | qty: {new_buy_order_info.get("origQty")} | orderId: {new_buy_order_info.get("orderId")}')
                        print_order(new_buy_order_info, '买单成交，根据成交价挂下一格买单')
                        # 新的卖单加入到sell_orders中
                        buy_orders.append(new_buy_order_info)
                i+1
            # # 买单成交，挂下一格的买单（挂更便宜的买单）
            # buy_price = final_price * (1 - gap_percent)
            # buy_price = round(buy_price, pricePrecision) # 处理价格精度
            #
            # # 防止新计算的buy_price比当前bid高
            # # （如果网格运行时间间隔较长会出现该情况，因为新挂单价是由上一个成交价决定的，从成交到现在价格可能已经大幅波动了）
            # if buy_price > bid_price:
            #     # 如果新计算的buy_price比当前bid高，以bid挂买单，防止挂单变吃单
            #     buy_price = round(bid_price, pricePrecision)
            #
            # # 挂新买单
            # if len(buy_orders)==0:
            #     new_buy_order_info = place_order(exchange, symbol, 'BUY', quantity, buy_price)
            #     # 挂单成功后
            #     if new_buy_order_info.get('orderId'):
            #         # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单成交，根据成交价挂下一格买单: {new_buy_order_info.get("symbol")} | price: {new_buy_order_info.get("price")} | qty: {new_buy_order_info.get("origQty")} | orderId: {new_buy_order_info.get("orderId")}')
            #         print_order(new_buy_order_info, '买单成交，根据成交价挂下一格买单')
            #         # 新的卖单加入到sell_orders中
            #         buy_orders.append(new_buy_order_info)

        elif order_info.get('status') == 'CANCELED':
            # 订单被取消，从buy_orders中删除
            buy_orders.remove(buy_order_info)
        
        elif order_info.get('status') == 'NEW':
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单尚未成交，挂单价{order_info.get("price")}')

        else:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单当前状态为{order_info.get("status")}')




    # 若sell_orders已记录有卖单，逐个检查是否有最新成交
    for sell_order_info in sell_orders:
        
        # 获取该订单当前最新状态
        order_info = get_order_info(exchange, symbol, sell_order_info.get('orderId'))
        
        # 若订单当下已成交
        if order_info.get('status') == 'FILLED':
            final_price = float(order_info.get('price'))
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {symbol}卖单成交了，卖单价为{final_price}')

            # 成交后，从buy_orders中删除
            # 注意remove的是sell_order_info不是order_info
            # order_info是成交后的sell_order_info
            sell_orders.remove(sell_order_info)

            # 卖单成交，挂下一格的买单
            buy_price = final_price * (1 - gap_percent)
            buy_price = round(buy_price, pricePrecision) # 处理价格精度

            # 防止新计算的buy_price比当前bid高
            #（如果网格运行时间间隔较长会出现该情况，因为新挂单价是由上一个成交价决定的，从成交到现在价格可能已经大幅波动了）
            # if buy_price > bid_price:
            #     # 如果新计算的buy_price比当前bid高，以bid挂买单，防止挂单变吃单
            #     buy_price = round(bid_price, pricePrecision)

            # 挂新买单
            new_buy_order_info = place_order(exchange, symbol, 'BUY', quantity, buy_price)
            # 挂单成功后
            if new_buy_order_info.get('orderId'):
                # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 卖单成交，根据成交价挂下一格买单: {new_buy_order_info.get("symbol")} | price: {new_buy_order_info.get("price")} | qty: {new_buy_order_info.get("origQty")} | orderId: {new_buy_order_info.get("orderId")}')
                print_order(new_buy_order_info, '卖单成交，根据成交价挂下一格买单')
                # 新的买单加入到buy_orders中
                buy_orders.append(new_buy_order_info)

            #卖单成交，查询卖单数量，不够max的凑够max
            # 买单成交，查一下买单个数，不到max的凑够max
            for i in range(1,max_orders + 1):
                if i>len(sell_orders):
                    sell_price = round(final_price /((1 - gap_percent) ** i), pricePrecision)
                    new_sell_order_info = place_order(exchange, symbol, 'SELL', quantity, sell_price)
                    # 挂单成功后
                    if new_sell_order_info.get('orderId'):
                        # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单成交，根据成交价挂下一格买单: {new_buy_order_info.get("symbol")} | price: {new_buy_order_info.get("price")} | qty: {new_buy_order_info.get("origQty")} | orderId: {new_buy_order_info.get("orderId")}')
                        print_order(new_sell_order_info, '卖单成交，根据成交价挂下一格卖单')
                        # 新的卖单加入到sell_orders中
                        sell_orders.append(new_sell_order_info)
                i + 1
            # # 卖单成交，挂上一格的卖单（挂更高价的卖单）
            # sell_price = final_price * (1 + gap_percent)
            # sell_price = round(sell_price, pricePrecision) # 处理价格精度
            #
            # # 防止新计算的sell_price比当前ask低
            # # （如果网格运行时间间隔较长会出现该情况，因为新挂单价是由上一个成交价决定的，从成交到现在价格可能已经大幅波动了）
            # if sell_price < ask_price:
            #     # 如果新计算的sell_price比当前ask低，以ask挂卖单，防止挂单变吃单
            #     sell_price = round(ask_price, pricePrecision)
            #
            # if len(sell_orders)==0:
            # # 挂新买单
            #     new_sell_order_info = place_order(exchange, symbol, 'SELL', quantity, sell_price)
            #     # 挂单成功后
            #     if new_sell_order_info.get('orderId'):
            #         # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 卖单成交，根据成交价挂上一格卖单: {new_sell_order_info.get("symbol")} | price: {new_sell_order_info.get("price")} | qty: {new_sell_order_info.get("origQty")} | orderId: {new_sell_order_info.get("orderId")}')
            #         print_order(new_sell_order_info, '卖单成交，根据成交价挂上一格卖单')
            #         # 新的卖单加入到sell_orders中
            #         sell_orders.append(new_sell_order_info)

        elif order_info.get('status') == 'CANCELED':
            # 订单被取消，从sell_orders中删除
            sell_orders.remove(sell_order_info)
        
        elif order_info.get('status') == 'NEW':
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 卖单尚未成交，挂单价{order_info.get("price")}')

        else:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 卖单当前状态为{order_info.get("status")}')

    ################################################################

    # 若当下没有挂买单 - 挂个买单
    if not buy_orders:
        
        # 计算买单价格
        price = bid_price * (1 - gap_percent) # 按当前买一价的下一格，挂买单
        price = round(price, pricePrecision) # 处理价格精度
        print(price)
        # 下单
        buy_order_info = place_order(exchange, symbol, 'BUY', quantity, price)

        # 下单成功后
        if buy_order_info.get('orderId'):
            # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 当下没有买单，根据买一价挂单: {buy_order_info}')
            # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 当下没有买单，根据买一价挂单: {buy_order_info.get("symbol")} | price: {buy_order_info.get("price")} | qty: {buy_order_info.get("origQty")} | orderId: {buy_order_info.get("orderId")}')
            print_order(buy_order_info, '当下没有买单，根据买一价挂单')
            buy_orders.append(buy_order_info)


    # 若当下没有挂卖单 - 挂个卖单
    if not sell_orders:

        # 计算卖单价格
        price = ask_price / (1 - gap_percent) # 按当前卖一价的上一格，挂卖单
        price = round(price, pricePrecision) # 处理价格精度

        # 下单
        sell_order_info = place_order(exchange, symbol, 'SELL', quantity, price)

        # 下单成功后
        if sell_order_info.get('orderId'):
            # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 当下没有卖单，根据卖一价挂单: {sell_order_info}')
            # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 当下没有卖单，根据卖一价挂单: {sell_order_info.get("symbol")} | price: {sell_order_info.get("price")} | qty: {sell_order_info.get("origQty")} | orderId: {sell_order_info.get("orderId")}')
            print_order(sell_order_info, '当下没有卖单，根据卖一价挂单')
            sell_orders.append(sell_order_info)

    ################################################################

    # 确保某个价位下，仅有一个买单
    if buy_orders:
        buy_orders.sort(key=lambda x: float(x['price']), reverse=False) # 最低价到最高价
        delete_orders = []
        for i in range(len(buy_orders)-1):
            order = buy_orders[i]
            next_order = buy_orders[i+1]
            # 价差过小，即为重复订单
            if abs(float(next_order['price'])/float(order['price']) - 1) < 0.0045:
                # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 有重复买单，撤销订单：{next_order.get("symbol")} | price: {next_order.get("price")} | qty: {next_order.get("origQty")} | orderId: {next_order.get("orderId")}')
                print_order(next_order, '有重复买单，撤销订单')
                # 撤单
                cancel_order(exchange, symbol, next_order.get('orderId'))
                # 将next_order放入delete_orders，循环结束后再remove，否则会影响buy_orders的下标i
                delete_orders.append(next_order)

        # 将buy_orders中，价格重复的订单remove
        for order in delete_orders:
            buy_orders.remove(order)


    # 确保某个价位下，仅有一个卖单
    if sell_orders:
        sell_orders.sort(key=lambda x: float(x['price']), reverse=True) # 最低价到最高价
        delete_orders = []
        for i in range(len(sell_orders)-1):
            order = sell_orders[i]
            next_order = sell_orders[i+1]
            # 价差过小，即为重复订单
            if abs(float(next_order['price'])/float(order['price']) - 1) < 0.0045:
                # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 有重复卖单，撤销订单：{next_order.get("symbol")} | price: {next_order.get("price")} | qty: {next_order.get("origQty")} | orderId: {next_order.get("orderId")}')
                print_order(next_order, '有重复卖单，撤销订单')
                # 撤单
                cancel_order(exchange, symbol, next_order.get('orderId'))
                # 将next_order放入delete_orders，循环结束后再remove，否则会影响buy_orders的下标i
                delete_orders.append(next_order)

        # 将buy_orders中，价格重复的订单remove
        for order in delete_orders:
            sell_orders.remove(order)

    ################################################################


    # 如果当前买单挂单数大于设置量，则撤掉部分买单
    if len(buy_orders) > max_orders:

        # 最低价到最高价
        buy_orders.sort(key=lambda x: float(x['price']), reverse=False)
        # 取出最低价的买单
        delete_order = buy_orders[0]
        # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 买单超过设置量{max_orders}，撤销最低价的买单：{delete_order}')
        print_order(delete_order, f'买单超过设置量{max_orders}，撤销最低价的买单')
        # 撤单
        cancel_order(exchange, symbol, delete_order.get('orderId'))
        # 从buy_orders移除
        buy_orders.remove(delete_order)

    # 如果当前买单挂单数大于设置量，则撤掉部分买单
    if len(sell_orders) > max_orders:

        # 最高价到最低价
        sell_orders.sort(key=lambda x: float(x['price']), reverse=True)
        # 取出最高价的卖单
        delete_order = sell_orders[0]
        # print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 卖单超过设置量{max_orders}，撤销最高价的卖单：{delete_order}')
        print_order(delete_order, f'卖单超过设置量{max_orders}，撤销最高价的卖单')
        # 撤单
        cancel_order(exchange, symbol, delete_order.get('orderId'))
        # 从sell_orders移除
        sell_orders.remove(delete_order)

    # return最新的buy_orders, sell_orders
    sell_orders.sort(key=lambda x: float(x['price']), reverse=True)
    buy_orders.sort(key=lambda x: float(x['price']), reverse=True)
    print_orders(sell_orders)
    print_orders(buy_orders)



    return buy_orders, sell_orders






def print_order(order: dict, msg: str = '-'):
    # 将交易所返回的订单信息
    # {'orderId': 14591310204, 'symbol': 'LINKUSDT', 'status': 'NEW', 'clientOrderId': 'LrTr3QRQ15gBjcQRUoMuwp', 'price': '18.910', 'avgPrice': '0.00000', 'origQty': '1', 'executedQty': '0', 'cumQty': '0', 'cumQuote': '0', 'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': False, 'closePosition': False, 'side': 'SELL', 'positionSide': 'BOTH', 'stopPrice': '0', 'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'updateTime': 1627532849224
    # 输出为 2021-07-29 12:27:29 有重复买单，撤销订单：LINKUSDT | price: 18.890 | qty: 1 | orderId: 14591310091
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {msg} {order.get("symbol")} | {order.get("side")} | price: {order.get("price")} | qty: {order.get("origQty")} | orderId: {order.get("orderId")}')

def print_orders(orders: list):
    # 这个函数是为了输出buy_orders和sell_orders
    for order in orders:
        print_order(order)








