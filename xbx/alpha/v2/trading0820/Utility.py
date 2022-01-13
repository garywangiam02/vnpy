import traceback
import configparser
import time 
import json
import datetime
import calendar
import requests
import os
import pandas as pd
import time
import os
import json
import requests
import time
import hmac
import hashlib
import base64
from urllib import parse
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
import numpy as np

config = configparser.ConfigParser()
config.read('config.ini')
trade_market = config['trade']['trade_market']


def loadJson(filename):
    with open(filename, 'r', encoding='UTF-8') as f:
        data = json.load(f)
        return data


def saveJson(filename, data):
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


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


def send_dingding_msg(content):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。例如如下地址：https://oapi.dingtalk.com/robot/
    send?access_token=81a0e96814b4c8c3132445f529fbffd4bcce66
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """
    config.read('config.ini')
    robot_id = config['dingding']['robot_id']
    secret =  config['dingding']['secret']
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
        print('成功发送钉钉')
    except Exception as e:
        print("发送钉钉失败:", e)

def send_dingding_msg_old(content):
    config.read('config.ini')
    robot_id = config['dingding']['robot_id']
    try:
        msg={
            "msgtype":"text",
            "text":{
                "content":content+'\n'+datetime.datetime.now().strftime("%m-%d %H:%M:%S")
                }
            }
        Headers={
            "Content-Type":"application/json;charset=utf-8"
        }
        url='https://oapi.dingtalk.com/robot/send?access_token='+robot_id
        body=json.dumps(msg)
        res = requests.post(url,data=body,headers=Headers,timeout=10)
        print(res)
    except Exception as err:
        print('钉钉发送失败',err)


def run_function_till_success(function, tryTimes=5, sleepTimes=60):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    while True:
        if retry > tryTimes:
            return False
        try:
            result = function()
            return [result, retry]
        except (Exception) as reason:
            print(reason)
            send_dingding_msg(trade_market + ':' + str(reason))
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内



def robust(actual_do,*args, **keyargs):
    config.read('config.ini')
    tryTimes    = int(config['default']['tryTimes'])
    sleepTimes  = int(config['default']['sleepTimes'])
    result = run_function_till_success(function=lambda: actual_do(*args, **keyargs), tryTimes=tryTimes, sleepTimes=sleepTimes)
    if result:
        return result[0]
    else:
        send_dingding_msg(trade_market + ':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数')
        os._exit(0)        
        # exit()






if __name__ == "__main__":
    pass

