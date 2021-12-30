import os
import time
import requests
import hmac
import json
import hashlib
import base64
from datetime import datetime
from urllib import parse
from icecream import ic

trade_market = 'DataBase'
dingding_id = 'xx'
dingding_secret = 'xx'

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


# 发送钉钉消息
def send_dingding_msg(content, robot_id=dingding_id, secret=dingding_secret):
    """
    :param content:
    :param robot_id:  你的access_token，即webhook地址中那段access_token。例如如下地址：https://oapi.dingtalk.com/robot/
    :param secret: 你的secret，即安全设置加签当中的那个密钥
    :return:
    """
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
        ic('成功发送钉钉')
    except Exception as e:
        ic("发送钉钉失败:", e)


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
            ic(reason)
            # send_dingding_msg(trade_market + ':' + str(reason))
            # send_wechat_message(trade_market + ':' + str(reason), 'crypto')
            retry += 1
            if sleepTimes != 0:
                time.sleep(sleepTimes)  # 一分钟请求20次以内


def robust(actual_do, *args, **keyargs):
    tryTimes = 10
    sleepTimes = 20
    result = run_function_till_success(function=lambda: actual_do(*args, **keyargs), tryTimes=tryTimes,
                                       sleepTimes=sleepTimes)
    if result:
        return result[0]
    else:
        # send_dingding_msg(trade_market + ':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数')
        # send_wechat_message(trade_market + ':' + str(tryTimes) + '次尝试获取失败，请检查网络以及参数', 'crypto')
        os._exit(0)
        # exit()

