from threading import Lock, Thread
import time
import requests
import json
from vnpy.trader.utility import print_dict

global wechat_lock
wechat_lock = Lock()


class wechat_thread(Thread):
    def __init__(self, content):
        super(wechat_thread, self).__init__(name="wechat_thread")
        self.CORPID = 'ww7006a98ef46c2b44'
        self.CORPSECRET = 'CqvRT9iu3LwRSOqa9vhF8GETLylunZZjeU_5u_0gWNo'
        self.AGENTID = '1000002'
        self.TOUSER = "WangYang"  # 接收者用户名
        self.lock = wechat_lock
        self.message = content

    def _get_access_token(self):
        url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken'
        values = {'corpid': self.CORPID,
                  'corpsecret': self.CORPSECRET,
                  }
        req = requests.post(url, params=values)
        data = json.loads(req.text)
        return data["access_token"]

    def get_access_token(self):
        try:
            with open('./access_token.conf', 'r') as f:
                t, access_token = f.read().split()
        except:
            with open('./access_token.conf', 'w') as f:
                access_token = self._get_access_token()
                cur_time = time.time()
                f.write('\t'.join([str(cur_time), access_token]))
                return access_token
        else:
            cur_time = time.time()
            if 0 < cur_time - float(t) < 7260:
                return access_token
            else:
                with open('./access_token.conf', 'w') as f:
                    access_token = self._get_access_token()
                    f.write('\t'.join([str(cur_time), access_token]))
                    return access_token

    def run(self):
        send_url = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=' + self.get_access_token()
        send_values = {
            "touser": self.TOUSER,
            "msgtype": "text",
            "agentid": self.AGENTID,
            "text": {
                "content": self.message
            },
            "safe": "0"
        }
        send_msges = (bytes(json.dumps(send_values), 'utf-8'))
        respone = requests.post(send_url, send_msges)
        respone = respone.json()  # 当返回的数据是json串的时候直接用.json即可将respone转换成字典
        return


def send_wx_msg(*args, **kwargs):
    """
    发送微信Msg
    :param content:   发送内容
    :return:
    """
    content = kwargs.get('content', None)
    if content is None:
        if len(args) == 0:
            return
        content = args[0]
    if len(content) == 0:
        return

    if not isinstance(content, str):
        if isinstance(content, dict):
            content = '{}'.format(print_dict(content))
        else:
            content = str(content)

    t = wechat_thread(content=content)
    t.daemon = False
    t.run()


if __name__ == '__main__':
    text = u'微信测试标题!!!!\n第二行'
    send_wx_msg(content=text, target="accountid", msg_type='TRADE')
    # send_wx_msg(text)
