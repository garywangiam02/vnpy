
import os,sys
# 将repostory的目录，作为根目录，添加到系统环境中。
VNPY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..' ))
if VNPY_ROOT not in sys.path:
    sys.path.append(VNPY_ROOT)
    print(f'append {VNPY_ROOT} into sys.path')
import asyncio

from vnpy.api.eastmoney_api.eastmoney import EastMoneyBackend, URL_ROOT

if __name__ == '__main__':

    loop = asyncio.get_event_loop()

    debug = False
    username = "xxx"
    password = "xxx"
    if debug:
        backend = EastMoneyBackend(browser_url=None, debug=True)
    else:
        backend = EastMoneyBackend()
        if username is None or password is None:
            print("err_msg", "无效的登录信息")

    if username is not None:
        task = backend.login(username, password, max_retries=10)
        result = loop.run_until_complete(task)
    else:
        result = True

    print('登录完成')
    print('validatekey:{}'.format(backend.validatekey))
    print('cookies:{}'.format(backend.cookies))
