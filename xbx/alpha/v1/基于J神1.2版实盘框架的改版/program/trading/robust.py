import time

from config import Config


def run_function_till_success(function, try_times=5, sleep_seconds=60):
    '''
    将函数function尝试运行tryTimes次，直到成功返回函数结果和运行次数，否则返回False
    '''
    retry = 0
    while True:
        if retry > try_times:
            return False
        try:
            result = function()
            return [result, retry]
        except (Exception) as reason:
            print(reason)
            retry += 1
            if sleep_seconds != 0:
                time.sleep(sleep_seconds)


class Robust(object):
    def __init__(self, origin_config: dict):
        self.config = Config(origin_config, False)

    def robust(self, actual_do, *args, **keyargs):
        try_times = int(self.config['robust']['try_times'])
        sleep_seconds = int(self.config['robust']['sleep_seconds'])
        result = run_function_till_success(function=lambda: actual_do(*args, **keyargs), try_times=try_times,
                                           sleep_seconds=sleep_seconds)
        if result:
            return result[0]
        else:
            print(f'{try_times}次尝试获取失败，请检查网络以及参数')
