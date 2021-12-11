"""
《邢不行-2021新版|Python股票量化投资课程》
author: 邢不行
微信: xbx2626
事件小组基础代码
"""
import os
from xbx.money_flow.program.Config import *

for hold in [3, 5, 8, 13, 21]:
    for stk_num in [3, 10, 30, None]:
        max_cap_num = hold
        back_test_path = root_path + '/data/回测结果/回测详情/回测结果_%s_%s_%s_%s.csv' % (
            event, hold, max_cap_num, stk_num)
        print('=' * 20, event, '=' * 5, hold, '=' * 5, stk_num)
        if os.path.exists(back_test_path):
            print('回测结果已存在')
        else:
            os.system('python /Users/gary/workspace/quant/vnpy/xbx/money_flow/program/2_事件策略回测.py %s %s ' % (hold, stk_num))
