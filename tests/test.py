from vnpy.app.cta_strategy_pro import (
    CtaProTemplate,
    StopOrder,
    Direction,
    Offset,
    Status,
    TickData,
    BarData,
    TradeData,
    OrderData,
)
import json
from vnpy.component.base import CtaComponent, MyEncoder
# aa = {
#     'cc': "dd",
#     'bb': Direction.LONG,
# }

# json_file = 'test_Policy.json'
# with open(json_file, 'w', encoding='utf8') as f:
#     data = json.dumps(aa, indent=4, ensure_ascii=False, cls=MyEncoder)
#     f.write(data)

import bz2
import pickle

file_name = "/root/workspace/vnpy/prod/future_simnow/data/Strategy_TripleMa_klines.pkb2"
kline_names = []
with bz2.BZ2File(file_name, 'rb') as f:
    klines = pickle.load(f)
    for kline_name in kline_names:
        # 缓存的k线实例
        cache_kline = klines.get(kline_name, None)
        # 当前策略实例的K线实例
        # strategy_kline = self.klines.get(kline_name, None)
