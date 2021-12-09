
import csv
from vnpy.trader.utility import load_json, save_json, append_data


with open("/Users/gary/workspace/quant/vnpy/prod/stock_tora-screener/data/stock_screener_ThreeBuy_M30_2021-10-21.csv", "r", encoding='utf8') as f:
    reader = csv.reader(f)
    result = []
    for row in reader:
        result.append({
            "cn_name": row[2],
            "vt_symbol": row[4],
            "entry_date": "2021-06-12",
            "active": "true"
        })
    bb = {
        "vt_symbols": result
    }
    save_json("aaaaaa.json", bb)
