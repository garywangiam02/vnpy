import sys  # NOQA: E402
import os  # NOQA: E402
import datetime  # NOQA: E402
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from app.service.stock.collect import load_history_data, get_today_auto, zip2sql  # NOQA: E402
from core.util import get_random_str  # NOQA: E402
# from scheduler.utils.common import flask_app  # NOQA: E402
# from auto_trade.coin.binance.run import binance_trade  # NOQA: E402


if __name__ == "__main__":
    # binance_trade()
    print("****************")
    # # load history data start
    # load_history_data("index")
    # load_history_data("stock")
    # # load history data end

    today = datetime.date.today().strftime("%Y%m%d")
    # get_today_auto('overview-data-push', 'data', 'overview-{}-{}.zip'.format(today, get_random_str(6)))
    yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    get_today_auto(
        'binance-candle-data-push',
        'data/coin/history_candle_data/binance/spot',
        yesterday
    )

    # zip2sql('data', 'overview-data-push-20210629.zip')
    # zip2sql('data', 'overview-data-push-20210630.zip')
    # zip2sql('data', 'overview-data-push-20210701.zip')
    # zip2sql('data', 'overview-data-push-20210702.zip')
