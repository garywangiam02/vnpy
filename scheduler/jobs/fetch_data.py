import datetime
from app.service.stock.collect import get_today_auto
from core.util import get_random_str
from scheduler.utils.decorater import run_task
from app.service.data.stock.fetch import get_sina_data


def fetch_data():
    run_fetch_data()


def fetch_binance_data():
    run_fetch_binance_data()


@run_task("fetch_today_data")
def run_fetch_data():
    print("fetch today data ......")
    today = datetime.date.today().strftime("%Y%m%d")
    get_today_auto(
        'overview-data-push',
        'data',
        'overview-{}-{}.zip'.format(today, get_random_str(6))
    )


@run_task("fetch_binance_data")
def run_fetch_binance_data():
    print("fetch today binance data ......")
    yesterday = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    get_today_auto(
        'binance-candle-data-push',
        'data/coin/history_candle_data/binance/spot',
        yesterday
    )


def fetch_sina_data():
    run_fetch_sina_data()


@run_task("fetch_sina_data")
def run_fetch_sina_data():
    get_sina_data()
