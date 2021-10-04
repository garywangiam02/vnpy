
import sys  # NOQA: E402
import os  # NOQA: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from scheduler.utils.config import get_scheduler  # NOQA: E402
from scheduler.jobs.ping import ping  # NOQA: E402
from scheduler.jobs.fetch_data import fetch_data, fetch_binance_data, fetch_sina_data  # NOQA: E402


if __name__ == "__main__":
    scheduler = get_scheduler()
    # PING 每分钟一次
    scheduler.add_job(
        ping,
        "interval",
        id="ping",
        minutes=2,
        replace_existing=True,
    )
    scheduler.add_job(
        fetch_data,
        "cron",
        id="fetch_today_data",
        hour=18,
        minute=18,
        replace_existing=True,
    )

    scheduler.add_job(
        fetch_binance_data,
        "cron",
        id="fetch_today_binance_data",
        hour=9,
        minute=10,
        replace_existing=True,
    )

    scheduler.add_job(
        fetch_sina_data,
        "cron",
        id="fetch_sina_data",
        hour=15,
        minute=15,
        replace_existing=True,
    )

    try:
        scheduler.start()
    except SystemExit:
        print("exit")
        exit()
