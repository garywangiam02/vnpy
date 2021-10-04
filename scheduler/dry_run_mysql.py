import sys  # NOQA: E402
import os

from sqlalchemy.sql.expression import true  # NOQA: E402
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from scheduler.utils.common import flask_app  # NOQA: E402


if __name__ == "__main__":
    # load history data start
    # load_history_data("index")
    # load_history_data("stock")
    # load history data end

    # today = datetime.date.today().strftime("%Y%m%d")
    # get_today_auto('overview-data-push', 'data', 'overview-{}-{}.zip'.format(today, get_random_str(6)))
    # zip2sql('data', 'overview-data-push-20210629.zip')
    # zip2sql('data', 'overview-data-push-20210630.zip')
    # zip2sql('data', 'overview-data-push-20210701.zip')
    # zip2sql('data', 'overview-data-push-20210702.zip')
    from app.service.stockSelection.select import run as run2
    from app.service.stockSelection.clean_up import run as run1
    from app.service.stockSelection.Functions import load_index_to_csv
    # run2()
    with flask_app.app_context():
        # load_index_to_csv(True)
        # run1('M')
        # run1('W')
        run2()
    # '交易日期','股票名称','涨跌幅','振幅1_max','振幅1_min','振幅2_max' ,  '振幅2_min','换手率'                                           下周期每天涨跌幅
