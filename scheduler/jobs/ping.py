from scheduler.utils.decorater import run_task
from app.models.stock_overview import Overview


def ping():
    run_ping()


@run_task("ping")
def run_ping():
    print("#2021063001ping")
    Overview.ping()
