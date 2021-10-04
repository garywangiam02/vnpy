from config.config import sqlalchemy_database_url
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler
import sys
import os
import logging
import pytz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, os.path.join(BASE_DIR, "common"))
sys.path.insert(0, os.path.join(BASE_DIR))
print(os.path.join(BASE_DIR))


jobstores = {
    "default": SQLAlchemyJobStore(
        url=sqlalchemy_database_url,
        tablename="apscheduler_jobs",
        engine_options={"pool_pre_ping": True, "pool_recycle": 25200},
    ),
}

executors = {
    "default": ThreadPoolExecutor(5),
}

job_defaults = {"coalesce": False, "max_instances": 10}


def my_listener(event):
    if event.exception:
        print("****start", "=" * 100)
        print(event.exception)
        print(event.__dict__)
        print("****end", "=" * 100)
        print("任务出错了！！！！！！")
    else:
        print("=" * 100)
        print("任务照常运行...")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="./logs/scheduler.log",
    filemode="a",
)


def get_scheduler():
    scheduler = BlockingScheduler(
        executors=executors,
        jobstores=jobstores,
        job_defaults=job_defaults,
        timezone=pytz.timezone("Asia/Shanghai"),
    )
    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler._logger = logging
    return scheduler
