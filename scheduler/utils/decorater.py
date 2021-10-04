# from functools import wraps
import datetime

from app.models.task_log import TaskLog
from scheduler.utils.common import flask_app


def run_task(task_id):
    def wrapper(fn):
        def inner(*args, **kwargs):
            print("*" * 10 + task_id + "*" * 90)
            if task_id != "ping":
                with flask_app.app_context():
                    task_log_id = TaskLog.create(
                        task_id=task_id,
                        status=0,
                        exe_time=datetime.datetime.now(),
                        commit=True,
                    ).id
                    task_log = TaskLog.get_detail_by_id(task_log_id)
                    status = 0
                    stdout = ""
                    try:
                        fn(*args, **kwargs)
                        status = 1
                    except Exception as e:
                        import traceback

                        print("****start", "=" * 100)
                        print(str(e))
                        print(repr(e))
                        print(traceback.print_exc())
                        print("traceback.format_exc():\n%s" % traceback.format_exc())
                        print("****end", "=" * 100)
                        status = -1
                        stdout = str(e)
                    finally:
                        task_log.update(
                            task_id=task_log.task_id,
                            status=status,
                            exe_time=task_log.exe_time,
                            stdout=stdout,
                            commit=True,
                        )
            else:
                with flask_app.app_context():
                    try:
                        fn(*args, **kwargs)
                        status = 1
                    except Exception as e:
                        import traceback

                        print("****ping start", "=" * 100)
                        print(str(e))
                        print(repr(e))
                        print(traceback.print_exc())
                        print("traceback.format_exc():\n%s" % traceback.format_exc())
                        print("****ping end", "=" * 100)
                        status = -1
                        stdout = str(e)

        return inner

    return wrapper
