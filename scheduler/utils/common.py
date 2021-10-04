# 创建定时任务中需要使用到的数据库对象和redis对象
# 可以通过调用create_app 方法，创建flask app的时候，此方法会连带初始化数据库db对象和redis对象
from app.app import create_app

flask_app = create_app()
