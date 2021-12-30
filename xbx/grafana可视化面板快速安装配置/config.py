import pymysql
import ccxt
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pytz

# debug为1时，runtime sleep 为false
debug = 0
# 常用变量设置
time_interval = '15m'
bnb_filter = 11  # 过滤掉出入金额小于此值的记录
# 币安api设置
all_api = {

    # 策略名称不得少于4个字节
    '账户1': {
        'apiKey': 'xxx',
        'secret': 'xxx',
        "strategy": 'cta_v3'
    },

}

# 下面的设置都不用动
# 时区设置, 该时区的0点为今日盈亏/盈亏比的刷新时间
tz_local = pytz.timezone('Asia/Shanghai')  # 此处填入服务器所在时区
# tz_local = pytz.timezone('America/Toronto')
tz_server = pytz.timezone('Asia/Shanghai')  # 此处填入你所在时区
# tz_server = pytz.timezone('America/Toronto')
# mysql config
db_name = 'quant'
user = 'root'
password = '3ctMcfmlKKPwwNaI'
host = 'db'
port = 3306

con = pymysql.Connect(host=host, user=user, passwd=password, port=port, charset='utf8')

cursor = con.cursor()

create_db = f"CREATE DATABASE IF NOT EXISTS {db_name}"

cursor.execute(create_db)

conn = pymysql.Connect(
    host=host,
    user=user,
    passwd=password,
    db=db_name,
    port=port,
    charset='utf8',
    cursorclass=pymysql.cursors.DictCursor
)
engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (user, password, host, port, db_name))
