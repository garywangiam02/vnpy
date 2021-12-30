# 基于tg的量化实盘运维平台实现
量化实盘运维是量化整个生命周期中关键的一环,高效灵活可靠的工具可以让我们实时把控程序的运行情况,并灵活调整.相关的解决方案较多,比如基于Flask提供Web API,Supervisor可视化进程管理,或原python开发的存活性检测.然而,诸多的解决方案要么需要二次开发可视化前端界面,要么需要依赖第三方工具没法定制化功能,要么没法实时交互调整.
而tg提供了非常友好的SDK,我们完全可以基于此快速开发出一套功能强大,用户友好的移动化实盘的运维工具.

### 程序功能
1. 策略状态查询,启动,停止,重启.
2. 灵活的仓位控制服务,可以所有账户一键清仓,特定账户指定比例清仓.
3. 支持PM2和docker两种策略运行方式
4. 权限校验
5. 使用环境变量设置系统参数,一定程度上保护隐私数据的安全
6. 容器化部署,避免环境依赖


### 实现关键细节
1. 权限校验

```python
   def restricted(func):
       @wraps(func)
       def wrapped(update, context, *args, **kwargs):
           user_id = update.effective_user.id
           if user_id not in enabled_users:
               print(f"Unauthorized access denied for {user_id}.")
               return
           return func(update, context, *args, **kwargs)
   
       return wrapped

```
所有的操作函数都适用装饰器进行了权限校验,与tg机器人交互的用户id必须在合法用户id列表enabled_users里. 

2. 一键清仓
一键清仓是实际运维工程中使用最多的一块功能,这里支持对所有配置的账户U本位合约一键清仓,指定账户账号一键清仓,指定账户按照特定比例减仓. 清仓时,如果输入了账户id,根据配置的账户id与exchange实例映射dict获取要操作的账户类,然后进行清仓处理.

```python
   def clear_pos(exchange,percent = 100):
       """
       一键清仓
       percent 清仓百分比
       """
       exchange_info = robust(exchange.fapiPublic_get_exchangeinfo,)  # 获取账户净值    
       _symbol_list = [x['symbol'] for x in exchange_info['symbols']]
       symbol_list = [symbol for symbol in _symbol_list if symbol.endswith('USDT')] #过滤usdt合约
   
       # ===从exchange_info中获取每个币种最小交易量
       min_qty = {x['symbol']: int(math.log(float(x['filters'][1]['minQty']), 0.1)) for x in exchange_info['symbols']}
   
       # ===从exchange_info中获取每个币种下单精度
       price_precision = {x['symbol']: int(math.log(float(x['filters'][0]['tickSize']), 0.1)) for x in exchange_info   ['symbols']}
   
       symbol_info = update_symbol_info(exchange, symbol_list)
   
       # ==== 过滤实际持仓为0的币种
       symbol_info = symbol_info[symbol_info['当前持仓量'] != 0] 
   
       symbol_info['目标下单份数'] = 0
       symbol_info['目标下单量'] = 0
       
       # =====计算实际下单量
       symbol_info['实际下单量'] =  - symbol_info['当前持仓量']*percent/100
   
       # =====获取币种的最新价格
       symbol_last_price = fetch_binance_ticker_data(exchange)
   
       # =====逐个下单
       place_order(exchange,symbol_info, symbol_last_price, min_qty, price_precision)
   
```

3. PM2和docker多运行形式支持
Config.py文件中定义了CTRL_MODE环境变量,用于指定策略运行的形式为PM2还是docker,执行的时候据此执行哪种指令. 方便有计算机背景的老板后期拓展为Dokcer容器化运行策略.

### 如何使用
1. 创建tg机器人和tg消息群
参考本人之前写的帖子[《使用telegram实盘监控告警》](https://bbs.quantclass.cn/thread/6139)

2. 获取tg账户id

tg搜索机器人“GetIDs Bot”,然后随意发送一段文字,机器人便会返回你的账户id信息

3. 配置关键账户信息   
在Config.py中配置关键的账户信息
```python
   TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", '1872009999:AAGF8YtfYZomovrtPVkFmFAfTPczyM1D-Bc') # tg token
   TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", -1001265758984) # 用于实盘操作告警
   ENABLED_USERS = os.environ.get("ENABLED_USERS", 1874744992) # 合法用户id列表
   BINANCE_API_CONFIGS = os.environ.get("BINANCE_API_CONFIGS", '{"qinghaihu999999@163.com": {"apiKey": "ufEnlqdwRn240HCAuOwFGLNyV1QcbmruwiisszYTU3VncPlPElCS3PRffffffff","secret": "KWq8yf5ljrNEXky8T5qm8xwJHDjz5mul8zEe7sJTVRqy7ETUPAxti8ffffffffff"}}') # 账户列表   
```
4. 基于PM2或者dokcer启动你的实盘策略
 可以参考 Nep.Yan老板的帖子[用PM2轻松守护和管理你的Python程序](https://bbs.quantclass.cn/thread/6307)


5. Docker环境搭建
> curl -fsSL https://get.docker.com -o get-docker.sh
  sudo sh get-docker.sh
  ## 如果你使用的是 centos，需要多运行一行。 
  ## 启动docker daemon 进程 sudo systemctl start docker
  ## 保持docker 开机自启动 sudo systemctl enable docker
  sudo pip3 install docker-compose

6. 容器运行
>
   cd TgQuantRobot
   docker bulid -t tgquantrobot:1.0.0 .
   docker run -d -v TELEGRAM_TOKEN=XXXXX --name=tgquantrobot  tgquantrobot:1.0.0

