# TgQuantRobot

#### 介绍
基于telegram的实盘运维机器人

#### 软件架构
程序作为服务端运行在容器环境中,tg作为客户端输入操作指令,服务端接收到指令后执行,并返回操作结果


#### 安装教程

1.  cd TgQuantRobot
2.  docker bulid -t tgquantrobot:1.0.0 .
3.  docker run -d -v TELEGRAM_TOKEN=XXXXX --name=tgquantrobot  tgquantrobot:1.0.0

#### 使用说明

1.  /start 获取tg机器人操作说明
2.  /list_stratege 查询策略列表
3.  /clean_position 清仓