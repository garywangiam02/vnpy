from Function import *
from Statistics import *
import glob


pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 500)  # 最多显示数据的行数

# 策略名称
strategy_name = 'adaptboll_max'

# 每个币种、时间周期的策略数量
strategy_num = 3


# 遍历所有策略结果
rtn = pd.DataFrame()
path_list = glob.glob(root_path + '/data/output/para/*.csv')  # python自带的库，或者某文件夹中所有csv文件的路径
for path in path_list:

    if strategy_name not in path:
        continue

    # if 'ETH' not in path:
    #     continue

    # 防止读取其他策略数据csv
    # 将所有读取的文件打印出来，肉眼排查
    print(path)

    # 读取最优参数，选择排名前strategy_num的
    # df = pd.read_csv(path, skiprows=1, nrows=strategy_num) # 邢大原版要skiprows
    df = pd.read_csv(path, nrows=strategy_num)
    # print(df)

    df['strategy_name'] = strategy_name
    filename = path.split('/')[-1][:-4]
    df['symbol'] = filename.split('-')[1]
    df['leverage'] = filename.split('-')[2]
    df['周期'] = filename.split('-')[3]
    df['tag'] = filename.split('-')[4]

    # 将para转换为1h级别，方便对比各time_interval数据的参数大小
    # 如15m的200 和 1h的50，其实基本是一个策略
    df['para_1h'] = df['para'].apply(para_to_1h, args=(df['周期'][0],))


    rtn = rtn.append(df, ignore_index=True)

# 输出策略详细结果
rtn = rtn[['strategy_name', 'symbol', '周期', 'leverage', 'para', 'para_1h', '累计净值', '年化收益', '最大回撤', '年化收益回撤比']]
rtn.sort_values(by=['strategy_name', 'symbol', '周期', '年化收益回撤比'], ascending=[1, 1, 1, 0], inplace=True)
print('\n\n', rtn)
rtn.to_csv(root_path + f'/data/{strategy_name}所有策略最优参数.csv', index=False)

# 输出策略
summary = rtn.groupby(['strategy_name', 'symbol'])[['年化收益回撤比']].mean().reset_index()
print('\n\n', summary)
summary.to_csv(root_path + f'/data/{strategy_name}策略总体评价.csv', index=False)
