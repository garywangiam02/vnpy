"""
邢不行2020策略分享会
0705数字货币多空选币中性策略
邢不行微信：xbx9025
"""
import pandas as pd
from glob import glob
import os
import matplotlib.pyplot as plt
import numpy as np
import geatpy as ea
from program.backtest.Function import *
from program.backtest.gen_seed import *
import itertools
import multiprocessing as mp
from multiprocessing import Pool as ProcessPool
import time
from datetime import datetime, timedelta
from joblib import Parallel, delayed
import warnings
warnings.filterwarnings("ignore")
from warnings import simplefilter

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 200)  # 最多显示数据的行数
pd.set_option('display.max_columns', 20)  # 最多显示数据的行数
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth',100)




class Factory:
  def __init__(self):
    self.select_coin_num = 1
    self.c_rate = 6 / 10000
    # self.t_rate = 1 / 1000
    # self.period = 'W'
    self.hold_hour = '6H'  # 持币周期
    self.back_hour_list = [3, 4, 6, 8, 9, 12, 24, 36, 48, 72, 96]  # back_hour 参数集
    self.if_reverse_list = [False, True]  # if_reverse 参数集
    self.weight_list = [0.1 * i for i in range(1, 11)]  # 权重参数集合，0.1~1.0
    self.init_control = 0.5
    self.init_cal_time = 43200
    self.file_name = 'control.txt'
    self.njobs = 25
    self.epoch_rounds = 100
    self.rolling_days = 90
    self.push_days = 30
    self.files = list(map(lambda x: 'coin_' + str(x).zfill(3), np.arange(self.njobs) + 1))
    self.strategy_name = '中性船队1'
    # self.sub_factor = ['K', 'D', 'J', 'RSI','涨跌幅', 'bias', '振幅', '振幅2', '涨跌幅std', '涨跌幅skew', '成交额', '成交额std',
    #                    '资金流入比例', '量比', '成交笔数', '量价相关系数', 'gap', 'cci', 'psy', 'cmo', 'tr_trix', 'reg',
    #                    'sreg', 'magic_cci', 'vwap_bias', 'ADTM', 'POS', '前dhSTC', '前dhER_bull', '前dhER_bear', '前dhRCCD',
    #                    '前dhPMO', '前dhVRAMT']
    self.sub_factor = ['J', 'RSI', '涨跌幅skew', '成交额', '成交额std',  'gap', 'cci', 'cmo', 'tr_trix',
                       'sreg', 'magic_cci', '前dhSTC', '前dhRCCD',
                       '前dhPMO']
    self.choice_list = []
    for factor_name in self.sub_factor:
      for if_reverse in self.if_reverse_list:
        for back_hour in self.back_hour_list:
          for weight in self.weight_list:
            self.choice_list.append([factor_name, if_reverse, back_hour, weight])
    self.A = np.array(self.choice_list)

    # self.ranking_dir = ['T', 'F']
    self.dna_lenth = 3
    self.df = pd.read_pickle(root_path + '/data/backtest/swap/output/data_for_select/all_coin_data_hold_hour_%s.pkl' % self.hold_hour)
    self.spot_df = pd.read_pickle(root_path + '/data/backtest/spot/output/data_for_select/all_coin_data_hold_hour_%s.pkl' % self.hold_hour)
    self.spot_df = self.spot_df[~self.spot_df['symbol'].isin(['COCOS-USDT', 'BTCST-USDT', 'DREP-USDT', 'SUN-USDT', 'BTCBUSD', 'ETHBUSD'])]
    # self.spot_df = self.spot_df[~self.spot_df['symbol'].str.contains('UP-')]
    # self.spot_df = self.spot_df[~self.spot_df['symbol'].str.contains('DOWN-')]
    # self.spot_df = self.spot_df[~self.spot_df['symbol'].str.contains('BEAR-')]
    # self.spot_df = self.spot_df[~self.spot_df['symbol'].str.contains('BULL-')]
    self.spot_df = self.spot_df[self.spot_df['symbol'].str.contains('UP-') == False]
    self.spot_df = self.spot_df[self.spot_df['symbol'].str.contains('DOWN-') == False]
    self.spot_df = self.spot_df[self.spot_df['symbol'].str.contains('BEAR-') == False]
    self.spot_df = self.spot_df[self.spot_df['symbol'].str.contains('BULL-') == False]
    self.df = self.df[self.df['candle_begin_time'] >= pd.to_datetime('2020-06-01')]  # 数据设定
    self.spot_df = self.spot_df[self.spot_df['candle_begin_time'] >= pd.to_datetime('2020-06-01')]  # 数据设定
    # all_coin_data = all_coin_data[all_coin_data['candle_begin_time'] <= pd.to_datetime('2021-02-01')] # 数据设定
    self.df = self.df[self.df['candle_begin_time'] <= pd.to_datetime('2021-06-01')]
    self.spot_df = self.spot_df[self.spot_df['candle_begin_time'] <= pd.to_datetime('2021-06-01')]
    self.df = self.df[self.df['volume'] > 0]  # 该周期不交易的币种
    self.spot_df = self.spot_df[self.spot_df['volume'] > 0]
    self.df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
    self.spot_df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
    # all_coin_data = all_coin_data[all_coin_data['candle_begin_time'] >= pd.to_datetime('2021-02-01')] # 数据设定

    self.rolling_df = self.df[self.df['candle_begin_time'] <= pd.to_datetime(self.df['candle_begin_time'].iloc[0] +
                                                                timedelta(days=self.rolling_days))]
    self.rolling_spot_df = self.spot_df[self.spot_df['candle_begin_time'] <= pd.to_datetime(self.spot_df['candle_begin_time'].iloc[0] +
                                                                timedelta(days=self.rolling_days))]
    self.push_df = pd.DataFrame()
    self.push_spot_df = pd.DataFrame()

  def create_factor_conbine(self, factor_list, _df, offset):

    df = _df.copy()

    df = df[df['offset'] == offset]


    # 横截面因子
    df['因子'] = 0

    for factor_name, if_reverse, back_hour, weight in factor_list:
      if_reverse = True if if_reverse == 'True' else False
      back_hour = int(back_hour)
      weight = float(weight)
      if if_reverse:
        if 'diff' in factor_name:
          factor_diff = factor_name.split('_')
          factor_name = factor_diff[0]
          d_num = factor_diff[2]
          df[factor_name + '_因子'] = -df['%s_bh_%d_diff_%s' % (factor_name, back_hour, d_num)]
        else:
          df[factor_name + '_因子'] = -df['%s_bh_%d' % (factor_name, back_hour)]
      else:
        if 'diff' in factor_name:
          factor_diff = factor_name.split('_')
          factor_name = factor_diff[0]
          d_num = factor_diff[2]
          df[factor_name + '_因子'] = df['%s_bh_%d_diff_%s' % (factor_name, back_hour, d_num)]
        else:
          df[factor_name + '_因子'] = df['%s_bh_%d' % (factor_name, back_hour)]
      df[factor_name + '_排名'] = df.groupby('candle_begin_time')[factor_name + '_因子'].rank()
      df['因子'] += df[factor_name + '_排名'] * weight

    # df = df[df['offset'] == offset]

    # df = df[1:100]
    # print(df)
    # select_coin = pd.DataFrame()
    # # df['symbol'] += ' '
    # select_coin['做多币种数量'] = df.groupby('candle_begin_time')['symbol'].size()
    #
    # select_coin['做多币种'] = df.groupby('candle_begin_time')['symbol'].sum()

    # print(select_coin['做多币种数量'].head(200))
    # print(select_coin['做多币种'].head(200))
    # print(select_coin[select_coin['做多币种数量']>1])

    # 根据因子对比进行排名
    # 从小到大排序
    df['排名1'] = df.groupby('candle_begin_time')['因子'].rank(method='first')
    df1 = df.copy()[df.copy()['排名1'] <= self.select_coin_num]
    df1['方向'] = 1
    # 从大到小排序
    df['排名2'] = df.groupby('candle_begin_time')['因子'].rank(ascending=False, method='first')
    df2 = df.copy()[df.copy()['排名2'] <= self.select_coin_num]
    df2['方向'] = -1
    # 合并排序结果
    df = pd.concat([df1, df2], ignore_index=True)
    df.sort_values(by=['candle_begin_time', '方向'], inplace=True)
    df['本周期涨跌幅'] = -(1 * self.c_rate) + 1 * (1 + (df['下个周期_avg_price'] / df['avg_price'] - 1) * df['方向']) * (
              1 - self.c_rate) - 1
    # print(df)
    # print(df['close']-df['open'])
    # print(df['volume'])



    return df

  def cal_curve(self, _df):
    df = _df.copy()

    select_coin = pd.DataFrame()
    df['symbol'] += ' '
    select_coin['做多币种数量'] = df[df['方向'] == 1].groupby('candle_begin_time')['symbol'].size()
    select_coin['做空币种数量'] = df[df['方向'] == -1].groupby('candle_begin_time')['symbol'].size()
    select_coin['做多币种'] = df[df['方向'] == 1].groupby('candle_begin_time')['symbol'].sum()
    select_coin['做空币种'] = df[df['方向'] == -1].groupby('candle_begin_time')['symbol'].sum()
    select_coin['本周期多空涨跌幅'] = df.groupby('candle_begin_time')['本周期涨跌幅'].mean()


    # 计算整体资金曲线
    select_coin.reset_index(inplace=True)
    select_coin['资金曲线'] = (select_coin['本周期多空涨跌幅'] + 1).cumprod()

    # 计算最大回撤
    # select_coin['max2here'] = select_coin['资金曲线'].expanding().max()
    # select_coin['dd2here'] = select_coin['资金曲线'] / select_coin['max2here'] - 1
    # end_date, max_draw_down = tuple(select_coin.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])

    return select_coin

  def cal_one_factor(self, factor_set):
    sum = 0

    for offset in range(int(self.hold_hour.strip('H'))):
      df = self.create_factor_conbine(factor_set, self.rolling_df, offset)
      select_coin = self.cal_curve(df)
      # print(select_coin)
      if len(select_coin) > 0:
        rtn = self.cal_fig(select_coin)
        # print(rtn)
      # print(rtn)
      # print(rtn['累积净值'].values)
        r1 = rtn['累积净值'].values[0]
        r2 = rtn['最大回撤'].values[0]
        r2 = abs(float(r2.replace('%', '').strip()) / 100.)
        # print(r1)
        # print(r2)
        if r2 != 0:
          _ind = 0.1 * r1 / r2  # 优化指标 0.1 * 累积净值 / abs(最大回撤)
        else:
          _ind = 0
      else:
        _ind = 0
      for i in select_coin['做多币种数量']:
        if i != self.select_coin_num:
          # print(i)
          # print(select_coin)
          _ind = 0
      for i in select_coin['做空币种数量']:
        if i != self.select_coin_num:
          # print(i)
          # print(select_coin)
          _ind = 0
      sum += _ind
    average = sum / int(self.hold_hour.strip('H'))
    return average

  def make_kid(self, _parent, _choose):
    try:
      control = open(self.file_name, mode='r')
      control_content = float(control.read())
      control.close()
    except IOError:
      control = open(self.file_name, mode='w')
      control.write(str(self.init_control))
      control_content = float(self.init_control)
      control.close()

    _kid = _parent.copy()
    for replace in range(len(_parent)):
      if np.random.rand() < control_content:
        # new_coice_list = list(set(_choice_list) - set(list(_parent[replace])))
        new_choice_list = np.delete(self.A, _choose[replace], 0)
        _kid[replace] = new_choice_list[np.random.choice(new_choice_list.shape[0], 1)[0]]
    return _kid

  def revalution(self, _file_name):
    opt_value = 0
    opt_record = {}
    opt_df = pd.DataFrame()
    # for i in self.sub_factor:
    #   for j in self.ranking_dir:
    #     choice_list.append(i + j)
    # A = np.array(self.choice_list)
    choose = np.random.choice(self.A.shape[0], self.dna_lenth, replace=True)
    parent = gen_seed() if using_seed else self.A[choose, :]
    current_time = self.rolling_df['candle_begin_time'].iloc[-1]
    for epoch in range(self.epoch_rounds):
      kid = self.make_kid(_parent=parent.copy(), _choose=choose)

      ind_parent = self.cal_one_factor(parent)
      ind_kid = self.cal_one_factor(kid)

      if ind_parent > ind_kid:
        best_dna_in_this_epoch = parent
        best_ind_in_this_epoch = ind_parent
      else:
        best_dna_in_this_epoch = kid
        best_ind_in_this_epoch = ind_kid

      if best_ind_in_this_epoch >= opt_value:
        opt_value = best_ind_in_this_epoch
        # print('========== 新的历史值', opt_value)
        opt_record['历史最优个体'] = best_dna_in_this_epoch
        opt_record['操作值'] = best_ind_in_this_epoch

        to_save = pd.DataFrame()
        to_save['w'] = best_dna_in_this_epoch.flatten()
        to_save = to_save.T
        to_save.reset_index(drop=True, inplace=True)
        to_save['ind'] = best_ind_in_this_epoch

        opt_df = pd.concat([opt_df, to_save], ignore_index=True)
        opt_df.sort_values(by=['ind'], inplace=True)
        opt_df.reset_index(drop=True, inplace=True)
        opt_df.to_csv(root_path + f'/data/factory/{_file_name}.csv', encoding='utf-8-sig')
        print(current_time)
        print('parent:', ind_parent, '    kid:', ind_kid)
        print(f"epochs {epoch}  历史最优个体 {opt_record['操作值']}")
        print(opt_record['历史最优个体'], '\n\n\n')

      parent = kid if ind_kid > ind_parent else parent

  def cal_fig(self, equity_df):
    equity = equity_df.copy()


    # ===新建一个dataframe保存回测指标
    results = pd.DataFrame()

    col = '资金曲线'
    # print(equity)
    # ===计算累积净值
    results.loc[col, '累积净值'] = round(equity[col].iloc[-1], 2)

    # # ===计算年化收益
    # annual_return = (equity[col].iloc[-1]) ** (
    #         '1 days 00:00:00' / (equity['交易日期'].iloc[-1] - equity['交易日期'].iloc[0]) * 365) - 1
    # results.loc[col, '年化收益'] = str(round(annual_return * 100, 2)) + '%'

    # ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
    # 计算当日之前的资金曲线的最高点
    equity['max2here'] = equity[col].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    equity['dd2here'] = equity[col] / equity['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间

    end_date, max_draw_down = tuple(equity.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
    # 计算最大回撤开始时间
    start_date = equity[equity['candle_begin_time'] <= end_date].sort_values(by=col, ascending=False).iloc[0]['candle_begin_time']
    # 将无关的变量删除
    equity.drop(['max2here', 'dd2here'], axis=1, inplace=True)
    results.loc[col, '最大回撤'] = format(max_draw_down, '.2%')
    results.loc[col, '最大回撤开始时间'] = str(start_date)
    results.loc[col, '最大回撤结束时间'] = str(end_date)

    # ===年化收益/回撤比：我个人比较关注的一个指标
    # results.loc[col, '年化收益/回撤比'] = round(annual_return / abs(max_draw_down), 2)

    return results

  def real_trade(self):
    for offset in range(int(self.hold_hour.strip('H'))):
      exec('final_df_%s = pd.DataFrame()' % offset)
      exec('final_spot_df_%s = pd.DataFrame()' % offset)
    end_date = self.rolling_df['candle_begin_time'].iloc[0] - timedelta(hours=1)
    fac_list = []
    n = 1
    print(datetime.now())
    while 1:
      Parallel(n_jobs=fac.njobs)(delayed(fac.revalution)(_file_name=_file) for _file in fac.files)
      print(datetime.now())
      print(end_date)

      # 先等待初始化因子计算
      # sleep(self.init_cal_time)
      # 拿到最优因子计算初始的净值并且生成新的df

      def read_one_file(_path):
        df: pd.DataFrame = pd.read_csv(_path, index_col=0)
        return df

      db_all = pd.concat(
        Parallel(n_jobs=4)(delayed(read_one_file)(path) for path in glob(root_path + '/data/factory/*.csv')),
        ignore_index=True)
      db_all.sort_values(by='ind', inplace=True, ascending=False)
      db_all.reset_index(inplace=True, drop=True)
      db_all = db_all[0 == db_all.index].copy()  # 取指标最大的列
      print('ind', db_all['ind'].values[0])
      print(db_all[list(db_all.columns)[:-1]].values[0].tolist())
      factor_set = db_all[list(db_all.columns)[:-1]].values[0].tolist()
      factor_set = np.array(factor_set).reshape(int(len(factor_set) / 4), 4)
      pre_start_df = self.rolling_df
      if n == 1:
        pre_factor_set = factor_set
        n = 2
        for offset in range(int(self.hold_hour.strip('H'))):
          # exec('df_start_%s = %s' % (offset, self.create_factor_conbine(factor_set, self.rolling_df, offset)))
          exec('df_start_%s = self.create_factor_conbine(factor_set, self.rolling_df, offset)' % offset)
          exec('spot_df_start_%s = self.create_factor_conbine(factor_set, self.rolling_spot_df, offset)' % offset)
          # df_start = self.create_factor_conbine(factor_set, self.rolling_df)
          exec('final_df_%s = final_df_%s.append(df_start_%s)' % (offset, offset, offset))
          exec('final_spot_df_%s = final_spot_df_%s.append(spot_df_start_%s)' % (offset, offset, offset))
          # final_df = final_df.append(df_start)
          exec('print(final_df_%s.tail())' % offset)
        fac_list.append([end_date + timedelta(hours=1), factor_set])
        # exec('print(final_df_%s.tail())' % offset)
        print(fac_list)
      # end_date = self.start_df['交易日期'].iloc[-1]
      end_date = self.rolling_df['candle_begin_time'].iloc[-1]
      start_date = self.rolling_df['candle_begin_time'].iloc[0]
      # fac_list.append([end_date + timedelta(days=1), factor_set])

      if end_date < self.df['candle_begin_time'].iloc[-1]:
        if end_date + timedelta(days=self.push_days * 3 / 2) > self.df['candle_begin_time'].iloc[
          -1]:
          self.push_df = self.df[self.df['candle_begin_time'] > pd.to_datetime(end_date)]
          self.push_spot_df = self.spot_df[self.spot_df['candle_begin_time'] > pd.to_datetime(end_date)]

        else:
          self.push_df = self.df[self.df['candle_begin_time'] <= pd.to_datetime(end_date +
                                                                   timedelta(days=self.push_days))]
          self.push_spot_df = self.spot_df[self.spot_df['candle_begin_time'] <= pd.to_datetime(end_date +
                                                                                timedelta(days=self.push_days))]
          self.push_df = self.push_df[self.push_df['candle_begin_time'] > pd.to_datetime(end_date)]
          self.push_spot_df = self.push_spot_df[self.push_spot_df['candle_begin_time'] > pd.to_datetime(end_date)]
          self.rolling_df = self.df[self.df['candle_begin_time'] <= pd.to_datetime(end_date +
                                                                      timedelta(days=self.push_days))]
          self.rolling_spot_df = self.spot_df[self.spot_df['candle_begin_time'] <= pd.to_datetime(end_date +
                                                                                   timedelta(days=self.push_days))]
          self.rolling_df = self.rolling_df[self.rolling_df['candle_begin_time'] > pd.to_datetime(start_date +
                                                                                     timedelta(days=self.push_days))]
          self.rolling_spot_df = self.rolling_spot_df[self.rolling_spot_df['candle_begin_time'] > pd.to_datetime(start_date +
                                                                                                  timedelta(
                                                                                                    days=self.push_days))]
        for path in glob(root_path + '/data/factory/*.csv'):
          os.remove(path)
      exec('equity_1 = 0')
      exec('equity_2 = 0')
      for offset in range(int(self.hold_hour.strip('H'))):
        exec('df_to_start_%s = self.create_factor_conbine(factor_set, pre_start_df, offset)' % offset)
        # exec('df_to_start_%s = %s' % (offset, self.create_factor_conbine(factor_set, pre_start_df, offset)))
        # df_to_start = self.create_factor_conbine(factor_set, pre_start_df)
        exec('select_coin_p_%s = self.cal_curve(df_to_start_%s)' % (offset, offset))
        # select_coin_p = self.cal_curve(df_to_start)
        exec('equity_1_%s = select_coin_p_%s["资金曲线"].iloc[-1]' % (offset, offset))
        # equity_1 = select_coin_p['资金曲线'].iloc[-1]
        exec('df_to_start_pre_%s = self.create_factor_conbine(pre_factor_set, pre_start_df, offset)' % offset)
        # exec('df_to_start_pre_%s = %s' % (offset, self.create_factor_conbine(pre_factor_set, pre_start_df, offset)))
        # df_to_start_pre = self.create_factor_conbine(pre_factor_set,
        #                                              pre_start_df)
        exec('select_coin_p2_%s = self.cal_curve(df_to_start_pre_%s)' % (offset, offset))
        # select_coin_p2 = self.cal_curve(df_to_start_pre)
        exec('equity_2_%s = select_coin_p2_%s["资金曲线"].iloc[-1]' % (offset, offset))
        # equity_2 = select_coin_p2['资金曲线'].iloc[-1]
        exec('equity_1 += equity_1_%s' % offset)
        exec('equity_2 += equity_2_%s' % offset)
      eval('print(equity_1)')
      eval('print(equity_2)')
      # for offset in range(int(self.hold_hour.strip('H'))):
      if eval('equity_1 >= equity_2'):
        for offset in range(int(self.hold_hour.strip('H'))):
          # exec('df_next_%s = %s' % (offset, self.create_factor_conbine(factor_set, self.push_df, offset)))
          exec('df_next_%s = self.create_factor_conbine(factor_set, self.push_df, offset)' % offset)
          exec('spot_df_next_%s = self.create_factor_conbine(factor_set, self.push_spot_df, offset)' % offset)

          # df_next = self.create_factor_conbine(factor_set, self.push_df)
          exec('final_df_%s = final_df_%s.append(df_next_%s)' % (offset, offset, offset))
          exec('final_spot_df_%s = final_spot_df_%s.append(spot_df_next_%s)' % (offset, offset, offset))
          # final_df = final_df.append(df_next)
          exec('print(final_df_%s.tail())' % offset)
        fac_list.append([end_date + timedelta(hours=1), factor_set])
        pre_factor_set = factor_set
      else:
        for offset in range(int(self.hold_hour.strip('H'))):
          # exec('df_next_%s = %s' % (offset, self.create_factor_conbine(pre_factor_set, self.push_df, offset)))
          exec('df_next_%s = self.create_factor_conbine(pre_factor_set, self.push_df, offset)' % offset)
          exec('spot_df_next_%s = self.create_factor_conbine(pre_factor_set, self.push_spot_df, offset)' % offset)
          # df_next = self.create_factor_conbine(pre_factor_set, self.push_df)
          exec('final_df_%s = final_df_%s.append(df_next_%s)' % (offset, offset, offset))
          exec('final_spot_df_%s = final_spot_df_%s.append(spot_df_next_%s)' % (offset, offset, offset))
          # final_df = final_df.append(df_next)
          exec('print(final_df_%s.tail())' % offset)
        fac_list.append([end_date + timedelta(hours=1), pre_factor_set])
      # print(final_df.tail())
      print(fac_list)

      if self.push_df['candle_begin_time'].iloc[-1] == self.df['candle_begin_time'].iloc[-1]:
        break

    print('推进至结尾')
    rtn = pd.DataFrame()
    spot_rtn = pd.DataFrame()
    # max_draw_down = 0
    # annual_return = 0
    for offset in range(int(self.hold_hour.strip('H'))):
      exec('print(final_df_%s.tail())' % offset)
    # print(final_df.tail())
      exec('select_coin_%s = self.cal_curve(final_df_%s)' % (offset, offset))
      exec('spot_select_coin_%s = self.cal_curve(final_spot_df_%s)' % (offset, offset))
    # select_coin = self.cal_curve(final_df
      exec('print(select_coin_%s)' % offset)
    # print(select_coin)
      exec('print("最终净值为:", select_coin_%s["资金曲线"].iloc[-1])' % offset)

      exec('select_coin_%s["max2here"] = select_coin_%s["资金曲线"].expanding().max()' % (offset, offset))
      exec('spot_select_coin_%s["max2here"] = spot_select_coin_%s["资金曲线"].expanding().max()' % (offset, offset))
      exec('select_coin_%s["dd2here"] = select_coin_%s["资金曲线"] / select_coin_%s["max2here"] - 1' % (offset, offset, offset))
      exec('spot_select_coin_%s["dd2here"] = spot_select_coin_%s["资金曲线"] / spot_select_coin_%s["max2here"] - 1' % (offset, offset, offset))
      # exec('end_date, max_draw_down = tuple(select_coin_%s.sort_values(by=["dd2here"]).iloc[0][["candle_begin_time", "dd2here"]])' % offset)
      exec('end_date = select_coin_%s.sort_values(by=["dd2here"]).iloc[0]["candle_begin_time"]' % offset)
      exec('spot_end_date = spot_select_coin_%s.sort_values(by=["dd2here"]).iloc[0]["candle_begin_time"]' % offset)
      # exec('print(select_coin_%s.sort_values(by=["dd2here"]).iloc[0]["candle_begin_time"])' % offset)
      # exec('print(select_coin_%s.sort_values(by=["dd2here"])["dd2here"])' % offset)
      # exec('print(select_coin_%s.sort_values(by=["dd2here"])["dd2here"].iloc[0])' % offset)
      # exec('print(select_coin_%s.sort_values(by=["dd2here"])["dd2here"].iloc[0]["dd2here"])' % offset)
      # exec('print(select_coin_%s.sort_values(by=["dd2here"])["dd2here"].iloc[-1]["dd2here"])' % offset)
      exec('max_draw_down = select_coin_%s.sort_values(by=["dd2here"])["dd2here"].iloc[0]' % offset)
      exec('spot_max_draw_down = spot_select_coin_%s.sort_values(by=["dd2here"])["dd2here"].iloc[0]' % offset)
      # exec('print(end_date)')
      # exec('print(max_draw_down)')
      # print(max_draw_down)

      l = len(rtn)
      spot_l = len(spot_rtn)
      rtn.loc[l, 'offset'] = offset
      spot_rtn.loc[spot_l, 'offset'] = offset
      exec('rtn.loc[l, "最终收益"] = select_coin_%s.iloc[-1]["资金曲线"]' % offset)
      exec('spot_rtn.loc[spot_l, "最终收益"] = spot_select_coin_%s.iloc[-1]["资金曲线"]' % offset)
      exec('rtn.loc[l, "最大回撤"] = max_draw_down')
      exec('spot_rtn.loc[spot_l, "最大回撤"] = spot_max_draw_down')
      # rtn.loc[l, '最大回撤'] = max_draw_down
      # ===计算年化收益
      exec('annual_return = (select_coin_%s["资金曲线"].iloc[-1] / select_coin_%s["资金曲线"].iloc[0]) ** ("1 days 00:00:00" / (select_coin_%s["candle_begin_time"].iloc[-1] - select_coin_%s["candle_begin_time"].iloc[0]) * 365) - 1' % (offset, offset, offset, offset))
      exec('spot_annual_return = (spot_select_coin_%s["资金曲线"].iloc[-1] / spot_select_coin_%s["资金曲线"].iloc[0]) ** ("1 days 00:00:00" / (spot_select_coin_%s["candle_begin_time"].iloc[-1] - spot_select_coin_%s["candle_begin_time"].iloc[0]) * 365) - 1' % (offset, offset, offset, offset))
      # rtn.loc[l, '年化收益'] = str(round(annual_return, 2)) + ' 倍'
      exec('rtn.loc[l, "年化收益"] = str(round(annual_return, 2)) + " 倍"')
      exec('spot_rtn.loc[spot_l, "年化收益"] = str(round(spot_annual_return, 2)) + " 倍"')
      # print(annual_return)
      # exec('print(annual_return)')

      # if max_draw_down != 0:
      #   rtn.loc[l, '年化收益/回撤比'] = round(abs(annual_return / max_draw_down), 2)
      # else:
      #   rtn.loc[l, '年化收益/回撤比'] = 0
      exec("""if max_draw_down != 0:\n\trtn.loc[l, '年化收益/回撤比'] = round(abs(annual_return / max_draw_down), 2)\nelse:\n\trtn.loc[l, '年化收益/回撤比'] = 0""")
      exec("""if spot_max_draw_down != 0:\n\tspot_rtn.loc[spot_l, '年化收益/回撤比'] = round(abs(spot_annual_return / spot_max_draw_down), 2)\nelse:\n\tspot_rtn.loc[spot_l, '年化收益/回撤比'] = 0""")
      # ===统计每个周期
      exec('rtn.loc[l, "盈利周期数"] = len(select_coin_%s.loc[select_coin_%s["本周期多空涨跌幅"] > 0])' % (offset, offset))  # 盈利笔数
      exec('spot_rtn.loc[spot_l, "盈利周期数"] = len(spot_select_coin_%s.loc[spot_select_coin_%s["本周期多空涨跌幅"] > 0])' % (offset, offset))
      exec('rtn.loc[l, "亏损周期数"] = len(select_coin_%s.loc[select_coin_%s["本周期多空涨跌幅"] <= 0])' % (offset, offset))  # 亏损笔数
      exec('spot_rtn.loc[spot_l, "亏损周期数"] = len(spot_select_coin_%s.loc[spot_select_coin_%s["本周期多空涨跌幅"] <= 0])' % (offset, offset))
      exec('rtn.loc[l, "胜率"] = format(rtn.loc[l, "盈利周期数"] / len(select_coin_%s), ".2%s")' % (offset, '%'))  # 胜率
      exec('spot_rtn.loc[spot_l, "胜率"] = format(spot_rtn.loc[spot_l, "盈利周期数"] / len(spot_select_coin_%s), ".2%s")' % (offset, '%'))  # 胜率
      exec('rtn.loc[l, "每周期平均收益"] = format(select_coin_%s["本周期多空涨跌幅"].mean(), ".2%s")' % (offset, '%'))  # 每笔交易平均盈亏
      exec('spot_rtn.loc[spot_l, "每周期平均收益"] = format(spot_select_coin_%s["本周期多空涨跌幅"].mean(), ".2%s")' % (offset, '%'))
      exec('rtn.loc[l, "盈亏收益比"] = round(select_coin_%s.loc[select_coin_%s["本周期多空涨跌幅"] > 0]["本周期多空涨跌幅"].mean() / select_coin_%s.loc[select_coin_%s["本周期多空涨跌幅"] <= 0]["本周期多空涨跌幅"].mean() * (-1), 2)' % (offset, offset, offset, offset))  # 盈亏比
      exec('spot_rtn.loc[spot_l, "盈亏收益比"] = round(spot_select_coin_%s.loc[spot_select_coin_%s["本周期多空涨跌幅"] > 0]["本周期多空涨跌幅"].mean() / spot_select_coin_%s.loc[spot_select_coin_%s["本周期多空涨跌幅"] <= 0]["本周期多空涨跌幅"].mean() * (-1), 2)' % (offset, offset, offset, offset))
      exec('rtn.loc[l, "单周期最大盈利"] = format(select_coin_%s["本周期多空涨跌幅"].max(), ".2%s")' % (offset, '%'))  # 单笔最大盈利
      exec('spot_rtn.loc[spot_l, "单周期最大盈利"] = format(spot_select_coin_%s["本周期多空涨跌幅"].max(), ".2%s")' % (offset, '%'))  # 单笔最大盈利
      # if eval('spot_select_coin_%s["本周期多空涨跌幅"].max() > 2' % offset):
      #   eval('print(spot_select_coin_%s["本周期多空涨跌幅"])' % offset)
      exec('rtn.loc[l, "单周期大亏损"] = format(select_coin_%s["本周期多空涨跌幅"].min(), ".2%s")' % (offset, '%'))  # 单笔最大亏损
      exec('spot_rtn.loc[spot_l, "单周期大亏损"] = format(spot_select_coin_%s["本周期多空涨跌幅"].min(), ".2%s")' % (offset, '%'))  # 单笔最大亏损

      # ===连续盈利亏损
      exec('rtn.loc[l, "最大连续盈利周期数"] = max([len(list(v)) for k, v in itertools.groupby(np.where(select_coin_%s["本周期多空涨跌幅"] > 0, 1, np.nan))])' % offset)
      exec('spot_rtn.loc[spot_l, "最大连续盈利周期数"] = max([len(list(v)) for k, v in itertools.groupby(np.where(spot_select_coin_%s["本周期多空涨跌幅"] > 0, 1, np.nan))])' % offset)# 最大连续盈利次数
      exec('rtn.loc[l, "最大连续亏损周期数"] = max([len(list(v)) for k, v in itertools.groupby(np.where(select_coin_%s["本周期多空涨跌幅"] <= 0, 1, np.nan))])' % offset)
      exec('spot_rtn.loc[spot_l, "最大连续亏损周期数"] = max([len(list(v)) for k, v in itertools.groupby(np.where(spot_select_coin_%s["本周期多空涨跌幅"] <= 0, 1, np.nan))])' % offset)# 最大连续亏损次数

      # ax = plt.subplot(int("%d1%d" % (int(hold_hour[:-1]),offset+1)))
      # 支持 hold_hour >= 10
      plt.figure(0)
      ax = plt.subplot(int(self.hold_hour[:-1]), 1, offset + 1)
      plt.figure(1)
      ax2 = plt.subplot(int(self.hold_hour[:-1]), 1, offset + 1)
      plt.figure(0)
      exec('ax.plot(select_coin_%s["candle_begin_time"], select_coin_%s["资金曲线"])' % (offset, offset))
      plt.figure(1)
      exec('ax2.plot(spot_select_coin_%s["candle_begin_time"], spot_select_coin_%s["资金曲线"])' % (offset, offset))
    print(rtn)
    print(spot_rtn)
    print(datetime.now())
    rtn.to_csv('rtn.csv')
    spot_rtn.to_csv('spot_rtn.csv')
    print('所有offset平均"年化收益/回撤比" ', rtn['年化收益/回撤比'].mean())
    print('spot所有offset平均"年化收益/回撤比" ', spot_rtn['年化收益/回撤比'].mean())
    plt.figure(0)
    plt.legend(loc='best')
    plt.gcf().autofmt_xdate()
    plt.show()
    plt.figure(1)
    plt.legend(loc='best')
    plt.gcf().autofmt_xdate()
    plt.show()



if __name__ == '__main__':
    fac = Factory()
    fac.real_trade()


