#!/bin/bash
CONDA_HOME=/root/anaconda3
source $CONDA_HOME/bin/deactivate

source $CONDA_HOME/bin/activate vnpy2
############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended
export PYTHONPATH=/root/workspace/vnpy/
PROGRAM_NAME=/root/workspace/vnpy/prod/jobs/daily_stock_adjust_grids.py
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME gary_stock_em01 >/root/workspace/vnpy/prod/jobs/crontab/log/daily_stock_adjust_grids.log 2>>/root/workspace/vnpy/prod/jobs/crontab/log/daily_stock_adjust_grids-error.log &
