#!/bin/bash
CONDA_HOME=/root/anaconda3
source $CONDA_HOME/bin/deactivate

source $CONDA_HOME/bin/activate vnpy2

############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended
export PYTHONPATH=/root/workspace/vnpy/
PROGRAM_NAME=/root/workspace/vnpy/vnpy/data/stock/adjust_factor.py
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME >/root/workspace/vnpy/prod/jobs/crontab/log/adjust_factor.log 2>>/root/workspace/vnpy/prod/jobs/crontab/log/adjust_factor-error.log &
