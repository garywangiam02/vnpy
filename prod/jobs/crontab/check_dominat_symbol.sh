#!/bin/bash
CONDA_HOME=/root/anaconda3
source $CONDA_HOME/bin/deactivate

source $CONDA_HOME/bin/activate vnpy2
############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended
export PYTHONPATH=/root/workspace/vnpy/
PROGRAM_NAME=/root/workspace/vnpy/prod/jobs/check_dominat_symbol.py
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME prod/future_hz01 >/root/workspace/vnpy/prod/jobs/crontab/log/check_dominat_symbol.log 2>>/root/workspace/vnpy/prod/jobs/crontab/log/check_dominat_symbol-error.log &
