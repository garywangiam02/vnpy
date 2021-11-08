#!/bin/bash
CONDA_HOME=/root/anaconda3
source $CONDA_HOME/bin/deactivate

source $CONDA_HOME/bin/activate vnpy2
pwd
echo "开始3---------------------------"
export PYTHONPATH=/root/workspace/vnpy/
############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended

PROGRAM_NAME=/root/workspace/vnpy/prod/stock_em01/service.py
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME schedule >/root/workspace/vnpy/prod/jobs/crontab/log/service_em01.log 2>>/root/workspace/vnpy/prod/jobs/crontab/log/service-error_em01.log &
