#!/bin/bash
CONDA_HOME=/root/anaconda3
source $CONDA_HOME/bin/deactivate

source $CONDA_HOME/bin/activate vnpy2
export PATH=$PATH:/usr/local/bin
export PYTHONPATH=/root/workspace/vnpy/

cd /root/workspace/vnpy/prod/gary_future_hz01
PROGRAM_NAME=service.py
# /root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME schedule
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME schedule >/root/workspace/vnpy/prod/jobs/crontab/log/service_hz01.log 2>>/root/workspace/vnpy/prod/jobs/crontab/log/service-error_hz01.log &
