#!/bin/bash
CONDA_HOME=/root/anaconda3
source $CONDA_HOME/bin/deactivate

source $CONDA_HOME/bin/activate vnpy2
export PATH=$PATH:/usr/local/bin
export PYTHONPATH=/root/workspace/vnpy/

PROGRAM_NAME=/root/workspace/vnpy/prod/gary_stock_em01/run_screener.py
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME >/root/workspace/vnpy/prod/jobs/crontab/log/screener.log 2>>/root/workspace/vnpy/prod/jobs/crontab/log/screener-error.log &
