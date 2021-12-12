#!/bin/bash

CONDA_HOME=/root/anaconda3

############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended

BASE_PATH=$(cd `dirname $0`; pwd)
echo $BASE_PATH
cd `dirname $0`
PROGRAM_NAME=./service.py

$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME restart
