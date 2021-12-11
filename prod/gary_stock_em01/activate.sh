#!/bin/bash
source activate
source deactivate
conda activate vnpy2 

############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended

BASE_PATH=$(cd `dirname $0`; pwd)
echo $BASE_PATH
cd `dirname $0`
PROGRAM_NAME=./run_service.py
/root/anaconda3/envs/vnpy2/bin/python $PROGRAM_NAME >$BASE_PATH/log/service.log 2>>$BASE_PATH/log/service-error.log &
