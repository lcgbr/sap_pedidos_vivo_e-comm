#!/bin/bash

source /home/fferrary/.profile

cd /ce/sap

TIMESTAMP=$(date +"%FT%H-%M-%S" )
LOGFILE=/ce/sap/logs/"${TIMESTAMP}.log"

mkdir -p /ce/sap/logs

echo $(date +"%FT%H-%M-%S" ) "******* [sap_rt_bq.py] *******" | tee -a $LOGFILE
pipenv run python3 sap_rt_bq.py 2>&1 |& tee -a $LOGFILE
echo $(date +"%FT%H-%M-%S" ) "******* FIM *******" | tee -a $LOGFILE

