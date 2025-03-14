#!/bin/bash

source /home/fferrary/.profile

cd /ce/sap

TIMESTAMP=$(date +"%FT%H-%M-%S")
LOGFILE=/ce/sap/logs/"${TIMESTAMP}_repro.log"

mkdir -p /ce/sap/logs

echo $(date +"%FT%H-%M-%S" ) "******* [sap_repro_bq_d30.py] *******" | tee -a $LOGFILE
pipenv run python3 sap_repro_bq_d30.py 2>&1 |& tee -a $LOGFILE
echo $(date +"%FT%H-%M-%S" ) "******* FIM *******" | tee -a $LOGFILE

