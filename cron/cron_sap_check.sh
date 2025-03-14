#!/bin/bash

source /home/fferrary/.profile

cd /ce/sap

pipenv run python3 sap_check.py >> check.log

