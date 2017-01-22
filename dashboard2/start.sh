#!/bin/bash
# (to keep it running after you quit your session) execute this script as 
#   > nohup ./start.sh > /dev/null &
rm nohup.out
module purge
module load hopper/2016a
module load Python
echo $$ >  pid.txt
echo $$ >  ojm.log
date    >> ojm.log
python --version 2>&1 >> ojm.log
python ojm.py    2>&1 
