#!/bin/bash
source /etc/bashrc
module purge
module load hopper/2016a
module load Python/3.5.1-intel-2016a

cd /user/antwerpen/201/vsc20170/data/jobmonitor

if [ ! -f ojm_cron.pickled ]; then
    date > ojm_cron.err
    rm -f ojm_cron.out
    rm -f ojm_cron.log
fi
#which python >> ojm_cron.out

START=$(date)
python ojm_cron.py 2>> ojm_cron.err 1>> ojm_cron.out
STOP=$(date)
echo ${START} './ojm_cron.sh' ${STOP} >> ojm_cron.log
