module purge
module load hopper/2016a
module load Python
echo $$ >  ojm.log
date    >> ojm.log
python --version 2>&1 >> ojm.log
python ojm.py    2>&1 >> ojm.log
echo $$ > pid.txt

