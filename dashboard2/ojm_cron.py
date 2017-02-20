"""
The off-line job monitor does offline sampling. This script is run on a login node as::

    > cd data/jobmonitor
    > module purge
    > module load hopper/2016a
    > module load Python
    > python ojm.py
    
This will produce an overview of the activity of the script as will as an 
overview of ill-performing jobs.
If you want to run the offline job monitor continuously::

    > nohup ./start.sh &

"""
from cfg import Cfg
Cfg.offline = True

import os,datetime,argparse,pickle

from showq      import Sampler
from titleline  import title_line

#===============================================================================
if __name__=="__main__":
    parser = argparse.ArgumentParser('ojm')
    parser.add_argument('--show_progress','-s',action='store_true')
    args = parser.parse_args()
#     print('ojm.py: command line arguments:',args)
    
    print(title_line('off-line job monitor cron job started  : {}'.format(datetime.datetime.now()),char='=',width=100,above=True))
    if os.path.exists('ojm_cron.pickled'):
        pickled = open('ojm_cron.pickled','rb')
        sampler = pickle.load(pickled)
        print('ojm_cron.pickled loaded')
    else:
        sampler = Sampler()    
    timestamp = sampler.sample(verbose=False,show_progress=args.show_progress)
    pickled = open('ojm_cron.pickled','wb')
    pickle.dump(sampler,pickled)
    
    print(title_line('off-line job monitor cron job completed: {}'.format(datetime.datetime.now()),char='=',width=100,below=True))
    
    


