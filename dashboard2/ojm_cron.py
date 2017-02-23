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

import os,datetime,argparse,pickle,gzip

from showq      import Sampler
from titleline  import title_line

#===============================================================================
if __name__=="__main__":
    start = datetime.datetime.now()
    
    parser = argparse.ArgumentParser('ojm')
    parser.add_argument('--show_progress','-s',action='store_true')
    args = parser.parse_args()
#     print('ojm.py: command line arguments:',args)
    
    print(title_line('off-line job monitor cron job started  : {}'.format(start),char='=',width=100,above=True))
    if os.path.exists('ojm_cron.pickled.gz'):
        print('Loading ojm_cron.pickled.gz ...',end='')
        fo = gzip.open('ojm_cron.pickled.gz','rb')
    elif os.path.exists('ojm_cron.pickled'):
        print('Loading ojm_cron.pickled ...',end='')
        fo = open('ojm_cron.pickled','rb')
    else:
        print('Creating new Sampler ...',end='')
        fo = None
        sampler = Sampler()    
    if fo:    
        sampler = pickle.load(fo)
        fo.close()
        print('done')
    timestamp = sampler.sample(verbose=False,show_progress=args.show_progress)
    with gzip.open('ojm_cron.pickled.gz','wb') as fo:
        pickle.dump(sampler,fo)

    duration = datetime.datetime.now()-start
    print(title_line('off-line job monitor cron job completed in {}s'.format(duration.seconds),char='=',width=100,below=True))
    
    


