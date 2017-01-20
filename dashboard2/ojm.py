"""
The off-line job monitor
"""
from cfg import Cfg
from progress import printProgress
Cfg.offline = True

import os,time

from showq import Sampler

#===============================================================================
if __name__=="__main__":
    verbose = False
    
    sampler = Sampler()
    
    running_flag = 'ojm.running'
    with open(running_flag,'w') as f:
        f.write('running')
    assert os.path.exists('ojm.running')
    
    print('-- off-line job monitor started --')
    stopped = False
    while not stopped: 
        timestamp = sampler.sample(verbose=verbose)
        stopped = not os.path.exists('ojm.running')
        minutes_to_sleep = int(Cfg.sampling_interval/60)
        print()
        for m in range(minutes_to_sleep+1):
            printProgress( m, minutes_to_sleep, prefix = 'Sleeping: ', suffix='minutes', decimals=-1)
            time.sleep(60)
        print('\n')

    print('\n\n-- off-line job monitor stopped --')



