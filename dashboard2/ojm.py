"""
The off-line job monitor
"""
from cfg import Cfg
Cfg.offline = True

import os

from showq import Sampler

#===============================================================================
if __name__=="__main__":

    sampler = Sampler()
    
    done = False
    while not done: 
        timestamp = sampler.sample(verbose=True)
        if not os.path.exists('ojm.running'):
            break 

    print('-- off-line job monitor started --')
