"""
This python (3.5) script starts the offline job monitor (ojm.py) on a login node.  

"""
import constants
constants.fix_login_node(1)

import remote
from time import sleep
import glob

#===============================================================================
if __name__=="__main__":
    # copy the offline job monitor
    files_needed = glob.glob('*.py') 
    for file in files_needed:
        remote.copy_local_to_remote(file,'data/jobmonitor/'+file)
        print(file)
    print('\n--files copied--')
    exit(0)
    
    command = 'cd data/jobmonitor; module load hopper/2016a; module load Python; echo $$ > ojm.pid; date > ojm.log; python --version 2>&1 >> ojm.log; python ojm.py 2>&1 >> ojm.log'
    lines = remote.run_remote(command)
    for line in lines:
        print(line)
        
    sleep(5)
    ojm_pid ='ojm.pid'
    remote.copy_remote_to_local(ojm_pid, 'data/jobmonitor/'+ojm_pid)
    with open(ojm_pid,'r') as p:
        for line in p:
            print(line)
    
    print('-- off-line job monitor started --')
