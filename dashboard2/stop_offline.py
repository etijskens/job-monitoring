"""
This python (3.5) script stops the offline job monitor (ojm.py) on a login node.  

"""
import constants
# constants.fix_login_node(1)

import remote

#===============================================================================
if __name__=="__main__":
        
    command = 'rm data/jobmonitor/ojm.running'

    while True:
        try:    
            remote.run_remote(command)
        except Exception as e:
            print(e)
            continue # try again
        break
        
    print('-- off-line job monitor stopped --')
