"""
A Python (3.5) script that copies the job monitor to the remote location ~/data/jobmonitor.
This makes the offline job monitor available.

No command line arguments.
"""

import remote
import glob

#===============================================================================
if __name__=="__main__":
    remote.connect_to_login_node()
    # copy the offline job monitor    
    extensions = ['*.py','*.sh']
    for ext in extensions:
        files_needed = glob.glob(ext) 
        for file in files_needed:
            remote.copy_local_to_remote(file,'data/jobmonitor/'+file)
            print(file)
    print('\n--files copied--')
    