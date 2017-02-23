"""
Script that provides an overview of the offline job monitor.
"""
import argparse
import remote
from titleline import title_line

#===============================================================================    
def ojm_cron_overview():
    """
    Main functIon of the script.
    Print overview of the offline job monitorl
    
    * Tests if ojm.py is running, and if not prints 'ojm.err'
    * print a list of (new) completed jobs with warnings
    * print a list of running jobs with warnings
    
    :param [only_new_completed_jobs]: if *True* only new completed jobs with warnings are listed, otherwise old completed jobs are listed too. 
    
    """
    command = 'cat data/jobmonitor/ojm_cron.err'
    print(title_line(command, width=100, char='-', above=True))
    lines = remote.run(command, post_processor=remote.list_of_lines)
    if lines:
        for line in lines:
            print(line)
    print(title_line(width=100, char='-'))
        
    print(title_line('Completed jobs', width=100, char='-', above=True, below=True))
    command = 'ls data/jobmonitor/completed/'
    lines = remote.run(command,post_processor=remote.list_of_non_empty_lines,attempts=1,verbose=False)
    n = 0
    if lines:
        n = len(lines)
        for line in lines:
            print(line)
    print(n,'new completed jobs with warnings.\n')
        
    print(title_line('Running jobs', width=100, char='-', above=True, below=True))
    command = 'ls data/jobmonitor/running/'
    lines = remote.run(command,post_processor=remote.list_of_non_empty_lines)
    for line in lines:
        print(line)
    print(len(lines),'running jobs with warnings.\n')
    print(title_line(width=100, char='-'))
    #---------------------------------------------------------------------------

#===============================================================================    
if __name__=='__main__':
    remote.connect_to_login_node()

#     parser = argparse.ArgumentParser('offline_overview')
#     parser.add_argument('--old','-o',action='store_true')
#     args = parser.parse_args()
    print('ojm_cron_overview.py')
    ojm_cron_overview()
