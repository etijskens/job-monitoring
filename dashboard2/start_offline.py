"""
This python (3.5) script starts the offline job monitor (ojm.py) on a login node.  

"""
import remote

#===============================================================================
if __name__=="__main__":
    # copy the offline job monitor  
        python_script = 'ojm.py'
        remote.copy_local_to_remote(python_script,'data/jobmonitor/'+python_script)
        
        command = 'cd data/jobmonitor; module load hopper/2016a; module load Python; echo $$ > ojm.pid; date > ojm.log; python --version 2>&1 >> ojm.log; python ojm.py 2>&1 >> ojm.log'
        remote.run_remote(command)
        
        print('-- off-line job monitor started --')
