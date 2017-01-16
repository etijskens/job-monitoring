from remote import run_remote
from _collections import OrderedDict
from constants import bold, normal, red, green, default, str2gb
import datetime
from cpus import cpu_list, Data_sar
from script import Data_jobscript

_test = False

#===============================================================================    
def run_qstat_f(jobid):
    """
    Runs ``qstat -f <jobid>`` on a login node and returns the output, as an OrderedDict.
    
    :str jobid:  job id. 
    """
    try:
        result = run_remote("qstat -f "+jobid)
    except Exception as e:
        result = OrderedDict()
        result['error'] = str(e)
    return result

#===============================================================================    
class Data_qstat:
    """
    Class for storing the output of 'qstat -f <jobid>'
    Object properties:
       
        * jobid
        * data : the output of 'qstat -f <jobid>' consists of lines containing 'key = value'. The data property stores key-value pairs in an OrderDict.
        * node_cores : a dict with allocated compute nodes as key and a comma-separated range list identifying the allocated cores as values.
        * node_sar : a dict for storing Data_sar objects for each node.  
    """
    #---------------------------------------------------------------------------    
    def __init__(self,jobid):
        """
        """
        self.jobid = jobid
        lines = run_qstat_f(jobid)
        self.data = OrderedDict()
        
        # skip the last two line as they are empty
        nlines = len(lines)-2
        # skip first line which contains the jobid
        l = 1
        # the next lines start with 4 spaces (which we also ignore)
        # if the line starts with a tab character '\t', it is a continuation of the previous line 
        while l<nlines:            
            words = lines[l][4:].split('=',1)
            key = words[0][:-1]
#             print('\n%%  key',key) 
            value = words[1][1:]
            l += 1
            while l<nlines:
                if lines[l][0]=='\t':
                    value = value + lines[l][1:]
#                     print('%%  val',value)           
                else:
                    break
                l += 1
#             print('%%  val',value)
#             print(lines[l])           
#             print(key,value)
            self.data[key] = value

        self.node_cores = OrderedDict() 
        try:
            nodes_str = self.get_exec_host()
        except KeyError as e:
            # todo investigate why this is happening
            print(e)
            print(self.data)
            return
        words = nodes_str.split('+')
        self.node_sar = {}
        self.script = None
        self.report = ''
        self.performance_ok = None
        for word in words:
            words2 = word.split('/')
            self.node_cores[words2[0].split('.',1)[0]] = words2[1]
            
        # these are to be filled in by Data_showq.check_performance()
    
    #---------------------------------------------------------------------------    
    def get_nnodes(self):
        return len( self.node_cores )
    
    #---------------------------------------------------------------------------    
    def get_exec_host(self):
        value = self.data['exec_host']
        return value
    
    #---------------------------------------------------------------------------    
    def get_username(self):
        value = self.data['Job_Owner']
        value = value.split('@')[0]
        return value
    
    #---------------------------------------------------------------------------    
    def get_master_node(self):
        """
        return the master node (the first one in the 'exec_host' entry. 
        This is the node where the job script can be found
        """
        value = self.data['exec_host'].split('/',1)[0]
        return value
    
    #---------------------------------------------------------------------------    
    def get_walltime_remaining(self,fmt=True):
        """
        Returns the remaining walltime as 'hh:mm:ss'
        """
        value = int(self.data['Walltime.Remaining'])
        if not fmt:
            return value 
        hours = value//3600
        value -= hours*3600
        minutes = value//60
        value -= minutes*60
        seconds = value
        s = '{:0>2d}:{:0>2d}:{:0>2d}'.format(hours,minutes,seconds)
        return s
    
    #---------------------------------------------------------------------------    
    def get_walltime_used(self):
        try: 
            value = self.data['resources_used.walltime']
        except KeyError as e:
            print(e)
            value = '?'
        return value
    
    #---------------------------------------------------------------------------    
    def get_job_state(self,fmt=True):
        """
        return job status as readable string 
        """
        value = self.data['job_state']
        if not fmt:
            return value 
        s = {'C':'completed'
            ,'E':'exiting'
            ,'H':'on hold'
            ,'Q':'queued, eligible to run, or routed'
            ,'R':'running'
            ,'T':'Job is being moved to new location.'
            ,'W':'Job is waiting for its execution time (-a option) to be reached.'
            ,'S':'suspended'
            }[value]
        return '{} ({})'.format(value,s)
    
    #---------------------------------------------------------------------------    
    def get_mem_used(self):
        try:
            s = self.data['resources_used.mem'] # returns a str such as 'NNNNNNNNNkb'
        except KeyError:
            return 0
        value = str2gb(s)
        return value
        
    #---------------------------------------------------------------------------    
    def get_mem_requested(self):
        try:
            s = self.data['Resource_List.mem'] # returns a str such as 'NNgb'
        except KeyError:
            return 0 # nothing was requested?
        value = str2gb(s)
        return value
    
    #---------------------------------------------------------------------------    
    def sar(self):
        for compute_node,cores in self.node_cores.items():
            cores = cpu_list(cores)
            # find the load of these cores (sar)
            # can't avoid disturbing the compute node this time
            data_sar = Data_sar(compute_node,cores)
            self.node_sar[compute_node] = data_sar                
    #---------------------------------------------------------------------------        

################################################################################
# test code below
################################################################################
if __name__=="__main__":
    try:
        import connect_me
    except:
        _test = True
    
    jobid = '382830' 
    qstat = Data_qstat(jobid)
    print(qstat.data)
    print(qstat.node_cores)
    print(qstat.get_job_state())
    print(qstat.get_walltime_remaining())
    print(qstat.get_job_state(False))
    print(qstat.get_walltime_remaining(False))
        
    print('\n--finished--')
