from remote import run_remote
from collections import OrderedDict
from constants import str2gb
from cpus import cpu_list, Data_sar

#===============================================================================    
def run_qstat_f(jobid):
    """
    Runs ``qstat -x -f <jobid>`` on a login node and returns the output, as an OrderedDict.
    
    :str jobid:  job id. 
    """
    try:
        result = run_remote("qstat -x -f "+jobid)
        # the '--xml' is replaced with '-x' (qstat only understands '-x'
        # the '--xml' is used to trigger the parsing of the xml output.
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
    def __init__(self,jobid,offline_test__=False):
        """
        """
        self.jobid = jobid
        
        if offline_test__:
            import xmltodict
            xml_dict = xmltodict.parse( open('qstat.xml').read() )
            self.data = xml_dict['Data']['Job']
        else:
            xml_dict = run_qstat_f(jobid)
            self.data = xml_dict['Data']['Job']

        self.node_sar   = OrderedDict()
        self.node_cores = OrderedDict() 
        nodes_str = self.get_exec_host()
        words = nodes_str.split('+')
        for word in words:
            words2 = word.split('/')
            node = words2[0].split('.',1)[0]
            self.node_cores[node] = words2[1]
    #---------------------------------------------------------------------------    
    def get_nnodes(self):
        return len( self.node_cores )
    #---------------------------------------------------------------------------
    def get_ncores(self):
        ncores = 0
        for cores in self.node_cores.values():
            ncores += len(cpu_list(cores))
        return ncores
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
        try:
            value = int(self.data['Walltime']['Remaining'])
        except KeyError:
            return '?'
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
            value = self.data['resources_used']['walltime']
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
            s = self.data['resources_used']['mem'] # returns a str such as 'NNNNNNNNNkb'
        except KeyError:
            return 0
        value = str2gb(s)
        return value 
    #---------------------------------------------------------------------------    
    def get_mem_requested(self):
        try:
            s = self.data['Resource_List']['mem'] # returns a str such as 'NNgb'
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
    def is_interactive_job(self):
        try:
            s = self.data['submit_args']
            tf = '-I' in s
            return tf
        except:
            return False
    #---------------------------------------------------------------------------        
################################################################################
# test code below
################################################################################
if __name__=="__main__":
    try:
        import connect_me
    except:
        test__ = True
    
    jobid = '390159' 
    qstat = Data_qstat(jobid,offline_test__=False)
    for key,val in qstat.data.items():
        print('  *',key,':',val)
    print('\nqstat.node_cores:',qstat.node_cores)
    print('\nqstat.get_job_state():',qstat.get_job_state())
    print('\nqstat.get_walltime_remaining():',qstat.get_walltime_remaining())
    print('\nqstat.get_walltime_used():',qstat.get_walltime_used())
    print('\nqstat.get_username():',qstat.get_username())
    print('\nqstat.get_mem_requested():',qstat.get_mem_requested())
    print('\nqstat.get_mem_used():',qstat.get_mem_used())
            
    print('\n--finished--')
