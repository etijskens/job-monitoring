"""
Classes and functions for storing and manipulating the output of::

    > qstat -x -f <jobid>
     
Classes and functions
=====================

"""
import remote
from collections import OrderedDict
from constants import str2gb
from cpus import cpu_list, Data_sar
from remote import CommandBase

#===============================================================================    
def run_qstat_f(jobid):
    """
    Runs::
    
        > qstat -x -f <jobid>
    
    on a login node and returns the output, as an OrderedDict.
    
    :param str jobid:  job id. 
    """
    result = remote.run("qstat -x -f "+jobid, post_processor=remote.xml_to_odict )
    if result is None:
        result = OrderedDict()
        result['error'] = str(CommandBase.last_error_messages)
    return result
    #---------------------------------------------------------------------------

#===============================================================================    
class Data_qstat:
    """
    Class for storing the output of::

        > qstat -x -f <jobid>

    :param str jobid: job id.

    Object properties:
       
        * jobid
        * data : the output of 'qstat -f <jobid>' consists of lines containing 'key = value'. The data property stores key-value pairs in an OrderDict.
        * node_cores : a dict with allocated compute nodes as key and a comma-separated range list identifying the allocated cores as values.
        * node_sar : a dict for storing Data_sar objects for each node.  
    """
    #---------------------------------------------------------------------------    
    def __init__(self,jobid,offline_test__=False):
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
        """
        :return: number of nodes allocated to this job.
        :rtype: int
        """
        return len( self.node_cores )
    #---------------------------------------------------------------------------
    def get_ncores(self):
        """
        :return: total number of cores allocated to this job.
        :rtype: int
        """
        ncores = 0
        for cores in self.node_cores.values():
            ncores += len(cpu_list(cores))
        return ncores
    #---------------------------------------------------------------------------    
    def get_exec_host(self):
        """
        :return: exec_host value of this job. This lists the nodes and cores on which the job is running in detail.
        :rtype: str
        """
        value = self.data['exec_host']
        return value
    #---------------------------------------------------------------------------    
    def get_username(self):
        """
        :return: username of this job's owner.
        :rtype: str
        """
        value = self.data['Job_Owner']
        value = value.split('@')[0]
        return value
    #---------------------------------------------------------------------------    
    def get_master_node(self):
        """
        :return: master node of the job (= first one in the 'exec_host' entry).  
        :rtype: str

        This is the node where the job script can be found
        """
        value = self.data['exec_host'].split('/',1)[0]
        return value
    #---------------------------------------------------------------------------    
    def get_walltime_remaining(self,fmt=True):
        """
        :return: the remaining walltime as 'hh:mm:ss'.
        :rtype: str
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
        """
        :return: the walltime used so far.
        :rtype: str
        """
        try: 
            value = self.data['resources_used']['walltime']
        except KeyError as e:
            print(e)
            value = '?'
        return value
    #---------------------------------------------------------------------------    
    def get_job_state(self,fmt=True):
        """
        Return job status as readable string 
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
        """
        :return: memory used by the job (GB).
        :rtype: number
        """
        try:
            s = self.data['resources_used']['mem'] # returns a str such as 'NNNNNNNNNkb'
        except KeyError:
            return 0
        value = str2gb(s)
        return value 
    #---------------------------------------------------------------------------    
    def get_mem_requested(self):
        """
        :return: memory requested by the job (GB).
        :rtype: number
        """
        try:
            s = self.data['Resource_List']['mem'] # returns a str such as 'NNgb'
        except KeyError:
            return 0 # nothing was requested?
        value = str2gb(s)
        return value
    #---------------------------------------------------------------------------    
    def sar(self):
        """
        Run linux command sar on all nodes of the job and store processed output
        in an OrderedDict.
        """
        for compute_node,cores in self.node_cores.items():
            cores = cpu_list(cores)
            # find the load of these cores (sar)
            # can't avoid disturbing the compute node this time
            data_sar = Data_sar(compute_node,cores)
            self.node_sar[compute_node] = data_sar                
    #---------------------------------------------------------------------------        
    def is_interactive_job(self):
        """
        Test if this job is interactive.
        """
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
