"""
Module cpus.py. Collection of functions and classes to retrieve live information from 
compute nodes.

Classes and functions
=====================

"""
import remote
from mycollections import OrderedDict,od_first

#===============================================================================    
class ExecHost:
    """
    :param str exec_host_str: an exec_host description as reported by qstat or tracejob, e.g. "r5c1cn08.hopper.antwerpen.vsc/0-19+r5c6cn05.hopper.antwerpen.vsc/0-19".
    """
    #---------------------------------------------------------------------------    
    def __init__(self,exec_host_str):
        self.exec_host_str = exec_host_str
        self.data = OrderedDict()
        words = exec_host_str.split('+')
        self.ncores_all = 0
        for word in words:
            words2 = word.split('/')
            node = words2[0].split('.',1)[0]
            cores = words2[1]
            cores_list = cpu_list__(cores)
            self.ncores_all += len(cores_list)
            self.data[node] = (cores,cores_list)
        self.mhost = od_first(self.data)[0] 
    #---------------------------------------------------------------------------    
    def cores(self,cnode,as_list=False):
        """
        :param str cnode: compute node name, or *'mhost'*.
        :param bool as_list: determines the return type.
        :return: a str or list of int, describing which cores are used on *cnode*.
        """
        if cnode=='mhost':
            return self.cores(self.mhost, as_list)
        elif as_list:
            return self.data[cnode][1]
        else:
            return self.data[cnode][0]
    #---------------------------------------------------------------------------
    def ncores(self,cnode='all'):
        """
        :param str cnode: compute node name, *'mhost'*, or *'all'*.
        :return: the number of cores used on *cnode*.
        """
        if cnode=='all':
            return self.ncores_all
        elif cnode=='mhost':
            return self.ncores(self.mhost)
        else:
            return len(self.data[cnode][1])
    #---------------------------------------------------------------------------
    def nnodes(self):
        """
        :return: the number of compute nodes used by this job.  
        """
        return len(self.data)
    #---------------------------------------------------------------------------
    def nodes(self):
        """
        :return: the names of the compute nodes used by this job.
        :rtype: list of str  
        """
        return list(self.data.keys())
    #---------------------------------------------------------------------------
        
#===============================================================================    
def str2gb(s):
    """
    :param str s: a string containing an integer number and a unit (kb,mb,gb), e.g. '1234kb'.
    :return: number of GB, e.g. 1.205078125 (= 1234/1024)
    :rtype: int or float 
    """
    unit = s[-2:]
    unit = unit.lower()
    value = int(s[:-2])
    if unit=='kb':
        value /= 1024*1024
    elif unit=='mb':
        value /= 1024
    elif unit=='gb':
        pass
    else:
        raise ValueError('Unknown unit: '+s)
    return value
    #---------------------------------------------------------------------------

#===============================================================================    
def cpu_list__(s):
    """
    Convert string describing a list of cpus comprising comma-separated entries or 
    ranges 'first-last' to a sorted list of all cpus. E.g.:
    
    - '0,2,4,8' -> [0,2,4,8]
    - '0-3' -> [0,1,2,3]
    - '0-3,10' -> [0,1,2,3,10]
    - '0-3,10-13' -> [0,1,2,3,10,11,12,13]
    - '0-3,10-13,9' -> [0,1,2,3,9,10,11,12,13]
    
    :param str s: string with comma-separated cpu numbers (or ranges thereof).
    :return: list of ints with cpu numbers. 
    """
    ranges = s.split(',')
    cpus = []
    for range_ in ranges:
        first_last = range_.split('-')
        cpu = int(first_last[0])
        cpus.append(cpu)
        if len(first_last)>1:
            cpu +=1
            last=int(first_last[1])
            while cpu<=last:
                cpus.append(cpu)
                cpu+=1
    cpus.sort()
    return cpus
    #---------------------------------------------------------------------------    

#===============================================================================
def list_cores(compute_node,jobid):
    """
    List the cores used by a job on a compute node.
    
    :param str compute_node: name of the compute node.
    :param str jobid: job id
    :return list: list with the core numbers used by job ``jobid`` on compute node ``compute_node``.
    
    .. note:: not used anymore. 
    """
    lines = remote.run('ssh {} cat /dev/cpuset/torque/{}.hopper/cpus'.format(compute_node,jobid)
                      , post_processor=remote.list_of_lines
                      )
    if lines is None:
        cpus = []
    else:
        cpus = cpu_list__(lines[0])
    return cpus
    #---------------------------------------------------------------------------    
        
#===============================================================================
def run_sar_P(compute_node,cores=None):
    """
    This function runs the linux command ``sar -P ALL 1 1`` on a compute node 
    over ssh and returns its output as a list of lines. If cores is a list of core
    numbers, the output for the other cores is discarded.
    
    :param str compute_node: name of a compute node. 
    :param list cores: a list of core ids. 
    :returns: list with the relevant output lines
    """
    command = "ssh {} sar -P ALL 1 1".format(compute_node)
    lines = remote.run(command,post_processor=remote.list_of_lines)
    if lines is None:
        lines = ['command failed: '+command]
    # remove lines not containing 'Average:'
    lines_filtered = []
    for line in lines:
        if line.startswith('Average:'):
            line = line[16:] # remove the first column and the space between 1st and 2nd column, it is useless. 
            if not line.startswith('all'): # we do not keep the average because it is an all core average.
                lines_filtered.append(line)
                
    # remove the cores that were not requested 
    lines = lines_filtered 
    if cores is None:
        return lines
    else:
        # filter cores
        lines_filtered = [] 
        lines_filtered.append(lines[0]) 
        for cpu in cores:
            lines_filtered.append(lines[1+cpu]) 
        return lines_filtered
    #---------------------------------------------------------------------------    

#===============================================================================
def run_free(cnode):
    command = 'ssh {} free -m'.format(cnode)
    try:
        lines = remote.run(command, attempts=1, post_processor=remote.list_of_lines, raise_exception=True)
    except:
        return None
    for line in lines:
        if line.startswith('Swap:'):
            words = line.split()
            swap_used  = int(words[2])/1024 # GB
            swap_avail = int(words[1])/1024 # GB
            return [swap_used,swap_avail,swap_used/swap_avail] 
    #---------------------------------------------------------------------------    
            
#===============================================================================
#== test code below ============================================================
#===============================================================================
if __name__=="__main__":
    remote.connect_to_login_node()
    print(run_free('r4c6cn03'))
    assert str2gb('1gb')==1
    assert str2gb('1GB')==1
    assert str2gb('10GB')==10
    assert str2gb('1024MB')==1
    assert str2gb('1048576kb')==1

    eh = ExecHost("r5c1cn08.hopper.antwerpen.vsc/0-19+r5c6cn05.hopper.antwerpen.vsc/0-19")
    assert eh.mhost=='r5c1cn08'
    assert eh.ncores()==40
    assert eh.ncores('all')==40
    assert eh.ncores('mhost')==20
    assert eh.ncores('r5c1cn08')==20
    assert eh.ncores('r5c6cn05')==20
    assert eh.cores('mhost')=='0-19'
    assert eh.cores('r5c1cn08')=='0-19'
    assert eh.cores('r5c6cn05')=='0-19'

    cpus = cpu_list__('1')
    print(cpus)
    assert cpus==[1]
    
    cpus = cpu_list__('1,3')
    print(cpus)
    assert cpus==[1,3]

    cpus = cpu_list__('0-3')
    print(cpus)
    assert cpus==[0,1,2,3]

    cpus = cpu_list__('0-3,10-13')
    print(cpus)
    assert cpus==[0,1,2,3,10,11,12,13]

    cpus = cpu_list__('17,19,0-3,10-13')
    print(cpus)
    assert cpus==[0,1,2,3,10,11,12,13,17,19]

    cpus = cpu_list__('17,0-3,19,10-13')
    print(cpus)
    assert cpus==[0,1,2,3,10,11,12,13,17,19]
    
    
    print('finished')
