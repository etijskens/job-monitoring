"""
Module sar.py. Collection of functions and classes to retrieve live information from 
compute nodes.

Classes and functions
=====================

"""
import remote
from es import ES

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
class Data_sar:
    """
    Class for storing and manipulating the output of ``sar -P ALL 1 1`` on a compute node.
    The constructor arguments are the same as for :func:`run_sar_P`.
    
    :param str compute_node: name of a compute node. 
    :param list cores: a list of core ids.
    """
    line_fmt = '{:3}{:10.2f}{:10.2f}{:10.2f}{:10.2f}{:10.2f}{:10.2f}'
    #---------------------------------------------------------------------------    
    def __init__(self,compute_node,cores=None):
        self.compute_node = compute_node
        self.cores = cores
        self.data = run_sar_P( compute_node, cores )
        
        self.data_cores = [self.data[0]]
        for line in self.data[1:]:
            core = int(line.split()[0])
            if core in cores:
                self.data_cores.append(line)
            
        self.columns = {}
        colum_headers = self.data[0].split()
        for hdr in colum_headers:
            self.columns[hdr] = []
        
        for line in self.data[1:]:
            row = line.split()
            for col,value in enumerate(row):
                hdr = colum_headers[col]
                if col==0:
                    try:
                        value = int(value)
                    except: # value == 'avg'
                        pass
                    self.columns[hdr].append(value) 
                else:
                    self.columns[hdr].append(float(value)) # percentage
        # compute averages
        for hdr,column in self.columns.items():
            if hdr=='CPU':
                column.insert(0,len(self.data_cores)-1)
            else:
                avg = sum(column)/len(column)
                column.insert(0,avg)
        avg_line = Data_sar.line_fmt.format(self.columns['CPU'    ][0]
                                           ,self.columns['%user'  ][0]
                                           ,self.columns['%nice'  ][0]
                                           ,self.columns['%system'][0]
                                           ,self.columns['%iowait'][0]
                                           ,self.columns['%steal' ][0]
                                           ,self.columns['%idle'  ][0]
                                           )
        self.data_cores.insert(1,avg_line)
    #---------------------------------------------------------------------------    
    def get(self,column_header,core_id):
        """
        Read the output value in the column with header *column_header* for row *core_id*.
        
        :param str column_header: header of the column.
        :param int core_id: core number 
        """
        irow = self.columns['CPU'].index(core_id)
        value = self.columns[column_header][irow]
        return value 
    #---------------------------------------------------------------------------    
    def verify_load(self,threshold):
        """
        Returns *True* if all cores have a load > *threshold*, *False* otherwise.
        """
        self.threshold = threshold # used by self.message()
        percent_user = self.columns['%user']
        self.ok = False
        for i in range(len(percent_user)):
            if percent_user[i]<=threshold:
                break
        else:
            self.ok = True
        return self.ok
    #---------------------------------------------------------------------------    
    def message(self,fmt=False):
        """
        Format the data in a message and return it.
        """
        cpu            = self.columns['CPU'    ]
        percent_user   = self.columns['%user'  ]
        percent_system = self.columns['%system']
        percent_iowait = self.columns['%iowait']
        percent_idle   = self.columns['%idle'  ]
        cn = self.compute_node.split('.',1)[0]
        if fmt:
            # construct formatted message
            msg = (ES.blue+(len(cn)*' ')+' {:<3}{:>10}{:>10}{:>10}{:>10}'+ES.default+'\n').format('CPU','%user','%system','%iowait','%idle')
            fmt_ok_cpu_user     = ES.dim+     cn+' {:>3}{:>10.2f}'+ES.normal+ES.default
            fmt_not_ok_cpu_user = ES.bold+ES.red+cn+' {:>3}{:>10.2f}'+ES.normal
            fmt_not_ok          = ES.bold+ES.red+'{:>10.2f}'+ES.normal
            fmt_ok              =      ES.dim+'{:>10.2f}'+ES.normal
            t = round((100-self.threshold)/5,2)
            for i in range(len(percent_user)):
                if percent_user[i]<=self.threshold:
                    frmt = fmt_not_ok_cpu_user
                    if percent_system[i]>t:
                        frmt += fmt_not_ok
                    else:
                        frmt += fmt_ok
                    if percent_iowait[i]>t:
                        frmt += fmt_not_ok
                    else:
                        frmt += fmt_ok
                    if percent_idle[i]>t:
                        frmt += fmt_not_ok
                    else:
                        frmt += fmt_ok
                    msg += frmt.format(cpu[i],percent_user[i],percent_system[i],percent_iowait[i],percent_idle[i])
                else:
                    frmt = fmt_ok_cpu_user
                    msg += frmt.format(cpu[i],percent_user[i])
                msg += '\n'
        else:
            # construct unformatted message
            msg = '\n'.join(self.data)
            
        return msg
    #---------------------------------------------------------------------------    
     
#===============================================================================
# class Data_vmstat:
#     """
#     Class for storing and manipulating vmstat output of a compute node. Uses linux 
#     command ``vmstat``. I found the following links useful: 
#     
#     - `use-vmstat-to-monitor-system-performance <https://www.linode.com/docs/uptime/monitoring/use-vmstat-to-monitor-system-performance>`_
#     - `Check-Swap-Space-in-Linux <http://www.wikihow.com/Check-Swap-Space-in-Linux>`_
#     - `linux-which-process-is-using-swap <https://www.cyberciti.biz/faq/linux-which-process-is-using-swap>`_
#     - `Monitoring Virtual Memory with vmstat <http://www.linuxjournal.com/article/8178>`_
#     - `Tips for Monitoring Memory Usage in PBS jobs on Discover <https://www.nccs.nasa.gov/images/Montioring-Job-memory-brownbag.pdf>`_
#     """
#     def __init__(self,compute_node):
#         """"""
#         command = "ssh {} vmstat 1 1".format(compute_node)
#         try:
#             lines = remote.run(command,attempts=1,raise_exception=True)
#         except Exception as e:
#             remote.err_print(type(e),e)
#     #---------------------------------------------------------------------------    
        
#===============================================================================
# test code below ==============================================================
#===============================================================================
if __name__=="__main__":
    remote.connect_to_login_node()

    data_sar = Data_sar('r5c4cn04',[0,2,4,6])
    
    for line in data_sar.data:
        print(line)
    
    print('percent_user[0] =',data_sar.get('%user',0))

    threshold = 99.5
    not_ok = data_sar.verify_load(threshold)
    if not_ok:
        for line in not_ok:
            print(line)
    else:
        print('all cores ok')
    
    threshold = 70
    not_ok = data_sar.verify_load(threshold)
    if not_ok:
        for line in not_ok:
            print(line)
    else:
        print('all cores ok')
    
    
    print('finished')
