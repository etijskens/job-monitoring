"""
Module with data on the cluster - mainly on Hopper, but extensible

Classes and functions
=====================

"""
#===============================================================================    
def hopper_mem_avail_gb(node):
    """
    Return the memory available on a compute node on Hopper.
    
    :param str node: name of a compute node.
    :return: number of GB of memory available to the user on that node. 
    
    If *node* is a list of node names the total memory available is summed over 
    the nodes in the list. 
    """
    if isinstance(node,str):
        r = node[1]
        if r!='5':
            return 58
        c = node[3]
        if not c in '123':
            return 58
        cn = node[7]
        if not cn in '12345678':
            return 58
        return 256 # r5c[1-3]cn0[1-8]
    elif isinstance(node,list):
        mem = 0
        for ni in node:
            mem += hopper_mem_avail_gb(ni)
        return mem
    else:
        raise TypeError('Expected str, or list of str, got '+str(type(node)))
#===============================================================================
def hopper_ncores_per_node(node):
    """
    Return the number of cores of a compute node on Hopper.
    
    :param str node: name of a compute node.
    :return: number of cores on that node. 
    """
    return 20
#===============================================================================    
cluster_properties = {'hopper':{'ncores_per_node' : hopper_ncores_per_node
                               ,'login_nodes'     :['login.hpc.uantwerpen.be'
                                                   ,'login1-hopper.uantwerpen.be'
                                                   ,'login2-hopper.uantwerpen.be'
                                                   ,'login3-hopper.uantwerpen.be'
                                                   ,'login4-hopper.uantwerpen.be']
                               ,'mem_avail_gb'    : hopper_mem_avail_gb
                               }
                     }
#===============================================================================    
current_cluster = 'hopper'

#===============================================================================    
class ES:
    """
    "Namespace" class with ascii terminal escape sequences.
    """
    bell =         '\033[\07h'
    clear_screen = '\033[2J'
    # modes
    bold   = '\033[1m'
    dim    = '\033[2m'
    blink  = '\033[5m'
    normal = '\033[0m'
    reverse= '\033[7m'
    # colors
    black   = '\033[30m'
    red     = '\033[31m'
    green   = '\033[32m'
    blue    = '\033[34m'
    magenta = '\033[35m'
    white   = '\033[37m'
    default = '\033[39m'

#===============================================================================    
def str2gb(s):
    """
    Convert a string *s* containing and integer number and a unit (kb,mb,gb) to 
    the number of GB. 
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
#===============================================================================    

#===============================================================================    
# test code below
#===============================================================================    
if __name__=="__main__":
    print(ES.bold+ES.red+'bold'+ES.normal+'normal')
    for i in range(4):
        print(ES.bell)
        
    assert str2gb('1gb')==1
    assert str2gb('1GB')==1
    assert str2gb('10GB')==10
    assert str2gb('1024MB')==1
    assert str2gb('1048576kb')==1
    
    node_fmt = 'r5c{}cn0{}'
    for c in range(1,4):
        for cn in range(1,9):
            node = node_fmt.format(c,cn)
            assert hopper_mem_avail_gb(node)==256
            
    node_fmt = 'r6c{}cn0{}'
    for c in range(1,4):
        for cn in range(1,9):
            node = node_fmt.format(c,cn)
            assert hopper_mem_avail_gb(node)==58

    print('\n--finished--')
