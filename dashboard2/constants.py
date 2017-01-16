
# all_procs = -1000

def hopper_mem_avail_gb(node):
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
    
cluster_properties = {'hopper':{'ncores_per_node' : 20
                               ,'login_node'      :'login.hpc.uantwerpen.be'
                               ,'mem_avail_gb'    : hopper_mem_avail_gb
                               }
                     }
current_cluster = list(cluster_properties.keys())[0]

dashed_line = 80*'-'

# ascii terminal escape sequences
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

def str2gb(s):
    """
    s = 'NN...NNuu'  
    :return: int number of GB
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


################################################################################
# test code below
################################################################################
if __name__=="__main__":
    print(bold+red+'bold'+normal+'normal')
    for i in range(4):
        print(bell)
        
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
