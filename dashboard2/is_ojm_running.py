"""
Function and script to test if the offline job monitor ojm.py is running any of 
the login nodes of the cluster. Optionally, stops the offline job monitor.

Classes and functions
=====================

"""
import remote
import argparse
import mycollections
from cluster import current_cluster

#===============================================================================    
def is_ojm_running(kill=False):
    """
    Verifies if the offline job monitor is running on one of the login nodes.
    
    :param kill: if True kills the associated processe(s).
    :return: OrderedDict of {#login_node:[proces ids]} on which the offline job monitor is running. If *kill==True* it is empty.
    """
    remote.Connection.verbose = False
    login_nodes = remote.cluster_properties[remote.current_cluster]['login_nodes']
    result = mycollections.OrderedDict()
    for login_node in range(1,len(login_nodes)):
        remote.connect_to_login_node(login_node=login_node)
        username = remote.logindetails.me[0]
        command = "ps aux | grep '{}'".format(username)
        cmd = remote.RemoteCommand(command)
        lines = cmd.execute(post_processor=remote.list_of_lines)
        found = False
        for line in lines:
            if './start.sh' in line \
            or 'ojm.py'     in line :
                print(line)
                found = True
                psid = line.split(' ',2)[1]
                mycollections.od_add_list_item(result,login_node,psid)
                if kill:
                    cmd = remote.RemoteCommand('kill '+psid)
                    try:
                        cmd.execute()
                        print(cmd.str())
                    except Exception as e:
                        print(type(e),e)
                        print('failed:',cmd.str())
        if found:
            if kill:
                print('ojm.py was running on',login_nodes[login_node],'but has just been killed.\n')
            else:
                print('ojm.py is running on',login_nodes[login_node],'\n')
    if not result:
        remote.err_print('ojm.py is not running on',current_cluster,'\n')
    return result 
    #---------------------------------------------------------------------------

#===============================================================================    
if __name__=='__main__':
    
    parser = argparse.ArgumentParser('is_ojm_running.py')
    parser.add_argument('-k','--kill',action='store_true')
    args = parser.parse_args()
    print('is_ojm_running.py: command line arguments:',args,'\n')
    is_ojm_running(args.kill)