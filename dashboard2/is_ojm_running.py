import remote
import argparse

#===============================================================================    
def is_ojm_running(kill=False):
    """
    Verifies if the offline job monitor is running on one of the login nodes.
    
    :param kill: if True kills the associated processe(s)
    :return: list of login_nodes on which the offline job monitor is running.
    """
    remote.Connection.verbose = False
    login_nodes = remote.cluster_properties[remote.current_cluster]['login_nodes']
    result = []
    for login_node in range(1,len(login_nodes)):
        remote.set_connection(login_node=login_node)
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
                result.append(login_node)
                psid = line.split(' ',2)[1]
                if kill:
                    cmd = remote.RemoteCommand('kill '+psid)
                    try:
                        cmd.execute()
                        print(cmd.str())
                    except Exception as e:
                        print(type(e),e)
                        print('failed:',cmd.str())
        if found:
            print('ojm.py running on',login_nodes[login_node],'\n')
    return result 
    #---------------------------------------------------------------------------

#===============================================================================    
if __name__=='__main__':
    
    parser = argparse.ArgumentParser('is_ojm_running.py')
    parser.add_argument('-k','--kill',action='store_true')
    args = parser.parse_args()
    print('is_ojm_running.py: command line arguments:',args,'\n')

    is_ojm_running(args.kill)