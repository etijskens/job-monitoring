from cfg import Cfg
if Cfg.offline:
    import subprocess
else:
    import paramiko
    try:
        import connect_me
    except:
        print('connect_me.py not found.')
    
import xmltodict
from constants import current_cluster,cluster_properties

#===============================================================================    
class Connection:
    """
    Class for managing a paramiko (ssh) connection to hopper
    """
    #---------------------------------------------------------------------------    
    def __init__(self, username, ssh_key_filename, passphrase=None):
        """
        Open a connection
        """
        self.paramiko_client = None
        
        try:
            self.paramiko_client = paramiko.client.SSHClient()
            self.paramiko_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if passphrase:
                self.paramiko_client.connect( hostname     = cluster_properties[current_cluster]['login_node']
                                            , username     = username
                                            , key_filename = ssh_key_filename
                                            , passphrase   = passphrase
                                            )
            else:
                self.paramiko_client.connect( hostname     = cluster_properties[current_cluster]['login_node']
                                            , username     = username
                                            , key_filename = ssh_key_filename
                                            )
        except :
            print("failed to connected")            
            print(self.paramiko_client)
            self.paramiko_client = None
    #---------------------------------------------------------------------------    
    def is_connected(self):
        """
        Test if the connection succeeded.
        
        :rtype: bool.
        """
        return not self.paramiko_client is None
    #---------------------------------------------------------------------------    

#===============================================================================    
if Cfg.offline:
    the_connection = 'off-line'
else:
    the_connection = connect_me.connect()
#===============================================================================    
def run_remote( command, connection=the_connection ):
    is_xml = ('qstat -x' in command) or ('--xml' in command)

    if Cfg.offline:
        # we are running the offline job monitor on a login node.
        import shlex
        command = shlex.split(command)
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        if 'ssh' in command:
            timeout = 60
        else:
            timeout = 5
        try:
            sout, serr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            print('expired')
            proc.kill()
            sout, serr = proc.communicate()
        if not sout:
            serr = serr.decode('utf-8')
            if serr:
                print('!!!',serr)
                print('!!!',' '.join(command))
                raise Exception(serr)
        else:
            sout = sout.decode('utf-8')
    else:
        if not connection.is_connected():
            raise Exception("not connected")
        
        tpl = connection.paramiko_client.exec_command(command)
            
        sout = tpl[1].read().decode('utf-8')
        if not sout:
            serr = tpl[2].read().decode('utf-8')
            if serr:
                print('!!!',serr)
                print('!!!',command)
                raise Exception(serr)

    if is_xml:
        xml_dict = xmltodict.parse(sout) 
        return xml_dict
    else:
        lines = sout.split('\n')
        return lines
#===============================================================================
def copy_local_to_remote(local_path,remote_path, connection=the_connection):
    sftp = connection.paramiko_client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()
#===============================================================================
def copy_remote_to_local(local_path,remote_path, connection=the_connection):
    sftp = connection.paramiko_client.open_sftp()
    sftp.get(remote_path,local_path)
    sftp.close()
#===============================================================================
 