import paramiko
import xmltodict
from constants import current_cluster,cluster_properties
try:
    import connect_me
except:
    print('connect_me.py not found.')

class Connection:
    """
    Class for managing a paramiko (ssh) connection to hopper
    """
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
            
    def is_connected(self):
        """
        Test if the connection succeeded.
        
        :rtype: bool.
        """
        return not self.paramiko_client is None

the_connection = connect_me.connect()

def run_remote( command, connection=the_connection ):
    if not connection.is_connected():
        raise Exception("not connected")
    
    if 'qstat --xml' in command :
        commandx = command.replace('--xml','-x')
        tpl = connection.paramiko_client.exec_command(commandx)
    else:
        tpl = connection.paramiko_client.exec_command(command)
        
    sout = tpl[1].read().decode('utf-8')
    if not sout:
        serr = tpl[2].read().decode('utf-8')
        if serr:
            print('!!!',serr)
            raise Exception(serr)
    
    
    if '--xml' in command:
        xml_dict = xmltodict.parse(sout) 
        return xml_dict
    else:
        lines = sout.split('\n')
        return lines

def copy_local_to_remote(local_path,remote_path, connection=the_connection):
    sftp = connection.paramiko_client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()
    