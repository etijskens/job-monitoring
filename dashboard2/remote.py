import paramiko,subprocess    
import xmltodict
import shlex,sys
from time import sleep

import connect
from cfg import Cfg
from constants import current_cluster,cluster_properties

#===============================================================================
def err_print(*args):
    s = '!!!'
    for arg in args:
        s+=' '
        s+=str(arg)
    s+='\n'
    sys.stderr.write(s)
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
        except:
            err_print('Failed to connect',username)            
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
#     the_connection = 'off-line'
    the_connection = Connection( *connect.me )
#===============================================================================

#===============================================================================
class NotConnected(Exception):
    """
    This exception is raised if the no paramiko connection could be established.
    """
    pass
#===============================================================================
class Stderr(Exception):
    """
    This exception is raised if the command produced output on stderr.
    """
    pass
#===============================================================================

#===============================================================================
class CommandBase:
    """
    Base class for Command and LocalCommand
    """
    def __init__(self,command):
        """"""
        self.command = command
        self.sout = None
        self.serr = None
    #---------------------------------------------------------------------------
    def post_process(self,xml=False):
        """"""
        if xml:
            return xmltodict.parse(self.sout)
        else:
            return self.sout.split('\n')
    #---------------------------------------------------------------------------
    def maximum_wait_time(self,attempts=6,wait=60):
        return ( 2**(attempts-1) -1 )*wait
    #---------------------------------------------------------------------------
    def execute_repeat(self,attempts=6,wait=60,post_process=True,xml=False):
        """
        If the command fails, retry it after <wait> seconds. The total number 
        of attempts is <attempts>. After every attempt, the wait time is doubled.
        0,1,2,4, 8,16
        0,1,3,7,15,31 
        The maximum wait time is ( 2**(attempts-1) -1 )*wait.
        The default retries times, waiting at most 31 minutes. (This does not 
        include time the command is executing).
        """
        attempts_left = attempts
        sleep_time = wait
        slept_time = 0
        while attempts_left:
            try:
                result = self.execute(post_process,xml)
                if slept_time:
                    err_print('Attempt {}/{} succeeded after {} seconds.'.format(attempts-attempts_left+    1,attempts,slept_time))
                return result
            except Exception as e:
                attempts_left -= 1
                err_print('Attempt {}/{} failed.'.format(attempts-attempts_left,attempts))
                err_print(e)
                err_print('Retrying after',sleep_time,'seconds.')
                sleep(wait)
                slept_time += sleep_time 
                sleep_time *=2
        else:
            assert attempts_left==0
            err_print('Exhausted after {} attempts.'.format(attempts))
            return None
        assert False # should never happen
    #---------------------------------------------------------------------------
            
#===============================================================================
class Command(CommandBase):
    """
    command that is executed using subproces.Popen.
    """
    def __init__(self,command):
        if isinstance(command,str):
            self.command = shlex.split(command)
        elif isinstance(command,list):
            self.command = command
        else:
            raise TypeError('Expecting "str" or "list'".")
        super(Command,self).__init__(command)
        if 'ssh' in self.command:
            self.timeout = 60
        else:
            self.timeout = 5
    #---------------------------------------------------------------------------
    def str(self):
        return ' '.join(self.command)
    #---------------------------------------------------------------------------
    def execute(self,post_process=True,xml=False):
        """
        raises 
            subprocess.TimeoutExpired if the command does not complete in time
            Stderr if the command produces output on stderr
            an exception if anything goes wrong
        """
        proc = subprocess.Popen(self.command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.sout, self.serr = proc.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired as e:
#             err_print('Command', ' '.join(self.command), 'timed out after',self.timeout,'seconds.')
            proc.kill()
            self.sout, self.serr = proc.communicate()
            raise e
        if not self.sout:
            self.serr = self.serr.decode('utf-8')
            if self.serr:
                raise Stderr(self.serr)
        else:
            self.sout = self.sout.decode('utf-8')
        if post_process:
            return self.post_process(xml)
    #---------------------------------------------------------------------------

#===============================================================================    
class RemoteCommand(CommandBase):
    """
    Command that is executed remotely (on a login-node) using paramiko.client.
    """                
    def __init__(self,command):
        super(RemoteCommand,self).__init__(command)
        is_Connection = isinstance(the_connection,Connection)
        if not is_Connection or ( is_Connection and not the_connection.is_connected() ):
            raise Exception("not connected")
    #---------------------------------------------------------------------------
    def str(self):
        return self.command
    #---------------------------------------------------------------------------
    def execute(self,post_process=True,xml=False):
        """
        raises 
            subprocess.TimeoutExpired if the command does not complete in time
            Stderr if the command produces output on stderr
            an exception if anything goes wrong
        """
        tpl = the_connection.paramiko_client.exec_command(self.command)
        
        self.sout = tpl[1].read().decode('utf-8')
        if not self.sout:
            self.serr = tpl[2].read().decode('utf-8')
            if self.serr:
                raise Stderr(self.serr)
        if post_process:
            return self.post_process(xml)
    #---------------------------------------------------------------------------

#===============================================================================    
def run_remote(command, connection=the_connection,attempts=6,wait=60):
    """
    rather dirty wrapper around the Command
    """
    is_xml = ('qstat -x' in command) or ('--xml' in command)

    if Cfg.offline:
        # we are running on a login node, so we can execute the command using 
        # subprocess.Popen
        Cmd = Command
    else:
        # we are running on local machine and must use a paramiko client to 
        # execute the command on a login node.
        Cmd = RemoteCommand
    
    cmd = Cmd(command)
    try:
        return cmd.execute_repeat(attempts=attempts,wait=wait,post_process=True,xml=is_xml)
    except:
        return None
    #---------------------------------------------------------------------------

#===============================================================================    
def run_remote0( command, connection=the_connection ):
    is_xml = ('qstat -x' in command) or ('--xml' in command)

    if Cfg.offline:
        # we are running the offline job monitor on a login node.
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
def glob(pattern,path=None):
    """
    a remote glob
    """
    if path:
        command ='cd {} ; ls -1 {}'.format(path,pattern)
    else:
        command ='ls -1 {}'.format(pattern)
        
    cmd = RemoteCommand(command)
    lines = cmd.execute()
    # remove trailing empty lines
    while not lines[-1]:
        lines.pop()
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

#===============================================================================
# test code below
#===============================================================================
if __name__=='__main__':
    err_print('one:','two',3,[4,5])
    cmd = Command('ls')
    result = cmd.execute()
    print(result)
    result = cmd.execute(post_process=False)
    print(cmd.str(),cmd.sout)

    cmd = Command('cat test')
    try:
        result = cmd.execute()
    except Exception as e:
        err_print(e)
    result = cmd.execute_repeat(attempts=6, wait=1) 
    print(result)
    
    cmd = RemoteCommand('cd data/jobmonitor/ ; ls')
    result = cmd.execute()
    print(cmd.str()+'\n',result)
    
    print('\n--finished--')