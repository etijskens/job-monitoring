import paramiko,subprocess    
import xmltodict
import shlex,sys
from time import sleep

import logindetails
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
    return s
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
    the_connection = Connection( *logindetails.me )
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
def xml_to_odict(s):
    """
    post processor function for xml output of a command using xmltodict
    :returns: an OrderedDict object
    """
    return xmltodict.parse(s)
#===============================================================================
def list_of_lines(s):
    """
    post processor function. splits the output of a command in a list of lines
    (newlines are removed).
    :returns: a list of lines (str)
    """
    return s.split('\n')
#===============================================================================

#===============================================================================
class CommandBase:
    """
    Base class for Command and LocalCommand
    """
    last_error_messages = ''
    #---------------------------------------------------------------------------
    def __init__(self):
        """"""
        self.sout = None # command output on stdout
        self.serr = None # command output on stderr
    #---------------------------------------------------------------------------
    def maximum_wait_time(self,attempts=6,wait=60):
        return ( 2**(attempts-1) -1 )*wait
    #---------------------------------------------------------------------------
    def execute_repeat(self,attempts=6,wait=60,post_processor=None):
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
        CommandBase.last_error_messages = ''
        while attempts_left:
            try:
                result = self.execute(post_processor)
                if slept_time:
                    CommandBase.last_error_messages \
                        += err_print('Attempt {}/{} succeeded after {} seconds.'.format(attempts-attempts_left+1,attempts,slept_time))
                return result
            except Exception as e:
                attempts_left -= 1
                CommandBase.last_error_messages += err_print('Attempt {}/{} failed.'.format(attempts-attempts_left,attempts))
                CommandBase.last_error_messages += err_print(type(e),e)
                CommandBase.last_error_messages += err_print('Retrying after',sleep_time,'seconds.')
                sleep(wait)
                slept_time += sleep_time 
                sleep_time *=2
        else:
            assert attempts_left==0
            CommandBase.last_error_messages += err_print('Exhausted after {} attempts.'.format(attempts))
            return None
        
        assert False # should never happen
    #---------------------------------------------------------------------------
    def str(self):
        if isinstance(self.command,list):
            return ' '.join(self.command)
        else:
            return self.command
    #---------------------------------------------------------------------------
            
#===============================================================================
class Command(CommandBase):
    """
    command that is executed using subproces.Popen.
    """
    def __init__(self,command):
        super(Command,self).__init__()
        if isinstance(command,str):
            self.command = shlex.split(command)
        elif isinstance(command,list):
            self.command = command
        else:
            raise TypeError('Expecting "str" or "list'".")
        if 'ssh' in self.command:
            self.timeout = 60
        else:
            self.timeout = 5
    #---------------------------------------------------------------------------
    def execute(self,post_processor=None):
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
        if post_processor:
            return post_processor(self.sout)
        else:
            return self.sout
    #---------------------------------------------------------------------------

#===============================================================================    
class RemoteCommand(CommandBase):
    """
    Command that is executed remotely (on a login-node) using paramiko.client.
    """                
    def __init__(self,command):
        super(RemoteCommand,self).__init__()
        self.command = command
        is_Connection = isinstance(the_connection,Connection)
        if not is_Connection or ( is_Connection and not the_connection.is_connected() ):
            raise Exception("not connected")
    #---------------------------------------------------------------------------
    def execute(self,post_processor=None):
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
        if post_processor:
            return post_processor(self.sout)
        else:
            return self.sout
    #---------------------------------------------------------------------------
    
#===============================================================================    
def run(command, connection=the_connection,attempts=6,wait=60,post_processor=None):
    """
    rather dirty wrapper around the Command
    """
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
        return cmd.execute_repeat(attempts=attempts,wait=wait,post_processor=post_processor)
    except Exception as e:
        return None
    #---------------------------------------------------------------------------

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
    lines = cmd.execute(post_processor=list_of_lines)
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
def copy_remote_to_local(local_path,remote_path, connection=the_connection,rename=False):
    """
    Copy remote file <remote_path> to <local_path>, and optionally renames the source (to
    prevent it from being considered again in the next round, e.g.). As the remote host is 
    a linux machine the rename operation is actually a 'mv', hence, <rename> may have the 
    same filename but a different directory path, in wich case the source is effectively 
    to a different directory. 
     
    :rename: non-empty str: after copying the original file, it is renamed to <rename>
             empty str    : after copying the original file, it is removed.
             False        : after copying the original file, it is left as it was.
    """
    sftp = connection.paramiko_client.open_sftp()
    sftp.get(remote_path,local_path)
    sftp.close()
    if isinstance(rename,str):
        if rename:
            command = 'mv {} {}_done'.format(remote_path,remote_path)
        else:
            command = 'rm -f '+remote_path
        cmd = RemoteCommand(command)
        cmd.execute()
    else:
        if not (isinstance(rename,bool) and rename==False):
            raise ValueError("kwarg 'rename' must be str or False, got {}.".format(rename))
#===============================================================================

#===============================================================================
# test code below
#===============================================================================
if __name__=='__main__':
    err_print('one:','two',3,[4,5])
    cmd = Command('ls')
    result = cmd.execute()
    print(result)
    result = cmd.execute(post_processor=list_of_lines)
    print(cmd.str(),result)

    cmd = Command('cat test')
    try:
        result = cmd.execute()
    except Exception as e:
        err_print(e)
    result = cmd.execute_repeat(attempts=6, wait=1,post_processor=list_of_lines) 
    print(result)
    
    cmd = RemoteCommand('cd data/jobmonitor/ ; ls')
    result = cmd.execute()
    print(cmd.str()+'\n',result)
    
    print('\n--finished--')