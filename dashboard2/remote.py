"""
Classes and functions for handling commands to be executed on a login node, either
while one is logged in, or remotely from my laptop.

Classes and functions
=====================

"""
import paramiko,subprocess    
import xmltodict
import shlex,sys
from time import sleep

import logindetails
from cfg import Cfg
from constants import current_cluster,cluster_properties

#===============================================================================
def err_print(*args):
    """
    Utility for printing to stderr, behaves more or less as built-in print().
    """
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
    Class for managing a paramiko (ssh) connection to hopper:
    
    :param str username:
    :param str ssh_key_filename:
    :param str passphrase: optional possphrase to unlock ssh key.
    :param str cluster: optional, default is 'hopper'
    :param int login_node: optional, default is 0 
    """
    verbose = True
    the_connection = None
    #---------------------------------------------------------------------------    
    def __init__( self
                , username, ssh_key_filename, passphrase=None
                , cluster=current_cluster, login_node=0 ):
        """
        Open a connection
        """
        self.paramiko_client = None
        host = cluster_properties[cluster]['login_nodes'][login_node]
        try:
            self.paramiko_client = paramiko.client.SSHClient()
            self.paramiko_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if passphrase:
                self.paramiko_client.connect( hostname     = host
                                            , username     = username
                                            , key_filename = ssh_key_filename
                                            , passphrase   = passphrase
                                            )
            else:
                self.paramiko_client.connect( hostname     = host
                                            , username     = username
                                            , key_filename = ssh_key_filename
                                            )
            if Connection.verbose:
                print('Successfully connected {} to {}.'.format(username,host))
        except:
            err_print('Failed to connect {} to {}.'.format(username,host))            
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
def connect_to_login_node(cluster=current_cluster,login_node=0):
    """
    Make a new paramiko connection that will be used further on in this job
    monitoring session.
    """
    Connection.the_connection = Connection( *logindetails.me, cluster=cluster, login_node=login_node )
#===============================================================================    
# if Cfg.offline:
#     the_connection = 'off-line'
# else:
#     connect_to_login_node()
#===============================================================================

#===============================================================================
class NotConnected(Exception):
    """
    This exception is raised if no paramiko connection could be established.
    """
    pass
#===============================================================================
class Stderr(Exception):
    """
    This exception is raised if a CommandBase object produces output on stderr.
    """
    pass
#===============================================================================
def xml_to_odict(s):
    """
    A post-processor function that parses the xml output of a command into an 
    OrderedDict using :func:`xmltodict.parse`.
    
    :rtype: OrderedDict
    """
    return xmltodict.parse(s)
#===============================================================================
def list_of_lines(s):
    """
    A post-processor function that splits the output of a command in a list of lines
    (newlines are removed).
    
    :rtype: a list of lines (str)
    """
    return s.split('\n')
#===============================================================================
def list_of_non_empty_lines(s):
    """
    A post-processor function that splits the output of a command in a list of non-empty lines
    (newlines and empty lines are removed).
    
    :rtype: a list of non-empty lines (str)
    """
    lines = list_of_lines(s)
    nonempty_lines = []
    for line in lines:
        if line:
            nonempty_lines.append(line)
    return nonempty_lines
#===============================================================================

#===============================================================================
class CommandBase:
    """
    Base class for Command and LocalCommand. Derived classes typically (re)implement
    CommandBase.__init__() and execute(self,post_processor=None)
    """
    last_error_messages = str()
    """ This class member stores the error messages that accumulated during the last call to
    :func:`CommandBase.execute_repeat`.
    """
    #---------------------------------------------------------------------------
    def __init__(self):
        self.sout = None # command output on stdout
        self.serr = None # command output on stderr
    #---------------------------------------------------------------------------
    def maximum_wait_time(self,attempts=6,wait=60):
        """
        Compute the maximum wait time before the command gives up.
        """
        return ( 2**(attempts-1) -1 )*wait
    #---------------------------------------------------------------------------
    def execute_repeat(self,attempts=6,wait=60,post_processor=None):
        """
        Repeated execution after failure.
        
        :param int attempts: number of times the command is retried on failure, before it gives up. 
        :param int wait: seconds of wait time after the first failure, doubled on every failure.
        :param post_processor: a function the transforms the output (on stdout) of the command.
        
        :return: on success the output (on stdout) of the command as processed by *post_processor*, otherwise *None*
          
        If the command fails, retry it after <wait> seconds. The total number 
        of attempts is <attempts>. After every attempt, the wait time is doubled.
        
        =============== === === === === ==== ====
        attempt          1   2   3    4    5   6  
        wait time        0   1   2   4    8   16
        total wait time  0   1   3   7   15   31 
        =============== === === === === ==== ====
        
        The maximum wait time is ( 2**(attempts-1) -1 )*wait and can be obtained by
        method :func:`CommandBase.maximum_wait_time`.
        
        The default retries times, waiting at most 31 minutes. (This does not 
        include the time the command is being executed).
        
        If the repeated excution of the command fails, the accumulated error messages 
        are found in the class variable :class:`CommandBase.last_error_messages`. 
        
        This command is inherited by derived classes.
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
    def to_str(self):
        """
        Convert the command to a str and return it.
        """
        if isinstance(self.command,list):
            return ' '.join(self.command)
        else:
            return self.command
    #---------------------------------------------------------------------------
            
#===============================================================================
class Command(CommandBase):
    """
    System command that is executed using subproces.Popen. So, it is run on the machine
    where this python session runs.
    
    :param str command: the command as you would type it on a terminal. 
    """
    def __init__(self,command):
        super(Command,self).__init__()
        if isinstance(command,str):
            self.command = shlex.split(command)
#         elif isinstance(command,list):
#             self.command = command
        else:
            raise TypeError('Expecting "str" for command.')
        if 'ssh' in self.command:
            self.timeout = 10
        else:
            self.timeout = 5
    #---------------------------------------------------------------------------
    def execute(self,post_processor=None):
        """
        Execute the command.
        
        :param post_processor: a function the transforms the output (on stdout) of the command.
        
        :return: on success the output (on stdout) of the command as processed by *post_processor*, otherwise *None*
        
        This may raise one of these Exceptions:
         
        - *subprocess.TimeoutExpired* if the command does not complete in time,
        - *Stderr* if the command produces output on stderr,
        - an exception if anything goes wrong while trying to execute the command.
        """
        proc = subprocess.Popen(self.command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.sout, self.serr = proc.communicate(timeout=self.timeout)
        except subprocess.TimeoutExpired as e:
            err_print('Command', ' '.join(self.command), 'timed out after',self.timeout,'seconds.')
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
        is_Connection = isinstance(Connection.the_connection,Connection)
        if not is_Connection or ( is_Connection and not Connection.the_connection.is_connected() ):
            raise NotConnected()
    #---------------------------------------------------------------------------
    def execute(self,post_processor=None):
        """
        Execute the command.
        
        :param int attempts: number of times the command is retried on failure, before it gives up. 
        :param int wait: seconds of wait time after the first failure, doubled on every failure.
        :param post_processor: a function the transforms the output (on stdout) of the command.
        
        :return: on success the output (on stdout) of the command as processed by *post_processor*, otherwise an exception is raised and no result is returned.

        This may raise one of these Exceptions:
         
        - *Stderr* if the command produces output on stderr,
        - an exception if anything goes wrong while trying to execute the command.
        """
        tpl = Connection.the_connection.paramiko_client.exec_command(self.command)
        
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
def run(command,attempts=6,wait=60,post_processor=None,raise_exception=False):
    """
    Wrapper function around Command and RemoteCommand. If Cfg.offline is True, we
    are running on a login node, and the *commmand* string is executed in a :class:`Command`
    object, otherwise it is executed as a :class:`RemoteCommand` object.
    
    :param str command: the command as you would type it on a terminal.
    :param int attempts: number of times the command is retried on failure, before it gives up. 
    :param int wait: seconds of wait time after the first failure, doubled on every failure.
    :param post_processor: a function the transforms the output (on stdout) of the command.
    :param bool raise_exception: if True and ``attempts==1`` and the command fails, its exception is reraised.
    
    :return: on success the output (on stdout) of the command as processed by *post_processor*, otherwise *None*
      
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
        if attempts==1 and raise_exception:
            return cmd.execute(post_processor=post_processor) #may raise an exception
        else:
            return cmd.execute_repeat(attempts=attempts,wait=wait,post_processor=post_processor)
    except Exception as e:
        err_print(type(e),e)
        return None
    #---------------------------------------------------------------------------

#===============================================================================    
def glob(pattern,path=None):
    """
    A remote glob.
    
    :param str pattern: filename pattern to be matched, accepts linux wild cards.
    :param str path: path to the directory whose files are examined. If empty, glob looks in the remote home directory.  
    :return: a list of all filenames that match *pattern* in directory *path*.
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
def copy_local_to_remote(local_source,remote_destination):
    """
    Copy a locacl file to a remote file.
    
    :param str local_source: path to the local file.
    :param str remote_destination: path to remote file (filename must be included). 
    """
    sftp = Connection.the_connection.paramiko_client.open_sftp()
    sftp.put(local_source, remote_destination)
    sftp.close()
#===============================================================================
def copy_remote_to_local(local_destination,remote_source,rename=False):
    """
    Copy remote file <remote_source> to local file <local_destination>. Optionally 
    renames the source (typically to mark the file as copied). The parameter *rename* can be:
    
    - a non-empty :class:`str`: then the original file is renamed to *rename* after copying,
    - the empty :class:`str,`: then the original file is removed after copying, or
    - *False* : the original file is not renamed or removed..
    
    As the remote host is a linux machine the rename operation is actually a 'mv', hence, 
    *rename* may have the same filename but a different directory path, in wich case the 
    source is effectively to a different directory. 
    """
    sftp = Connection.the_connection.paramiko_client.open_sftp()
    sftp.get(remote_source,local_destination)
    sftp.close()
    if isinstance(rename,str):
        if rename:
            command = 'mv {} {}'.format(remote_source,rename)
        else:
            command = 'rm -f '+rename
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
    print(cmd.to_str(),result)

    cmd = Command('cat test')
    try:
        result = cmd.execute()
    except Exception as e:
        err_print(e)
    result = cmd.execute_repeat(attempts=6, wait=1,post_processor=list_of_lines) 
    print(result)
    
    connect_to_login_node()
    cmd = RemoteCommand('cd data/jobmonitor/ ; ls')
    result = cmd.execute()
    print(cmd.to_str()+'\n',result)
    
    print('\n--finished--')