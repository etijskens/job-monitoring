
import remote
from es import ES

_test = False

#===============================================================================
class Data_jobscript:
    """
    Class for storing and manipulating a job's job script
    
    :param str jobid: the job's jobid
    :param str compute_node: name of the mhost node of the job (moab keeps a copy of the job script on the mhost node).
    """
    #---------------------------------------------------------------------------    
    def __init__(self,jobid,compute_node):
        self.compute_node = compute_node # must be mhost
        self.jobid = jobid
        self.modules = None                    
        try:
            command = "ssh {} 'sudo cat /opt/moab/spool/torque/mom_priv/jobs/{}.hopper.SC'".format(compute_node,jobid)
            self.data = remote.run(command,post_processor=remote.list_of_lines,attempts=1,raise_exception=True)            
        except Exception as e:
            print(type(e),e)
            s = str(e)
            self.data = None
        if self.data is None:
            self.data = ['# Jobscript not found by command: "{}"'.format(command)
                        ,'# job may be completed already.'
                        ]
            self.clean = self.data
            return
            
        # remove comment lines, empty lines
        self.clean = []
        for line in self.data:
            stripped = line.strip()
            if stripped: # not empty
                if stripped[0]=='#':
                    if stripped[1:4]=='PBS':
                        self.clean.append(stripped) 
                else:
                    self.clean.append(line)
    #---------------------------------------------------------------------------    
    def __str__(self):
        """
        :return: a short - essential - version of the script. It is decorated using ascii escape characters.
        """
        s = ''
        for line in self.clean:
            if '#PBS' in line and '-l' in line:
                s += ES.bold+line+ES.normal
            elif 'module'in line and ('load' in line or 'add' in line): 
                s += ES.bold+ES.green+line+ES.default+ES.normal
            else:
                s += ES.dim +line+ES.normal
            s+='\n'
        return s[:-1]
    #---------------------------------------------------------------------------    
    def loaded_modules(self,short=False):
        """
        :return: a list of loaded modules.
        :param bool short: if *True*, modules like 'hopper/2014a' are omitted and version information is omitted too.
        
        The short version is for having a quick view of the program(s) that might be run in the job.
        """
        if not hasattr(self,'modules'):
            self.modules = None
        if self.modules is None:
            self.modules = []
            for line in self.clean:
                words = line.split()
                if len(words)>2:
                    if words[0]=='module' and words[1] in ['load','add']:
                        self.modules.append(words[2])
        if not short:
            return self.modules    
        else:
            modules = list(self.modules) # make a copy
            for m in reversed(modules):
                # remove 'directory' modules
                if m.startswith('hopper'):
                    modules.remove(m)
                    continue
                # keep only the name of the module and not the version info
                if '/' in m:
                    i = modules.index(m)
                    modules[i] = m.split('/',1)[0]
            return modules
    #---------------------------------------------------------------------------    
    def isempty(self):
        """
        Test for empty script.
        """
        return len(self.clean)==0
    #---------------------------------------------------------------------------    
    
#===============================================================================
#== test code below ============================================================
#===============================================================================
if __name__=="__main__":
    _test = True
    
    js = Data_jobscript('r3c1cn01','384038')
    print(js.data)
    print(js)
    print(js.clean)
    
    print('\n--finished--')
        