
import remote
from constants import ES

_test = False

#===============================================================================
class Data_jobscript:
    """
    Class for storing and manipulating a job's job script
    
    :param str jobid: the job's jobid
    :param str compute_node: name of the mhost node of the job.
    """
    #---------------------------------------------------------------------------    
    def __init__(self,jobid,compute_node):
        self.compute_node = compute_node # must be mhost
        self.jobid = jobid
        command = "ssh {} 'sudo cat /opt/moab/spool/torque/mom_priv/jobs/{}.hopper.SC'".format(compute_node,jobid)
        if _test:
            self.data = [ '#!/bin/bash'
                        , ''
                        , ' # "name" of the job (optional)'
                        , ' #PBS -N BLACK-dftd3'
                        , ''
                        , ' # requested running time (required!)'
                        , ' #PBS -l walltime=72:00:00'
                        , ''
                        , ' # specification (required!)'
                        , ' #   nodes=   number of nodes; 1 for serial; 1 or more for parallel'
                        , ' #   ppn=     number of processors per node; 1 for serial; up to 8'
                        , ' #   if you want your "private" node: ppn=8'
                        , ' #   mem=     memory required'
                        , ''
                        , '# for hopper'
                        , '#PBS -l nodes=2:ppn=20'
                        , ''
                        , '== if, in addition, you want to avoid that other (yours) jobs of you also could'
                        , '== use this node, also add'
                        , '# #PBS -l naccesspolicy=singlejob'
                        , '#PBS -W x=nmatchpolicy:exactnode'
                        , ''
                        , ' # send mail notification (optional)'
                        , ' #   a        when job is aborted'
                        , ' #   b        when job begins'
                        , ' #   e        when job ends'
                        , ' #   M        your e-mail address (should always be specified)'
                        , ' #PBS -m e'
                        , ' #PBS -M deniz.cakir@uantwerpen.be'
                        , ''
                        , '# go to the (current) working directory (optional, if this is the'
                        , '# directory where you submitted the job)'
                        , '#cd $PBS_O_WORKDIR'
                        , ''
                        , 'cd /scratch/antwerpen/201/vsc20164/blackP-rotated/dft-d3/perp-pressure/e-field/0.01 '
                        , ''
                        , ''
                        , '# purge modules'
                        , 'module purge'
                        , ''
                        , '# for hopper:'
                        , 'module load VASP/5.3.5-intel-2014a'
                        , ''
                        , 'mpirun vasp > log-20'
                        , ''
                        ]
        else:
            try:
                self.data = remote.run(command,post_processor=remote.list_of_lines,attempts=1,raise_exception=True)
            except Exception as e:
                print(type(e),e)
                s = str(e)
                if 'No such file or directory' in s:
                    self.data = ['# '+s
                                ,'# job may be completed already.'
                                ]
                    self.clean = self.data
                return
            
        if len(self.data)==1 and not self.data[0]:
            self.data[0] = '# Jobscript not found by command: "{}"'.format(command)
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
        self.modules = None                    
    #---------------------------------------------------------------------------    
    def __str__(self):
        """
        Return a short - essential - version of the script.
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
        Test empty script.
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
        