import remote
import mycollections
from showq import hhmmss2s
from cfg import Cfg
from cpus import ExecHost,str2gb

#===================================================================================================
class Data_tracejob:
    """
    Gather the tracejob output for having some overall statistics on completed jobs. 
    
    :param str jobid: (short) jobid of the completed job.

    Typical tracejob output::
    
        Job: 394893.hopper 
        02/03/2017 14:28:39  A    queue=batch
        02/03/2017 14:28:39  A    queue=q1h
        02/03/2017 14:35:18  A    user=vsc20399 group=vsc20399 jobname=1-2-3 queue=q1h ctime=1486128519 qtime=1486128519 etime=1486128519 start=1486128918 owner=vsc20399@ln01.hopper.antwerpen.vsc exec_host=r5c1cn08.hopper.antwerpen.vsc/0-19+r5c6cn05.hopper.antwerpen.vsc/0-19 Resource_List.naccesspolicy=singlejob Resource_List.neednodes=2:ppn=20:ib Resource_List.nodect=2 Resource_List.nodes=2:ppn=20:ib Resource_List.pmem=2800mb Resource_List.walltime=01:00:00 
        02/03/2017 14:37:42  A    user=vsc20399 group=vsc20399 jobname=1-2-3 queue=q1h ctime=1486128519 qtime=1486128519 etime=1486128519 start=1486128918 owner=vsc20399@ln01.hopper.antwerpen.vsc exec_host=r5c1cn08.hopper.antwerpen.vsc/0-19+r5c6cn05.hopper.antwerpen.vsc/0-19 Resource_List.naccesspolicy=singlejob Resource_List.neednodes=2:ppn=20:ib Resource_List.nodect=2 Resource_List.nodes=2:ppn=20:ib Resource_List.pmem=2800mb Resource_List.walltime=01:00:00 session=40592 total_execution_slots=40 unique_node_count=2 end=1486129062 Exit_status=0 resources_used.cput=2560 resources_used.energy_used=0 resources_used.mem=4024852kb resources_used.vmem=14251528kb resources_used.walltime=00:02:23
    
    """
    #---------------------------------------------------------------------------------------------------------         
    def __init__(self,jobid):
        self.jobid = jobid
        # use only accounting files
        command = 'ssh mn.hopper tracejob -slmq -n 10 {}'.format(jobid)
        self.lines = remote.run(command,post_processor=remote.list_of_non_empty_lines)
        if not self.lines:
            remote.err_print('Failed ',command)
            return
        # we only process the last line
        words = self.lines[-1].split()
        # for word in words:
        #     print(word)
        self.data = mycollections.OrderedDict()
        self.data['date'] = words[0]
        self.data['time'] = words[1]
        for word in words[3:]:
            try:
                key_val = word.split('=',1)
                key = key_val[0]
                val = key_val[1]
                self.data[key] = val
            except:
                pass
    #---------------------------------------------------------------------------------------------------------
    def get_effic(self):
        """
        :return: overall efficiency from tracejob output. 
        
        .. note:: This is also subject to the problem that moab currently is ignorant
                  about the slave nodes.
        """
        if not hasattr(self,'effic'):
            walltime = hhmmss2s(self.data['resources_used.walltime'])
            ncores   = int(self.data['total_execution_slots'])
            cpu_time = int(self.data['resources_used.cput'])
            self.effic = 100 * cpu_time / (walltime*ncores)
            if Cfg.correct_effic:
                node_cores = ExecHost(self.data['exec_host'])
                ncores_on_mhost = node_cores.ncores(cnode='mhost')
                self.effic *= ncores/ncores_on_mhost
        return self.effic 
    #---------------------------------------------------------------------------------------------------------
    def get_mem_used(self):
        """
        :return: overall memory use of the job from tracejob output.

        .. note:: This is also subject to the problem that moab currently is ignorant
                  about the slave nodes.
        """
        gb = str2gb(self.data['resources_used.mem'])
        return gb
    #---------------------------------------------------------------------------------------------------------         

#===================================================================================================
### test code below ================================================================================
#===================================================================================================
if __name__=='__main__':
    remote.connect_to_login_node()
    
    jobid = '394893'
    tj = Data_tracejob(jobid)
    print('effic =',tj.get_effic())
    print('memGB =',tj.get_mem_used())