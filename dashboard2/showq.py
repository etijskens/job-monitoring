from remote import run_remote
from cpus   import cpu_list
from script import Data_jobscript
from cfg import Cfg

from constants import cluster_properties, current_cluster

from qstatx import Data_qstat
from rules import EfficiencyThresholdRule

import datetime
from _collections import OrderedDict
from listdict import ListDict

# list of users we want to ignore for the time being...
ignore_users = []

#===============================================================================    
def run_showq():
    """
    Runs ``showq -r -p hopper --xml`` on a login node,
    parse its xml output, discard everything but the job entries 
    convert the job entries from a list of OrderedDicts to a list of ShowqJobEntries,
    remove the job entries whose mhost is unknown 
    remove worker job entries
    """
    data_showq = run_remote("showq -r -p hopper --xml" )
    job_entries = data_showq['Data']['queue']['job']
    # remove jobs
    #  . which have no mhost set
    #  . which have jobid like '390326[1]' (=worker jobs)
    result = []
    for job_entry in job_entries:
        converted = ShowqJobEntry(job_entry)
        
        # ignore jobs with unknow mhost
        try:
            converted.get_mhost()
        except KeyError:
            print('ignoring',converted.get_jobid_long(), '(mhost unknown)')
            continue
        
        # ignore jobs with jobid containing '[n]'
        jobid = converted.get_jobid()
        if '[' in jobid:
            print('ignoring',job_entry.get_jobid_long(), '(worker job)')
            continue
        
        result.append(converted)
        
    return result

#===============================================================================    
class ShowqJobEntry:
    #---------------------------------------------------------------------------    
    def __init__(self,job_entry):
        self.data = job_entry # OrderedDict
    #---------------------------------------------------------------------------    
    def get_jobid_long(self):
        jobid = self.data['@DRMJID']
        return jobid
    #---------------------------------------------------------------------------    
    def get_jobid(self):
        jobid = self.data['@JobID']
        return jobid
    #---------------------------------------------------------------------------    
    def get_state(self):
        state = self.data['@State']
        return state
    #---------------------------------------------------------------------------    
    def get_effic(self):
        """
        return uncorrected efficiciency
        """
        numerator   = self.data['@StatPSUtl']
        denominator = self.data['@StatPSDed']
        try:
            value = 100*float(numerator)/float(denominator) # [%]
        except ZeroDivisionError:
            value = -1
        value = round(value,2)
        return value
    #---------------------------------------------------------------------------    
    def get_username(self):
        value = self.data['@User']
        return value
    #---------------------------------------------------------------------------    
    def get_mhost(self,short=True):
        value = self.data['@MasterHost']
        if short:
            value = value.split('.',1)[0]
        return value    
    #---------------------------------------------------------------------------    
    def get_ncores(self):
        value = int( self.data['@ReqProcs'] )
        return value

#===============================================================================    
def overview_by_user(arg):
    """
    sort key for sorting warnings by username
    """
    return arg.split(' ',1)[1]
#===============================================================================
class JobSample:
    #---------------------------------------------------------------------------    
    def __init__(self,job_entry,job):
        assert isinstance(job_entry, ShowqJobEntry)
        assert isinstance(job, Job)
        self.showq_job_entry = job_entry
        self.parent_job      = job
        
        self.data_qstat      = Data_qstat( job.jobid )
        self.data_sar        = None
        # Compute whether all cores are in use
        total_ncores_in_use = job_entry.get_ncores()
        nnodes = self.data_qstat.get_nnodes()
        total_ncores_available = nnodes*cluster_properties[current_cluster]['ncores_per_node'] 
        self.all_cores_in_use = (total_ncores_in_use==total_ncores_available)

        self.effic = job_entry.get_effic() # uncorrected
        #   we make a copy of this because it may be corrected if torque does not
        #   know the efficiency on the subordinate nodes.
        if nnodes>1 and Cfg.correct_effic:
            #if there is only one node allocated there is no need for correcting
            ncores_mhost = len(cpu_list(self.data_qstat.node_cores[job_entry.get_mhost()]))   # number of cores used by this job on the master compute node
            # compute the efficiency on the master node only. 
            # Note that, if torque is not ignorant about the loads on the slave nodes, this 
            # correction is overly optimistic.
            corrected_effic = round(self.effic*total_ncores_in_use/ncores_mhost,2)
            # if it is ok, we assume that it is also ok on the slqve nodes (but we are not sure)
            self.effic = corrected_effic
        
        self.details = ''
        
   #---------------------------------------------------------------------------
    def check_for_issues(self):
        """
        returns True (False) if there are (aren't) issues 
        """
        self.warnings = []
        self.overview = ''
        self.details  = ''
        if not self.data_qstat.is_interactive_job(): #interactive jobs are ignored
            for rule in Cfg.the_rules:
                msg = rule.check(self)
                if msg:
                    self.warnings.append(msg)
                    self.parent_job.warning_counts[rule] += 1
        
        if self.warnings:
            self.parent_job.nsamples_with_warnings += 1
            if self.effic < EfficiencyThresholdRule.effic_threshold:
                self.data_qstat.sar()
            return True
        else:
            return False
    
    #---------------------------------------------------------------------------
    def compose_overview(self):
        """
        """
        if self.warnings:
            if self.overview:
                return self.overview
            description = self.description()
            self.overview = '\n'+( description.ljust(32)+self.warnings[0] ).ljust(68)+str(self.parent_job.jobscript.loaded_modules())
            spaces = '\n'+(32*' ')
            for w in self.warnings[1:]:
                self.overview += spaces+w             
        else:
            self.overview = ''
        return self.overview
    
    #---------------------------------------------------------------------------
    def description(self):
        desc = '{} {} {} {}|{}'.format( self.showq_job_entry.get_jobid()
                                      , self.showq_job_entry.get_username()
                                      , self.showq_job_entry.get_mhost()
                                      , self.data_qstat     .get_nnodes()
                                      , self.showq_job_entry.get_ncores()
                                      )
        return desc
    
    #---------------------------------------------------------------------------
    def compose_details(self):
        """
        """
        if self.details or not self.warnings:
            return self.details

        self.details = str(self.overview) # make a copy
        self.details += '\n\n#samples with warnings : {} / {} = {}%'.format( self.parent_job.nsamples_with_warnings
                                                                           , self.parent_job.nsamples()
                                                                           , round(100*self.parent_job.nsamples_with_warnings/self.parent_job.nsamples(),2)
                                                                           )
        for rule,count in self.parent_job.warning_counts.items():
            if not rule.ignore_in_job_details and count>0:
                self.details +='\n  {:25}: {:5}'.format(rule.warning,count)
                
        self.details += '\nwalltime used/remaining: {} / {}'.format( self.data_qstat.get_walltime_used()
                                                                   , self.data_qstat.get_walltime_remaining()
                                                                   )
        self.details += '\nmemory   used/requested: {} / {}'.format( round(self.data_qstat.get_mem_used()     ,3)
                                                                   , round(self.data_qstat.get_mem_requested(),3) )
        hdr = 'nodes and cores used: '
        nohdr = len(hdr)*' '
        nodes = self.data_qstat.get_exec_host().split('+')
        self.details += '\n'+hdr+nodes[0]
        for node in nodes[1:]:
            self.details += '\n'+nohdr+node
            
        self.details += '\nother jobs on {}: '.format(self.showq_job_entry.get_mhost())
        self.details += str(self.parent_job.neighbouring_jobs) 
        self.details += '\n'
        stars = 40*'*'
        if self.data_qstat.node_sar:
            self.details += '*** sar -P ALL 1 1 ***'+stars+'\n'
            for node, data_sar in self.data_qstat.node_sar.items():
                for line in data_sar.data_cores:
                    self.details += node+' '+line +'\n'
        self.details += '*** Script *****************'+stars+'\n'
        for line in self.parent_job.jobscript.clean:
            self.details += line+'\n'
        self.details += '****************************'+stars
            
        return self.details
        
#===============================================================================
class Job:
    #---------------------------------------------------------------------------    
    def __init__(self,job_entry,neighbouring_jobs,timestamp):
        """
        """
        assert isinstance(job_entry,ShowqJobEntry)
        self.jobid = job_entry.get_jobid()
        self.mhost = job_entry.get_mhost()
    
        self.neighbouring_jobs = neighbouring_jobs # list of jobs on the mhost        
        
        self.nsamples_with_warnings = 0
        self.warning_counts = {}
        for rule in Cfg.the_rules:
            self.warning_counts[rule] = 0
            
        self.samples = OrderedDict() #{datetime:JobSamnple object}
        self.first_timestamp = timestamp
        self.last_timestamp  = None
        self.jobscript       = None
        
        self.add_sample(job_entry,timestamp)
        
    #---------------------------------------------------------------------------    
    def add_sample(self,job_entry,timestamp):
        """
        """
        self.last_timestamp = timestamp
        self.samples[timestamp] = JobSample(job_entry,self)
     
    #---------------------------------------------------------------------------
    def timestamps(self):
        keys = list(self.samples.keys())
        return keys
    #---------------------------------------------------------------------------
    def index(self,timestamp):
        index = self.timestamps().index(timestamp)
        return index
    #---------------------------------------------------------------------------
    def nsamples(self):
        return len(self.samples)
    #---------------------------------------------------------------------------
    def is_finished(self,current_timestamp):
        tf = self.last_timestamp < current_timestamp
        return tf
    #---------------------------------------------------------------------------
    def check_for_issues(self,timestamp):
        sample = self.samples[timestamp] 
        if sample.check_for_issues():
            #there are issues
            if self.jobscript is None:
                self.jobscript = Data_jobscript(self.jobid,self.mhost)
            overview_line = sample.compose_overview()
        else:
            overview_line = ''
        return overview_line
    #---------------------------------------------------------------------------
    def get_details(self,timestamp):
        if not timestamp in self.samples:
            timestamp = self.timestamps()[-1]
        details = self.samples[timestamp].compose_details()
        return details
#===============================================================================   
timestamp_format = '%Y-%m-%d %H:%M' 
#===============================================================================   
class Sampler:
    #---------------------------------------------------------------------------    
    def __init__(self,interval=None,qMainWindow=None):
        if interval is None:
            self.sampling_interval = Cfg.sampling_interval
        else:
            self.sampling_interval = interval
        self.qMainWindow = qMainWindow
            
        self.overviews = OrderedDict()  # {timestamp:job_overview}
        self.jobs    = {}               # {jobid    :Job object  }
        self.timestamps = []         # [datetime.strftime(timestamp_format)]
        self.timestamp_jobs = ListDict()# {timestamp:[jobids]}
    
    #---------------------------------------------------------------------------    
    def sample(self,test__=False):
        """
        """
        job_entries = run_showq()
        self.n_entries   = len(job_entries)
        
        if self.qMainWindow:
            from PyQt4.QtGui import QProgressDialog,QApplication
            dlg = QProgressDialog('','',0, self.n_entries,self.qMainWindow)
            
        # create a dict { mhost : [jobid] } with all the jobs running on node mhost 
        self.mhost_jobs = ListDict()
        for job_entry in job_entries:
            mhost = job_entry.get_mhost()
            jobid = job_entry.get_jobid()
            self.mhost_jobs.add(mhost,jobid)

        timestamp = datetime.datetime.now().strftime(timestamp_format)

        progress_message_fmt = 'Sampling #{} : {} {}/{}'
        overview = [] # one warning per job with issues, jobs without issues are skipped
        if test__:
            n_ok = 0
            n_ok_stop = 1
            n_notok = 0 
            n_notok_stop = 2
            print('testing: n_ok_stop={}, n_notok_stop={}'.format(n_ok_stop,n_notok_stop)) 
        for i_entry,job_entry in enumerate(job_entries):
            if job_entry.get_state() != 'Running':
                continue # we only analyze running jobs
            jobid = job_entry.get_jobid()
            self.timestamp_jobs.add(timestamp,jobid)
                        
            progress_message = progress_message_fmt.format(len(self.timestamp_jobs),jobid,i_entry,self.n_entries)
            if self.qMainWindow is None:
                print(progress_message)
            else:
                dlg.setLabelText(progress_message)
                dlg.setValue(i_entry)
                QApplication.processEvents()
                
            if not jobid in self.jobs:
                # this job is encountered for the first time
                mhost = job_entry.get_mhost()
                neighbouring_jobs = list(self.mhost_jobs[mhost]) # make copy of the list 
                neighbouring_jobs.remove(jobid)
                job = Job(job_entry,neighbouring_jobs,timestamp)
                self.jobs[jobid] = job 
            else:
                job = self.jobs[jobid]
                job.add_sample(job_entry,timestamp)
                
            overview_line = job.check_for_issues(timestamp)
            if overview_line:
                overview.append(overview_line)
            
            if test__:
                if overview_line:
                    n_notok += 1
                else:
                    n_ok += 1
                if n_ok >= n_ok_stop and n_notok >= n_notok_stop:
                    break
        
        if self.qMainWindow is None:
            for line in overview:
                print(line,end='')
        else:
            dlg.setValue(self.n_entries)
            QApplication.processEvents()
        
        self.overviews[timestamp] = self.overview_list2str(overview)
        
        self.timestamps.append(timestamp)
        #    this must be the last statement because the gui otherwise sees a timestamp which is not ready.
        return timestamp
    
    #---------------------------------------------------------------------------
    def timestamp(self,i=-1):
        return self.timestamps[i]
    #---------------------------------------------------------------------------    
    def overview_list2str(self,overview_list,key=overview_by_user,reverse=True):
        overview_list.sort(key=key,reverse=reverse)
        n_jobs = self.n_entries
        n_warn = len(overview_list)
        text = 'Jobs running well: {}/{}, efficiency threshold = {}%'.format(n_jobs-n_warn,n_jobs,Cfg.effic_threshold) 
        text+= ''.join(overview_list) 
        return text
    #---------------------------------------------------------------------------
    def nsamples(self):
        return len(self.timestamps)
    #---------------------------------------------------------------------------    
    #---------------------------------------------------------------------------    
    
################################################################################
# test code below
################################################################################
if __name__=="__main__":
    test__ = True
    test__ = False
    sampler = Sampler()
    timestamp = sampler.sample(test__=test__)
    
    current_jobids = sampler.timestamp_jobs[timestamp]
    for jobid in current_jobids:
        job_sample = sampler.jobs[jobid].samples[timestamp]
        print( job_sample.compose_details(),end='')
        
    print('\n--finished--')
