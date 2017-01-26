import remote
from cpus       import cpu_list
from script     import Data_jobscript
from cfg        import Cfg
from constants  import cluster_properties, current_cluster
from qstatx     import Data_qstat
from rules      import EfficiencyThresholdRule
from timestamp  import get_timestamp
from titleline  import title_line
from mycollections import OrderedDict,od_add_list_item

import pickle,os,shutil
from time       import sleep

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
    data_showq = remote.run_remote("showq -r -p hopper --xml" )
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
                                      , self.get_nnodes()
                                      , self.get_ncores()
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
        if self.data_qstat.node_sar:
            self.details += title_line('sar -P ALL 1 1',width=100)
            for node, data_sar in self.data_qstat.node_sar.items():
                for line in data_sar.data_cores:
                    self.details += node+' '+line +'\n'
        self.details += title_line('Script',width=100) 
        for line in self.parent_job.jobscript.clean:
            self.details += line+'\n'
        self.details += title_line(width=100)
            
        return self.details
    #---------------------------------------------------------------------------        
    
    def cputime_walltime_ratio(self,scale_with_ncores=True):
        """
        should be close to one for well-performing jobs.
        """
        walltime = hhmmss2s( self.data_qstat.data['resources_used']['walltime'] )
        cputime  = hhmmss2s( self.data_qstat.data['resources_used']['cput'] )
        ncores   = self.get_ncores()
        ratio = cputime/(ncores*walltime)
        if ratio > 1:
            if ratio > 1.05:
                print('WARNING: cputime_walltime_ratio > 1.0 :',ratio)
            ratio = 1.0
        return ratio
    #---------------------------------------------------------------------------
    def cputime_walltime_ratio_as_str(self,scale_with_ncores=True):
        """
        should be close to one for well-performing jobs.
        """
        try:
            ratio = self.cputime_walltime_ratio()
            s = '{:4.2f}'.format(ratio)
        except KeyError:
            s = '?.??'
        return s
    #---------------------------------------------------------------------------
    def get_ncores(self):        
        return self.showq_job_entry.get_ncores()
    #---------------------------------------------------------------------------        
    def get_nnodes(self):        
        return self.data_qstat.get_nnodes()
    #---------------------------------------------------------------------------        
        
#===============================================================================
def hhmmss2s(hhmmss):
    words = hhmmss.split(':')
    assert len(words)==3
    seconds = int(words[2]) + 60*( int(words[1]) + 60*int(words[0]) )
    return seconds 
#-------------------------------------------------------------------------------        
#===============================================================================
class Job:
    #---------------------------------------------------------------------------    
    def __init__(self,job_entry,neighbouring_jobs,timestamp):
        """
        """
        assert isinstance(job_entry,ShowqJobEntry)
        self.jobid    = job_entry.get_jobid()
        self.username = job_entry.get_username()
        self.mhost    = job_entry.get_mhost()
        self.address  = None
    
        self.neighbouring_jobs = neighbouring_jobs # list of jobs on the mhost        
        
        self.nsamples_with_warnings = 0
        self.warning_counts = {}
        for rule in Cfg.the_rules:
            self.warning_counts[rule] = 0
            
        self.samples = OrderedDict() #{timestamp:JobSamnple object}
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
    #---------------------------------------------------------------------------
    def remove_file(self):
        fname = 'running/{}_{}.pickled'.format(self.username,self.jobid)
        try:
            os.remove(fname)
        except:
            print('failed to remove',fname)
    #---------------------------------------------------------------------------
    def pickle(self,prefix,only_if_warnings=True):
        if (only_if_warnings and self.nsamples_with_warnings) \
        or (not only_if_warnings): 
            if prefix=='running':
                fname = '{}/{}_{}.pickled'   .format(prefix,self.username,self.jobid)
            else:
                fname = '{}/{}_{}_{}.pickled'.format(prefix,self.username,self.jobid,self.timestamps()[-1])
            with open(fname,'wb') as f:
                pickle.dump(self,f)
            print(' (pickled {})'.format(fname))
    #---------------------------------------------------------------------------
#===============================================================================   
def unpickle(prefix,username,jobid,timestamp=''):
    """
    This is the counterpart of Job.pickle()
    :returns: the Job object that was pickled, or None if inexisting.
    """
    if prefix=='running':
        fname = '{}/{}_{}.pickled'   .format(prefix,username,jobid)
    else:
        fname = '{}/{}_{}_{}.pickled'.format(prefix,username,jobid,timestamp)
    if os.path.exists(fname):
        job = pickle.load( open(fname,'rb') )
        print(' (unpickled {})'.format(fname))
    else:
        job = None
    return job
    
#===============================================================================   
class Sampler:
    #---------------------------------------------------------------------------    
    def __init__(self,interval=None,qMainWindow=None):
        if interval is None:
            self.sampling_interval = Cfg.sampling_interval
        else:
            self.sampling_interval = interval
        self.qMainWindow = qMainWindow
            
        self.overviews = OrderedDict()      # {timestamp:job_overview}
        self.jobs    = {}                   # {jobid    :Job object  }
        self.timestamps = []                # [datetime.strftime(timestamp_format)]
        self.timestamp_jobs = OrderedDict() # {timestamp:[jobids]}
        self.jobids_running_previous = []
    #---------------------------------------------------------------------------    
    def sample(self,verbose=False,test__=False):
        """
        """
        # get relevan part of showq output
        job_entries = run_showq()
        self.n_entries   = len(job_entries)
        
        if self.qMainWindow:
            from PyQt4.QtGui import QProgressDialog,QApplication
            dlg = QProgressDialog('','',0, self.n_entries,self.qMainWindow)
        else:
            from progress import printProgress
            hdr = 'sampling #{}'.format(len(self.timestamp_jobs)+1)
            
        # create a dict { mhost : [jobid] } with all the jobs running on node mhost 
        # and a list wit all uncompleted jobids
        # the latter is compared to the jobid list of the previous sample to find
        # out which jobs are finished.
        self.mhost_jobs = OrderedDict()
        self.jobids_running = []
        for job_entry in job_entries:
            mhost = job_entry.get_mhost()
            jobid = job_entry.get_jobid()
            od_add_list_item(self.mhost_jobs,mhost,jobid)
            self.jobids_running.append(jobid)
            try:
                self.jobids_running_previous.remove(jobid)
            except ValueError:
                pass
            #   when this loop has completed, self.jobids_running_previous 
            #   contains only jobides of finished jobs.
        jobids_finished = self.jobids_running_previous
        self.jobids_running_previous = self.jobids_running # prepare for next sample() call
        # pickle finished jobs (if they had issues) and remove them from self.jobs
        os.makedirs('completed', exist_ok=True)
        for jobid in jobids_finished:
            try:
                job = self.jobs.pop(jobid)
            except KeyError:
                continue
            job.pickle('completed')
            if Cfg.offline:
                job.remove_file()
        timestamp = get_timestamp()
        if Cfg.offline:
            os.makedirs ('running',exist_ok=True)
            if os.path.exists('running/timestamp'):
                os.remove('running/timestamp') 
            #   if ths file is absent ojm is sampling. 
            print(title_line(timestamp, width=100, above=True, below=True),end='')
            
        # loop over the running jobs (job_entries) 
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
            jobid    = job_entry.get_jobid()
            username = job_entry.get_username()
            od_add_list_item(self.timestamp_jobs,timestamp,jobid)
                        
            if self.qMainWindow is None:
                printProgress(i_entry, self.n_entries, prefix=hdr, suffix='jobid='+jobid, decimals=-1)
            else:
                progress_message = 'Sampling #{} : {} {}/{}'.format(len(self.timestamp_jobs),jobid,i_entry,self.n_entries)
                dlg.setLabelText(progress_message)
                dlg.setValue(i_entry)
                QApplication.processEvents()
                
            if not jobid in self.jobs:
                # this job is encountered for the first time
                # or this is a restart and the job was pickled 
                job =  unpickle('running', username, jobid)
                if job is None:
                    # this job is really encountered for the first time
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
                if verbose:
                    print('\n'+timestamp+'\n')
                    print(job.get_details(timestamp))
                if Cfg.offline:
                    job.pickle('running')
            
            if test__:
                if overview_line:
                    n_notok += 1
                else:
                    n_ok += 1
                if n_ok >= n_ok_stop and n_notok >= n_notok_stop:
                    break
        
        if Cfg.offline:
            # notify that sampling has finished.. 
            with open('running/timestamp','w') as f:
                f.write(timestamp)

        if self.qMainWindow is None:
            printProgress(self.n_entries, self.n_entries, prefix=hdr, suffix='', decimals=-1)
            for line in overview:
                print(line,end='')
            print()
        else:
            dlg.setValue(self.n_entries)
            QApplication.processEvents()
        
        self.overviews[timestamp] = self.overview_list2str(overview)
        
        self.timestamps.append(timestamp)
        #    this must be the last statement because the gui otherwise sees a timestamp which is not ready.
        return timestamp
    #---------------------------------------------------------------------------
    def get_remote_timestamp(self):
        """
        returns the last sample's timestamp. If ojm.py is in the process of sampling
        returns an empty string. 
        """
        try:
            lines = remote.run_remote('cd data/jobmonitor/running/; cat timestamp')
            return lines[0]
        except:
            return ''
    #---------------------------------------------------------------------------
    def sample_offline(self):
        """
        Check remote directory '~/data/jobmonitor/running for data on running jobs.
        Copy them to the local directory ./offline/running
        """
        shutil.rmtree('offline/running')
        os.makedirs('offline/running'  ,exist_ok=True)
        #os.makedirs('offline/completed',exist_ok=True)
        timestamp = self.get_remote_timestamp()
        while not timestamp:
            sleep(60)
            timestamp = self.get_remote_timestamp()
        if self.timestamps:
            if timestamp==self.timestamps[-1]:
                return # this timestamp is already in the samples
        self.timestamps.append(timestamp)
        filenames = remote.glob('*.pickled','data/jobmonitor/running/')
        self.n_entries = 0
        for filename in filenames:
            if not filename: # empty line
                continue
            lfname =         'offline/running/'+filename
            rfname = 'data/jobmonitor/running/'+filename
            print('copying '+rfname,'to',lfname,end='')
            try:
                remote.copy_remote_to_local(lfname,rfname)
                print(' - copied')
            except:
                print(' - failed')
                continue
            job = pickle.load(open('offline/running/'+filename,'rb'))
            self.add_offline_job(job)
            self.n_entries += 1
        self.overviews[timestamp] = self.overview_list2str(self.overviews[timestamp])
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
    def add_offline_job(self,job):
        """
        Add an offline monitored job to the sampler
        
        self.jobs    = {}                # {jobid    :Job object  }
        self.timestamps = []             # [timestamp]
        self.timestamp_jobs = ListDict() # {timestamp:[jobids]}
        self.overviews = OrderedDict()   # {timestamp:job_overview}
        """
        self.jobs[job.jobid] = job
        for timestamp,job_sample in job.samples.items():
            od_add_list_item(self.timestamp_jobs,timestamp,job.jobid)
            overview_line = job_sample.compose_overview()
            if not timestamp in self.overviews:
                self.overviews[timestamp] = [overview_line]
            else:
                overview = self.overviews[timestamp] 
                if isinstance(overview,str):
                    lines = overview.split('\n')
                    self.overviews[timestamp] = []
                    overview = self.overviews[timestamp]
                    for line in lines:
                        if not line.endswith('\n'):
                            line += '\n'
                        overview.append(line)
                overview.append(overview_line)                
    #---------------------------------------------------------------------------
    def when_done_adding_offline_jobs(self):
        self.timestamps.sort()
        for jobid_list in self.timestamp_jobs.values():
            jobid_list.sort()
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
