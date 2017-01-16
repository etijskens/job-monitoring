from constants import cluster_properties, current_cluster
# from job_sample import Job

class Rule:
    def __init__(self,ignore_in_job_details=False,severity=1):
        self.ignore_in_job_details = ignore_in_job_details
        self.severity = severity
    
class EfficiencyThresholdRule(Rule):
    
    effic_threshold = 70 # percentage
    
    def __init__(self):
        Rule.__init__(self)
        
        self.warning = '!! Efficiency is too low' 
    
    def check(self,job_sample,isample=-1):
        effic = job_sample.effic
        if effic < EfficiencyThresholdRule.effic_threshold:
            msg = self.warning+': {}%. '.format(effic)
            
            return msg
        else:
            return ''
    

class CoresInUseRule(Rule):
    """
    ok if all cores used
       or not all cores used but more than one job_sample on the mhost node
       or not all cores used but more nodes used for the job_sample
       or not all cores used but nearly all memory requested
    """
        
    def __init__(self):
        Rule.__init__(self)
        self.warning = '!! node is not fully used' 
    
    def check(self,job_sample):
        if job_sample.all_cores_in_use:
            return ''
        
        if job_sample.parent_job.neighbouring_jobs:
            return ''
        
        nnodes = job_sample.data_qstat.get_nnodes()
        if  nnodes > 1:
            return ''
        
        mem_available = 58 #on the thin nodes at least
        mem_requested = job_sample.data_qstat.get_mem_requested()
        if mem_requested==0:
            mem_requested = job_sample.data_qstat.get_mem_used()
        if mem_requested > .8*mem_available:
            return ''
        
        return self.warning
            
class TooManyWarningsRule(Rule):
    """
    not ok if there are more then 4 samples, and 25% or more of those have warnings
    """
    mininum_samples = 4
    maximum_warnings = .25 # fraction
    
    def __init__(self):
        Rule.__init__(self,ignore_in_job_details=True,severity=0)
        self.warning = '!! Too many warnings'
         
    
    def check(self,job_sample):
        nsamples = job_sample.parent_job.nsamples()
        if nsamples < TooManyWarningsRule.mininum_samples:
            return ''
        
        nwarnings_sofar = 0
        for rule,count in job_sample.parent_job.warning_counts.items():
            if rule.severity>0:
                nwarnings_sofar += count
        if nwarnings_sofar/nsamples < self.maximum_warnings:
            return ''
        
        warning = '!! Many warnings ({} for {} samples).'.format(nwarnings_sofar,nsamples)
        return warning
        
class NoModulesRule(Rule):
    """
    not ok if the script has no modules
    """
    def __init__(self):
        Rule.__init__(self)
        self.warning = '!! No modules loaded' 

    def check(self,job_sample):
        if job_sample.parent_job.jobscript is None:
            return ''
        if job_sample.parent_job.jobscript.loaded_modules():
            return ''
        
        return self.warning

################################################################################
# test code below
################################################################################
if __name__=='__main__':
    
    class DummyJob:
        def __init__(self):
            self.effic = 60
        def message(self):
            return 'oops'
            
    job_sample = DummyJob()
            
    rule = EfficiencyThresholdRule()
    print(rule.check(job_sample))
    job_sample.effic = 80
    print('<<'+rule.check(job_sample)+'>>')
    
    print('--finished--')
    