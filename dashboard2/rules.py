"""
Classes and functions for defining and applying rules to filter ill-performing 
jobs in the showq output. 
"""
from cluster import cluster_properties, current_cluster
from cfg import Cfg
import cpus
#===============================================================================    
class Rule:
    """
    Base class for rules. 
    
    :param str warning: warning (or a format string for it). This warning will show up in the job overview and job details.
    :param int severity: rules with a *severity* of 0 do not contribute to the total number of warnings sofar (default=1)
    
    Derived classes must provide a check(self,job_sample) method that accepts a
    :class"`showq.JobSample` object and determines whether it satisfies the rule,
    in which case it returns the empty string (= no warning), or not and then it 
    returns a non-empty string with a warning.  
    """
    def __init__(self,warning='',severity=1):
        self.severity = severity
        self.warning = warning
    #---------------------------------------------------------------------------
    def check(self,job_sample):
        """
        This method **must** be reimplemented by derived classes. It must verify that
        the :class"`showq.JobSample` object *job_sample* satisfies the rule or not.
        
        :return: the empty string if the rule is satisfied, otherwise a warning.
        :rtype: str
        """
        assert False, 'Implementation error: {} does not reimplement :func:`Rule.check.'.format(type(self)) 
    #---------------------------------------------------------------------------

#===============================================================================    
class EfficiencyThresholdRule(Rule):
    """
    This rule checks that the efficiency of a :class:`JobSample` object is above 
    :class:`EfficiencyThresholdRule.effic_threshold`.
    """
    effic_threshold = Cfg.effic_threshold # percentage
    """ Efficiency threshold [%]: jobs with an efficiency below this threshold will be 
    reported. """
#---------------------------------------------------------------------------    
    def __init__(self):
        Rule.__init__(self,warning='!! Efficiency is too low') 
    #---------------------------------------------------------------------------
    def check(self,job_sample):
        """ Reimplementation of :func:`Rule.check`. """
        if job_sample.get_effic() >= EfficiencyThresholdRule.effic_threshold:
            return ''

        sar_effic = job_sample.data_qstat.sar()
        if sar_effic >= EfficiencyThresholdRule.effic_threshold:
            msg = '?? Efficiency: qstat->{:5.2f}%, sar->{:5.2f}%.'.format(job_sample.get_effic(),sar_effic) 
            return msg
        msg = self.warning+': {:5.2f}%. '.format(job_sample.get_effic())
        return msg
    #---------------------------------------------------------------------------

#===============================================================================    
class ResourcesWellUsedRule (Rule):
    """
    This rule checks that a :class:`JobSample` object is using the resources of the
    allocated nodes well enough. This is the case:
     
    - if all cores are in use by the job,
    - if not all cores are used by the job, but it is a single node job and there are other jobs on the node.
      (note that Hopper is configured to only allow this on single node jobs).
    - if not all cores are used by used, but it is a multi-node job. (In which case
      we thrust that the user has requested only part of the cores on a node to have 
      more memory per node). 
    - if not all cores are used by the job, but it is a single node job that requested or uses a reasonable fraction of the available memory on the node.  
    """
    minimum_memory_fraction = .80
    """ A single node job must request at least this fraction of the memory 
    available on the node to be considered as using its resources well""" 
    #---------------------------------------------------------------------------
    def __init__(self):
        Rule.__init__(self,warning='!! node is not fully used') 
    #---------------------------------------------------------------------------
    def check(self,job_sample):
        """ Reimplementation of :func:`Rule.check`. """
        # are all cores in use
        total_ncores_in_use = job_sample.get_ncores()
        nnodes = job_sample.parent_job.get_nnodes()
        total_ncores_available = nnodes*cluster_properties[current_cluster]['ncores_per_node'](nnodes) 
        all_cores_in_use = (total_ncores_in_use==total_ncores_available)
        if all_cores_in_use:
            return ''
        # Not all cares are in use
        if nnodes==1: # single node job
            # take neighbouring jobs in consideration:
            if job_sample.mhost_job_info.n>1: # there are other jobs on the same node.
                total_ncores_in_use = job_sample.mhost_job_info.ncores[-1]
                if total_ncores_in_use == total_ncores_available:
                    return ''
                # even after checking for other jobs on the node, not all cores are used
                # mayby the used cores need all the memory:
                mem_used_or_reqd = job_sample.mhost_job_info.memory[-1]
                mem_available    = cluster_properties[current_cluster]['mem_avail_gb'](job_sample.mhost_job_info.mhost)
                if mem_used_or_reqd >= ResourcesWellUsedRule.minimum_memory_fraction * mem_available:
                    return ''
        else: # multinode job
            #todo : as not all cores are in use, check that the job uses nearly all available memory. 
            mem_used_or_reqd = job_sample.get_mem()
            mem_available = cluster_properties[current_cluster]['mem_avail_gb'](job_sample.get_nodes())
            if mem_used_or_reqd >= ResourcesWellUsedRule.minimum_memory_fraction * mem_available:
                return ''
        # The rule is not satisfied
        return self.warning
    #---------------------------------------------------------------------------


#===============================================================================    
class UsingSwapSpaceRule(Rule):
    """
    Warn iff:
    
    * efficiency is below threshold
    * memory is nearly exhousted
    * more than 10% of the swap space is used.
    """
    maximum_fraction_swap = .10
    
    #---------------------------------------------------------------------------
    def __init__(self):
        Rule.__init__(self,warning='!! swap space used: {}/{} = {}%')
    #---------------------------------------------------------------------------
    def check(self,job_sample):
        """ Reimplementation of :func:`Rule.check`. """
        
        if job_sample.get_effic() >= EfficiencyThresholdRule.effic_threshold:
            return ''
        mem_available = cluster_properties[current_cluster]['mem_avail_gb'](job_sample.get_nodes())
        if job_sample.data_qstat.get_mem_used() < .90*mem_available:
            return ''
        s = 'Swap space used:'
        warn = False
        for node in job_sample.get_nodes():
            result = cpus.run_free(node)
            if result[2] >= UsingSwapSpaceRule.maximum_fraction_swap:
                warn = True
            s += '\n    {}: swap used:{:6.2f} available {:6.2f} = {:6.2f}%'.format(node,*result)
        if warn:
            return s
        else:
            return ''
    #---------------------------------------------------------------------------
    
#===============================================================================    
class TooManyWarningsRule(Rule):
    """
    Warn that there are many warnings: if there are more then 4 samples, and 25% or more of those have warnings
    """
    mininum_samples = 4
    maximum_warnings = .25 # fraction
    #---------------------------------------------------------------------------
    def __init__(self):
        Rule.__init__(self,warning='!! Too many warnings',severity=0)
    #---------------------------------------------------------------------------
    def check(self,job_sample):
        """ Reimplementation of :func:`Rule.check`. """
        nsamples = job_sample.parent_job.nsamples()
        if nsamples < TooManyWarningsRule.mininum_samples:
            return ''        
        nwarnings_sofar = 0
        for irule,count in enumerate(job_sample.parent_job.warning_counts):
            rule = the_rules[irule]
            if rule.severity>0:
                nwarnings_sofar += count
        if nwarnings_sofar/nsamples < self.maximum_warnings:
            return ''
        warning = '!! Many warnings ({} for {} samples).'.format(nwarnings_sofar,nsamples)
        return warning
    #---------------------------------------------------------------------------
        
#===============================================================================    
class NoModulesRule(Rule):
    """
    Verify the script loads some modules. If not it probably relies on system 
    compilers and libraries, which are obsolete. 
    """
    #---------------------------------------------------------------------------
    def __init__(self):
        Rule.__init__(self,warning = '!! No modules loaded') 
    #---------------------------------------------------------------------------
    def check(self,job_sample):
        """ Reimplementation of :func:`Rule.check`. """
        if job_sample.parent_job.jobscript is None:
            return ''
        if job_sample.parent_job.jobscript.loaded_modules():
            return ''
        return self.warning
    #---------------------------------------------------------------------------

#===============================================================================    
the_rules = [ EfficiencyThresholdRule()
            , ResourcesWellUsedRule()
            , UsingSwapSpaceRule()
            , TooManyWarningsRule()
            , NoModulesRule()
            ]
""" List of rules that will be verified in order of appearance. """  
#===============================================================================    

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
    