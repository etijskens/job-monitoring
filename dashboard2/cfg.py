"""
Module for configuring the application.

Class Cfg
=========
"""
#===============================================================================
class Cfg: 
    """
    A "namespace" class for storing configuration parameters (as static variables).
    """
    effic_threshold = 70
    """ Efficiency threshold [%]: jobs with an efficiency below this threshold will be 
    reported. """
    
    sampling_interval = 15*60
    """ Sampling interval [s]: every this many seconds the output of ``showq -r -p 
    hopper --xml`` is examined and jobs below *effic_threshold* are reported """
    
    correct_effic = True 
    """ If *True* scale the EFFIC value reported by Torque to the masternode only.    
    
    When jobs create processes on other nodes which are
    NOT under the control of Torque, showq does not know the loads on these 
    other nodes and assumes they are zero. As a consequence, the average 
    cpu efficiency (EFFIC) reported is the sum of the loads on the cores of the 
    master host node, divided by the total number of cores requested and may
    overly pessimistic. E.g.a job runnin on all 20 cores of 5 nodes. with a
    an average cpu efficiency of 100% is reported as 20*100%/(20*5) = 20%.
    if ``Cfg.correct_effic==True`` the efficiency is replaced by the correct 
    efficiency on the moster node only. If this is above *effic_threshold* it is 
    hoped/assumed that the other nodes are also performing wel and the job is not 
    reported. 
    
    Hopefully, when cpusets are working well in the future, Torque will report 
    the EFFIC value correctly, and this scaling will not be necessary anymore.
    """
    
    verbose = False

    offline = False
    """ if True we are running the offline job monitor on a login node (offline 
    job monitoring), otherwise we are running on a local machine. This is auto-
    matically set.
    """
    #---------------------------------------------------------------------------
