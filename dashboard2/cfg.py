import rules

class Cfg: 
    """
    A "namespace" class for storing configuration parameters (as static variables).
    """
    # configuration parameters
    #   todo: document these static variables
    effic_threshold = 70        # a percentage
    sampling_interval= 1*60    # seconds
    correct_effic   = True 
    # When jobs create processes on other nodes which are
    #   NOT under the control of Torque, showq does not know the loads on these 
    #   other nodes and assumes they are zero. As a consequence, the average 
    #   cpu efficiency (EFFIC) reported is the sum of the loads on the cores of the 
    #   master host node, divided by the total number of cores requested and may
    #   overly pessimistic. E.g.a job runnin on all 20 cores of 5 nodes. with a
    #   an average cpu efficiency of 100% is reported as 20*100%/(20*5) = 20%.
    #   if Cfg.correct_effic==True the efficiency is replaced by the correct 
    #   efficiency on the moster node only.
    verbose = False
#     history = History()
    rules.EfficiencyThresholdRule.effic_threshold = effic_threshold
    the_rules = [ rules.EfficiencyThresholdRule()
                , rules.CoresInUseRule()
                , rules.TooManyWarningsRule()
                , rules.NoModulesRule()
                ]
        
