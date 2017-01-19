#===================================================================================================
class IgnoreSignals:
    """
    Context manager class for ignoring signals in QMainWindow instances 
    """
    #-----------------------------------------------------------------------------------------------
    def __init__(self, qwMainWindow):
        """
        Constructor.
                
        :parm expected_exceptions: expected exception type, or a tuple of expected exception types.
        """
        self.qwMainWindow = qwMainWindow
        # store original state
        self.ignore_signals = self.qwMainWindow.ignore_signals
        # modify the state
        self.qwMainWindow.ignore_signals = True
    #-----------------------------------------------------------------------------------------------
    def __enter__(self):
        pass
    #-----------------------------------------------------------------------------------------------
    def __exit__(self, exception_type, exception_value, tb):
        # reset original state
        self.qwMainWindow.ignore_signals = self.ignore_signals
    #-----------------------------------------------------------------------------------------------
