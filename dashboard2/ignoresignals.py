"""
Helper classes for gui scripts.

Classes and functions
=====================

"""
#===================================================================================================
class IgnoreSignals:
    """
    Context manager class for ignoring signals in QMainWindow instances
    
    :param qwMainWindow: the QMainWindow in which signals have to be ignored.
    """
    #-----------------------------------------------------------------------------------------------
    def __init__(self, qwMainWindow):
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
