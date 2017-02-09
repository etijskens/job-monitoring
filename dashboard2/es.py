"""
Module with Ascii terminal escape sequences.
"""
#===============================================================================    
class ES:
    """
    A "namespace" class with ascii terminal escape sequences.
    """
    bell =         '\033[\07h'
    clear_screen = '\033[2J'
    # modes
    bold    = '\033[1m'
    dim     = '\033[2m'
    blink   = '\033[5m'
    normal  = '\033[0m'
    reverse = '\033[7m'
    # colors
    black   = '\033[30m'
    red     = '\033[31m'
    green   = '\033[32m'
    blue    = '\033[34m'
    magenta = '\033[35m'
    white   = '\033[37m'
    default = '\033[39m'
    #---------------------------------------------------------------------------    
