#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
import sys
from datetime import datetime

#==================================================================================================
# Print iterations progress
#==================================================================================================
def printProgress(iteration, total, prefix = 'Progress: ', suffix = '', decimals = 1, barLength = 50):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int), if negative shows iteration/total rather 
        barLength   - Optional  : character length of bar (Int)
    """
    if iteration==0:
        print()
    if decimals<0:
        percents        = '{}/{}'.format(iteration,total)
        filledLength    = int(round(barLength * iteration / float(total)))
        bar             = '█' * filledLength + '-' * (barLength - filledLength)
        sys.stdout.write('\r{} |{}| {} {}'.format(prefix, bar, percents, suffix)),
    else:
        fmt             = "{0:." + str(decimals) + "f}"
        percents        = fmt.format(100 * (iteration / float(total)))
        filledLength    = int(round(barLength * iteration / float(total)))
        bar             = '█' * filledLength + '-' * (barLength - filledLength)
        sys.stdout.write('\r{} |{}| {}% {}'.format(prefix, bar, percents, suffix)),
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()
    #---------------------------------------------------------------------------

#==================================================================================================
# test code below
#==================================================================================================
if __name__=='__main__':

    from time import sleep
    
    # make a list
    items = list(range(0, 79))
    i     = 0
    l     = len(items)
    suffix = ''
    # Initial call to print 0% progress
    printProgress(i, l, prefix = 'printProgress:', suffix = suffix, barLength = 50, decimals=-1)
    for item in items:
        # Do stuff...
        sleep(0.01)
        # Update Progress Bar
        i += 1
        suffix = str(datetime.now())
        printProgress(i, l, prefix = 'printProgress:', suffix = suffix, barLength = 50, decimals=-1)

    print('\n\ndone')
