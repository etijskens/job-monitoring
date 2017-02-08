import datetime
#===============================================================================   
timestamp_format = '%Y.%m.%d.%Hh%M' 
#===============================================================================   
def get_timestamp():
    """
    :return: a timestamp based on the current time.
    :rtype: str
    """
    timestamp = datetime.datetime.now().strftime(timestamp_format)
    return timestamp
#===============================================================================   
# test code below
#===============================================================================   
if __name__=='__main__':
    print( get_timestamp() )
    
    print('\n--finished--')