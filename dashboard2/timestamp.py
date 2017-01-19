import datetime
#===============================================================================   
timestamp_format = '%Y.%m.%d.%Hh%M' 
#===============================================================================   
def get_timestamp():
    timestamp = datetime.datetime.now().strftime(timestamp_format)
    return timestamp
#===============================================================================   
# test code below
#===============================================================================   
if __name__=='__main__':
    print( get_timestamp() )
    
    print('\n--finished--')