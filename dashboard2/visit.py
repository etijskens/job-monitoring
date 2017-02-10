from _pickle import load

#===================================================================================================
def visit(filepath):
    """
    Visit a job monitor report in an interactive python session to access the original data in the report.
    
    :param str filepath: file path of .pickled file to visit. Path is absolute or relative to path of job-monitoring/dashboard2.
    :return: showq.Job object 
    """
    file = open(filepath,'rb')
    unpickled = load(file)
    return unpickled
    
#===================================================================================================
#== script =========================================================================================
#===================================================================================================
if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser('visit')
    parser.add_argument('filepath'      ,action='store',default='' , type=str)
    args = parser.parse_args()
    print(args)
    
    job = visit(args.filepath)
    
    print('\n--finished--')