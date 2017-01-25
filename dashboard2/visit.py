from _pickle import load

if __name__=='__main__':
    filename = 'vsc20133_392978_2017.01.23.15h06.pickled'
    file = open('offline/completed/'+filename,'rb')
    unpickled = load(file)
    
    print('\n--finished--')