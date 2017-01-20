#==================================================================================================
def title_line(text='', width=80,char='*',start=4,above=False,below=False):
    line0 = width*char + '\n'
    if text:
        text = ' '+text+' '
    n =len(text)
    line = line0[:start-1]+text+line0[n+start-1:]
    if above:
        line = line0+line
    if below:
        line = line+line0
    return line
    #---------------------------------------------------------------------------    


#==================================================================================================
# test code below
#==================================================================================================
if __name__=='__main__':

    print(title_line('hello',width=100))
    print(title_line('hello',below=True,above=True,char='%'))
    print(title_line(width=100))
    
    print('--finished--')
