#==================================================================================================
def title_line(text='', width=80,char='*',start=4,above=False,below=False):
    """
    Function for printing title lines.
    
    :param str text: the title text
    :param int width: width of the title line
    :param str char: character used for the line
    :param int start: start point of the title text
    :param bool above: print a line above or not
    :param bool above: print a line below or not
    
    Some examples::
    
        >>> print(titleline.title_line("A title", 30))
        *** A title ******************
        >>> print(titleline.title_line("A title",30,start=8 ,char='-',above=True,below=True))
        ------------------------------
        ------- A title --------------
        ------------------------------    
        >>> print(titleline.title_line(width=30))
        ******************************
    """
    w = int(width/len(char))
    line0 = w*char + '\n'
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
    print(title_line('hello',below=True,above=True,char='= '))
    print(title_line(width=100))
    
    print('--finished--')
