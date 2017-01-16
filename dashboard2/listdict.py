from _collections import OrderedDict

#===============================================================================
class ListDict(OrderedDict):
    """
    an OrderedDict of lists.
    """
    #---------------------------------------------------------------------------    
    def __init__(self):
        OrderedDict.__init__(self)        
    #---------------------------------------------------------------------------    
    def add(self,key,item):
        """
        Add <item> to <key>'s list. If <key> is new it is created.
        Note that
            some_ListDict[key] = value 
        removes key's original value
        """
        if key in self:
            self[key].append(item)
        else:
            self[key] = [item]
    #---------------------------------------------------------------------------    
