"""
utility functions for OrderedDict objects
"""
from _collections import OrderedDict

#===============================================================================
def od_add_list_item(ordered_dict,key,item):
    """
    Assumes that ordered_dict is an OrderedDict with lists as values.
    This function adds <item> to <key>'s list. If <key> is new it is created.
    Note that
        some_ListDict[key] = value 
    removes key's original value (as usual)
    """
    assert isinstance(ordered_dict,OrderedDict)
    if key in ordered_dict:
        ordered_dict[key].append(item)
    else:
        ordered_dict[key] = [item]
#===============================================================================
def od_last(ordered_dict):
    """
    Get the last (key,value) pair of an OrderedDict object.
    (apparently, this is the most efficient version, because OrderedDict is 
    implemented as a double linked list) 
    """
    assert isinstance(ordered_dict,OrderedDict)
    k=next(reversed(ordered_dict))
    return (k,ordered_dict[k])
#===============================================================================
