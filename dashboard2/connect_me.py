import remote 

#===============================================================================
def connect():
    assert False # remove this line
    # modify this line to connect with your authentication details
    return remote.Connection('your_user_name','path_to_your_ssh_key_file','passphrase_to_unlock_your_ssh_key_if_necessary')
    #---------------------------------------------------------------------------    

################################################################################
# test code below
################################################################################
if __name__=="__main__":
    connect()
    assert not remote.the_connection is None
    assert remote.the_connection.is_connected()
        
    print('\n--finished--')
