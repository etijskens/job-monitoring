import remote 

#===============================================================================
def connect():
    # modify this line to connect with your authentication details
    # the_connection = Connection('your_user_name','path_to_your_ssh_key_file','passphrase_to_unlock_your_ssh_key_if_necessary')
    return remote.Connection('vsc20170','/Users/etijskens/.ssh/id_rsa_npw')
    #---------------------------------------------------------------------------    

################################################################################
# test code below
################################################################################
if __name__=="__main__":
    connect()
    assert not remote.the_connection is None
    assert remote.the_connection.is_connected()
        
    print('\n--finished--')
