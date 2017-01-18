import remote

import pickle
from _collections import OrderedDict

#===================================================================================================
the_mail_adresses = None
#===================================================================================================
def address_of(username):
    global the_mail_adresses
    if not the_mail_adresses:
        the_mail_adresses = retrieve_mail_addresses()
    return the_mail_adresses[username]

#===================================================================================================
def retrieve_mail_addresses(refresh=False):
    """
    :return: dict {username:mail_address}
    """
    do_refresh = refresh
    if not do_refresh:
        try:
            mail_addresses = pickle.load( open('config/mail_addresses.pickled','rb') )
            print('Loaded config/mail_addresses.pickled')
        except :
            do_refresh = True
            
    if do_refresh:
        # first copy the script to my remote home directory, so i have the most recent version of it.
        python_script = 'vsc20xxx_mailaddresses.py'
        remote.copy_local_to_remote(python_script,python_script)
        command = pickle.load(open('config/retrieve_mail_addresses.pickled','rb'))
    #     command = "~/miniconda2/envs/jom2/bin/python vsc20xxx_mailaddresses.py 'ldap-mail-pwd'"  
    #     print(command)
        lines = remote.run_remote(command)
    #     print(lines)
        mail_addresses = OrderedDict()
        for line in lines:
            words = line.split()
            if words:
                mail_addresses[words[0]] = words[1]
        pickle.dump (mail_addresses, open('config/mail_addresses.pickled','wb') )
        print('Refreshed config/mail_addresses.pickled')
        
    return mail_addresses

#===================================================================================================
# test code below
#===================================================================================================
if __name__=='__main__':
    mail_adresses = retrieve_mail_addresses()
    print(mail_adresses)
    
    print(address_of('vsc20170'))
    
    print('\n--finished--')