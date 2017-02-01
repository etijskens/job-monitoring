import remote

import pickle,datetime
from collections import OrderedDict

#===================================================================================================
the_mail_adresses = None
#===================================================================================================
def address_of(username):
    global the_mail_adresses
    if the_mail_adresses is None:
        the_mail_adresses = refresh_mail_addresses()
    else:
        age = datetime.datetime.now() - the_mail_adresses['last_refreshed_on']
        if age.days>0:
            the_mail_adresses = refresh_mail_addresses()
    if the_mail_adresses is None:
        # we could not refresh, try to read from file
        the_mail_adresses = load_mail_addresses()
    if the_mail_adresses is None:
        # we could not read from file 
        return 'mail addresses not available'
    try:
        address = the_mail_adresses[username]
    except KeyError:
        address = '(mail address unknown)'
    return address
    #-----------------------------------------------------------------------------------------------
    
#===================================================================================================
def print_all():
    for username,address in the_mail_adresses.items():
        print(username,address)
    #-----------------------------------------------------------------------------------------------
    
#===================================================================================================
def load_mail_addresses():
    try:
        mail_addresses = pickle.load( open('config/mail_addresses.pickled','rb') )
        print('Loaded config/mail_addresses.pickled')
        return mail_addresses
    except :
        return None
    #-----------------------------------------------------------------------------------------------
    
#===================================================================================================
def refresh_mail_addresses():
    # first copy the script to my remote home directory, so i have the most recent version of it.
    python_script = 'vsc20xxx_mailaddresses.py'
    try:
        remote.copy_local_to_remote(python_script,python_script)
        command = pickle.load(open('config/retrieve_mail_addresses.pickled','rb'))
        lines = remote.run(command,post_processor=remote.list_of_lines)
    except:
        lines = None
    if lines is None:
        return None
    mail_addresses = OrderedDict()
    mail_addresses['last_refreshed_on'] = datetime.datetime.now()
    for line in lines:
        words = line.split()
        if words:
            mail_addresses[words[0]] = words[1]
    pickle.dump(mail_addresses, open('config/mail_addresses.pickled','wb') )
    print('Refreshed config/mail_addresses.pickled')
    return mail_addresses
    #-----------------------------------------------------------------------------------------------
    
#===================================================================================================
# test code below
#===================================================================================================
if __name__=='__main__':
#     mail_adresses = loa_mail_addresses()
#     print(mail_adresses)
    
    print(address_of('vsc20170'))
    print(address_of('vsc20170'))
    print_all()
    print('\n--finished--')