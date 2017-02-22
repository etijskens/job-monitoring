""" !!! Python script (v2.7) !!! 

This python script (v2.7) retrieves (username,mail_address) pairs from the ldap
and prints a single pair per line.
It it meant to be run on a login node.

Command line arguments:
    'password': password to access the ldap
    -n <N>    : print (username,mail_address) pairs for user name in the range vsc20001-vsc20NNN
                default value is 500
    --verbose : also print command line arguments (for debugging)
"""
import ldap
import argparse

parser = argparse.ArgumentParser('ldap_mail')
parser.add_argument('pwd'      ,action='store',default='' , type=str)
parser.add_argument('-n'       ,action='store',default=500, type=int)
parser.add_argument('--verbose',action='store_true')
args = parser.parse_args()
if args.verbose:
    print(args)

ldap_server = ldap.initialize('ldap://openldap.antwerpen.vsc')
username    = 'cn=mail,dc=vscentrum,dc=be'
try:
    ldap_server.protocol_version = ldap.VERSION3
    ldap_server.simple_bind_s(username,args.pwd)
    valid = True
except Exception, error:
    print error

for i in range(1,args.n+1):
    vsc20xxx = 'vsc20%03d' % i
    _filter = '(cn=%s)' % vsc20xxx
    try:
        result_id = ldap_server.search('dc=vscentrum,dc=be', ldap.SCOPE_SUBTREE,_filter, ['mail'])
    except:
        print 'oops'
        continue
    result_type, result_data = ldap_server.result(result_id, 0)
    try:
        mail_address = result_data[0][1]['mail'][0]
    except:
        continue
    print vsc20xxx,mail_address
