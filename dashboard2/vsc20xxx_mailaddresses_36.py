""" !!! Python script (v#.6) !!! 

This python script retrieves (username,mail_address) pairs from the ldap
and prints a single pair per line.
It is meant to be run on a login node.

Command line arguments:
    'password': password to access the ldap
    -n <N>    : print (username,mail_address) pairs for user name in the range vsc20001-vsc20NNN
                default value is 500
    --verbose : also print command line arguments (for debugging)
"""
import ldap3
import argparse

parser = argparse.ArgumentParser('ldap_mail')
parser.add_argument('pwd'      ,action='store',default='' , type=str)
parser.add_argument('-n'       ,action='store',default=500, type=int)
parser.add_argument('--verbose',action='store_true')
args = parser.parse_args()
if args.verbose:
    print(args)
try:
    srvr = ldap3.Server('ldap://openldap.antwerpen.vsc')
    ldap3_cnct = ldap3.Connection( server=srvr
                                 , user='cn=mail,dc=vscentrum,dc=be'
                                 , password=args.pwd
                                 )
    ldap3_cnct.bind()
except Exception as e:
#     print(e) # no idea why bind() raises "list index is out of range"
    pass
for i in range(1,args.n+1):
    vsc20xxx = 'vsc20%03d' % i
    try:
        result_id = ldap3_cnct.search( search_base='dc=vscentrum,dc=be'
                                     , search_scope=ldap3.SUBTREE
                                     , search_filter=('(cn=%s)' % vsc20xxx)
                                     , attributes=['mail']
                                     )
    except Exception as e:
        print(vsc20xxx,e)
    for entry in ldap3_cnct.response:
        mail = entry['attributes']['mail']
        if mail:
            print(vsc20xxx,mail[0])
