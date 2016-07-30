#!/usr/bin/python


import commands
import re


def gethostip():
    status,hostip = commands.getstatusoutput('hostname -i')
    if status:
        print 'Could not get the hostname. Check "hostname -i " command in the terminal'
        return -1,None
    else:
        return 0,hostip


def gethostinterfaces():
    status,interfaces = commands.getstatusoutput(' netstat -i  | grep --invert-match  face  |  awk \' { print $1 } \' ')
    if status:
        print 'Unable to get the interfaces of the host. Check "netstat -i" command in the terminal'
        return -1,None
    else:
        return 0,interfaces.split('\n')




def getnetmask(hostip):
    status,maskline = commands.getstatusoutput('ip addr | grep ' + hostip)
    if status:
        print 'Unable to get the netmask. Check " ip addr " command in the terminal'
        return -1,None

    ipwithnetmask = re.findall(
        '([0-2]?[0-9]?[0-9]\.[0-2]?[0-9]?[0-9]\.[0-2]?[0-9]?[0-9]\.[0-2]?[0-9]?[0-9]\/[0-3]?[0-9])', maskline)
    mask = ipwithnetmask[0].lstrip(hostip)
    mask = mask.lstrip('/')
    return 0, mask




def getnodesip(hostip,mask):
    status,output = commands.getstatusoutput('nmap --version')
    if status:
        print 'Could not find the nmap package'
        print 'Installing nmap ............'
        exitcode,output = commands.getstatusoutput('yum install nmap -y')
        if exitcode :
            print 'Unable to install the nmap package'
            print 'Install nmap manually'
            return 1,None
        else:
            print 'Package Nmap successfully installed.'

    pos = hostip.rfind('.')
    ip = hostip[:pos+1] + '0'
    exitcode,output = commands.getstatusoutput('nmap -sP ' + ip + '/' + mask )
    #print output
    exitcode,output = commands.getstatusoutput('echo -e \"  ' + output +' \" | grep "Nmap scan " ')
    #print output
    if exitcode:
         print 'Unable to fetch the IP of nodes available in the network'
         print 'Try to \" nmap \" tool manually o test if it is working.'
         return -1,None

    else:
         ips = re.findall('([0-2]?[0-9]?[0-9]\.[0-2]?[0-9]?[0-9]\.[0-2]?[0-9]?[0-9]\.[0-2]?[0-9]?[0-9])',output)
         #ips.remove(hostip)
         ips.remove('192.168.122.1')
         return 0,ips
