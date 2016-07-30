#!/usr/bin/python2

import ipgrab
from os import system

def resolve(passkey):
    _,hostip = ipgrab.gethostip()
    _,netmask = ipgrab.getnetmask(hostip)
    _,nodesips = ipgrab.getnodesip(hostip, netmask)

    system('rm -rf   /root/.ssh/id_rsa   /root/.ssh/id_rsa.pub ')
    system('ssh-keygen -N ""  -f  /root/.ssh/id_rsa')
    for ip in nodesips:
        system('sshpass -p  '+passkey+'  ssh-copy-id  -o StrictHostKeyChecking=no  '+ip)



