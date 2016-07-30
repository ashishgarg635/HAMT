#!/usr/bin/python2

import commands

def cores():
    return commands.getoutput('lscpu | grep "CPU(s):"  | grep --invert-match "NUMA" | awk \'{print $2}\' ')

def clockspeed():
    return int( float( commands.getoutput('lscpu | grep "CPU MHz" | awk \'{ print $3 }\' ') ) )

def ram():
    return int( commands.getoutput('cat /proc/meminfo | grep "MemTotal" | awk \'{ print $2 }\'  ') )/1024

def diskspace():
    return commands.getoutput('df -hT | grep "/dev/mapper/rhel" | awk \'{ print $5 }\' ')

def getstats():
    return '/'.join([ cores(),str(clockspeed()),str( ram() ),diskspace() ])

print getstats()
