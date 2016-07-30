#!/usr/bin/python

from xmlhandling import xmlparsing
import sys
import commands
import os
#import cleanInstall

def setMapredFilev1(jbip,jbport):
    file = xmlparsing('/etc/hadoop/mapred-site.xml')
    file.appendProperty('mapred.job.tracker',jbip+':'+jbport)
    file.saveFile()

def setYarnFile(rmip):
    #file = xmlparsing('/H2/etc/hadoop/yarn-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/yarn-site.xml')
    file.appendProperty('yarn.resourcemanager.resource-tracker.address',rmip+':8025')
    file.appendProperty('yarn.nodemanager.aux-services','mapreduce_shuffle')
    file.saveFile()


if __name__ == '__main__':
    commands.getstatusoutput('setenforce 0')
    commands.getstatusoutput('systemctl stop firewalld')
    commands.getstatusoutput('iptables -F')
    hadoopversion = sys.argv[1]
    jobtracip = sys.argv[2]
    jobtracport = None
    rmip = jobtracip
    if hadoopversion == '1':
        jobtracport = sys.argv[3]
    # cleaning the machine
    #cleanInstall.clean()
    # installing hadoop
    #cleanInstall.install(hadoopversion)

    if hadoopversion == '1':
        setMapredFilev1(jobtracip, jobtracport)
        commands.getstatusoutput('hadoop-daemon.sh start tasktracker')
    else:
        setYarnFile(rmip)
        commands.getstatusoutput('yarn-daemon.sh start nodemanager')