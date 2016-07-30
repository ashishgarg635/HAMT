#!/usr/bin/python2

from xmlhandling import xmlparsing
import sys
import commands
import os
#import cleanInstall

def setCoreFilev1(nnip,nnport):
    file = xmlparsing(filename='/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,' namenode address')
    #file.appendProperty('hadoop.tmp.dir','/usr/local/hadoop/tmp',' temporary data')
    file.saveFile()

def setMapredFilev1(jbip,jbport):
    file = xmlparsing('/etc/hadoop/mapred-site.xml')
    # setting job tracker ip
    file.appendProperty('mapred.job.tracker',jbip+':'+jbport)
    file.saveFile()

def setCoreFilev2(nnip,nnport):
    #file = xmlparsing('/H2/etc/hadoop/core-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME') + '/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,' namenode address')
    file.saveFile()

def setYarnFile(rmip):
    #file = xmlparsing('/H2/etc/hadoop/yarn-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/yarn-site.xml')
    file.appendProperty('yarn.resourcemanager.resource-tracker.address',rmip+':8025')
    file.appendProperty('yarn.resourcemanager.scheduler.address', rmip + ':8030')
    file.saveFile()


if __name__ == '__main__':
    commands.getstatusoutput('setenforce 0')
    commands.getstatusoutput('systemctl stop firewalld')
    commands.getstatusoutput('iptables -F')
    hadoopversion = sys.argv[1]
    nnip = sys.argv[2]
    nnport = sys.argv[3]
    jobtracip = commands.getoutput('hostname -i')
    jobtracport = None
    rmip = jobtracip
    if hadoopversion=='1':
        jobtracport = sys.argv[4]

    # cleaning the machine
    #cleanInstall.clean()
    # installing hadoop
    #cleanInstall.install(hadoopversion)

    if hadoopversion=='1':
        setCoreFilev1(nnip,nnport)
        setMapredFilev1(jobtracip,jobtracport)
        commands.getstatusoutput('hadoop-daemon.sh start jobtracker')
    else:
        setCoreFilev2(nnip, nnport)
        setYarnFile(rmip)
        commands.getstatusoutput('yarn-daemon.sh start resourcemanager')

    commands.getoutput('mr-jobhistory-daemon.sh   start  historyserver')
    # perform jps checking