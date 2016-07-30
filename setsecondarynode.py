#!/usr/bin/python

from xmlhandling import xmlparsing
import os
import sys
import commands
#import cleanInstall


def setCoreFilev1(nnip,nnport):
    file = xmlparsing('/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,' namenode address')
    file.saveFile()

def setHdfsFilev1(checkperiod):
    file = xmlparsing('/etc/hadoop/hdfs-site.xml')
    file.appendProperty('dfs.name.dir','/data/namenodeData')
    # file.appendProperty('dfs.http.address',nnip+':'+nnwebport)
    # file.appendProperty('dfs.secondary.http.address',snnip+':'+snnport)
    file.appendProperty('fs.checkpoint.dir','/data/check')
    file.appendProperty('fs.checkpoint.edits.dir','/data/edits')
    file.appendProperty('fs.checkpoint.period',checkperiod)
    file.saveFile()


def setCoreFilev2(nnip,nnport):
    #file = xmlparsing('/H2/etc/hadoop/core-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,' namenode address')
    file.saveFile()

def setHdfsFilev2(checkperiod):
    #file = xmlparsing('/H2/etc/hadoop/hdfs-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/hdfs-site.xml')
    file.appendProperty('dfs.name.dir','file:/data/namenodeData')
    # file.appendProperty('dfs.http.address',nnip+':'+nnwebport)
    # file.appendProperty('dfs.secondary.http.address',snnip+':'+snnport)
    file.appendProperty('fs.checkpoint.dir','file:/data/check')
    file.appendProperty('fs.checkpoint.edits.dir','file:/data/edits')
    file.appendProperty('fs.checkpoint.period',checkperiod)
    file.saveFile()


if __name__ == '__main__':
    commands.getstatusoutput('setenforce 0')
    commands.getstatusoutput('systemctl stop firewalld')
    commands.getstatusoutput('iptables -F')
    hadoopversion = sys.argv[1]
    nnip = sys.argv[2]
    nnport = sys.argv[3]
    checkpointtime = sys.argv[4]
    snnip = commands.getoutput('hostname -i')
    snnport = '50090'


    #cleaning the machine
    #cleanInstall.clean()
    #installing hadoop
    #cleanInstall.install(hadoopversion)

    if hadoopversion=='1':
        setCoreFilev1(nnip,nnport)
        setHdfsFilev1(checkpointtime)
        commands.getstatusoutput('hadoop-daemon.sh start secondarynamenode')
    else:
        setCoreFilev2(nnip,nnport)
        setHdfsFilev2(checkpointtime)
        commands.getstatusoutput('hadoop-daemon.sh start secondarynamenode')

    #NFS setup
    status,_ = commands.getstatusoutput('rpm -q nfs-utils')
    if status != 0:
        print commands.getoutput('yum install nfs-utils  -y')

    file = open('/etc/exports','a')
    file.write('/data/namenodeData         *(rw,no_root_squash)  \n')
    file.close()
    commands.getoutput('mkdir -p /data/namenodeData')
    commands.getstatusoutput('systemctl restart nfs-server')
    commands.getstatusoutput('systemctl enable  nfs-server')

