#!/usr/bin/python2

import sys
from xmlhandling import xmlparsing
import commands
import os
#import cleanInstall

def setCoreFilev1(nnip,nnport):
    file = xmlparsing(filename='/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,'namenode address')
    # file.appendProperty('hadoop.tmp.dir','/tmp/hadoop',' temporary data')
    file.saveFile()

def setHdfsFilev1(nnip,nnwebport):
    file = xmlparsing('/etc/hadoop/hdfs-site.xml')
    # setting namenode directory
    file.appendProperty('dfs.name.dir','/data/namenodeData,/data/backup')
    # setting web(http) management portal
    file.appendProperty('dfs.http.address',nnip+":"+nnwebport,'web management port.')
    file.saveFile()


def setCoreFilev2(nnip,nnport):
    file = xmlparsing('/H2/etc/hadoop/core-site.xml')
    #file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,'namenode address')
    # file.appendProperty('hadoop.tmp.dir','/tmp/hadoop',' temporary data')
    file.saveFile()

def setHdfsFilev2(nnip,nnwebport):
    #file = xmlparsing('/H2/etc/hadoop/hdfs-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/hdfs-site.xml')
    # setting namenode directory
    file.appendProperty('dfs.name.dir','file:/data/namenodeData,file:/data/backup')
    # setting web(http) management portal
    file.appendProperty('dfs.http.address',nnip+":"+nnwebport,'web management port.')
    file.saveFile()


if __name__ == '__main__':
    commands.getstatusoutput('setenforce 0')
    commands.getstatusoutput('systemctl stop firewalld')
    commands.getstatusoutput('iptables -F')
    nnip = commands.getoutput('hostname -i')
    hadoopversion = sys.argv[1]
    nnport = sys.argv[2]
    nnwebport = sys.argv[3]
    snnip = sys.argv[4]
    #cleaning the machine
    #cleanInstall.clean()
    #installing hadoop
    #cleanInstall.install(hadoopversion)

    if hadoopversion=='1':
        setCoreFilev1(nnip,nnport)
        setHdfsFilev1(nnip,nnwebport)
        commands.getstatusoutput('hadoop namenode -format')
        commands.getstatusoutput('hadoop-daemon.sh start namenode')
    else:
        setCoreFilev2(nnip,nnport)
        setHdfsFilev2(nnip,nnwebport)
        commands.getstatusoutput('hdfs namenode -format')
        commands.getstatusoutput('hadoop-daemon.sh start namenode')

    # Mounting NFS directory for namenode backup
    status, _ = commands.getstatusoutput('rpm -q nfs-utils')
    if status != 0:
        print commands.getoutput('yum install nfs-utils  -y')

    commands.getoutput('systemctl restart  nfs-server')
    print commands.getoutput('mount ' + snnip + ':/data/namenodeData   /data/backup')




