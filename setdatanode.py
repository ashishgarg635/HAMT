#!/usr/bin/python2

from xmlhandling import xmlparsing
import os
import sys
import commands
#import cleanInstall


def setCoreFilev1(nnip,nnport):
    file = xmlparsing(filename='/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,'namenode address')
    #file.appendProperty('hadoop.tmp.dir','/tmp/hadoop',' temporary data')
    file.saveFile()

def setHdfsFilev1(interval):
    file = xmlparsing('/etc/hadoop/hdfs-site.xml')
    # setting datanode directory
    file.appendProperty('dfs.data.dir', '/data/datanodeData', 'Directory for saving the datanode data')
    file.appendProperty('dfs.heartbeat.interval',interval,'heartbeat time interval')
    file.saveFile()


def setCoreFilev2(nnip,nnport):
    #file = xmlparsing('/H2/etc/hadoop/core-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,'namenode address')
    #file.appendProperty('hadoop.tmp.dir','file/tmp/hadoop',' temporary data')
    file.saveFile()

def setHdfsFilev2(interval):
    #file = xmlparsing('/H2/etc/hadoop/hdfs-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME') + '/etc/hadoop/hdfs-site.xml')
    # setting datanode directory
    file.appendProperty('dfs.data.dir', 'file:/data/datanodeData', 'Directory for saving the datanode data')
    file.appendProperty('dfs.heartbeat.interval',interval,'heartbeat time interval')
    file.saveFile()

if __name__ == '__main__':
    commands.getstatusoutput('setenforce 0')
    commands.getstatusoutput('systemctl stop firewalld')
    commands.getstatusoutput('iptables -F')
    hadoopversion = sys.argv[1]
    nnip = sys.argv[2]
    nnport = sys.argv[3]
    heartbeat = sys.argv[4]
    #cleaning the machine
    #cleanInstall.clean()
    #installing hadoop
    #cleanInstall.install(hadoopversion)

    if hadoopversion=='1':
        setCoreFilev1(nnip,nnport)
        setHdfsFilev1(heartbeat)
        commands.getstatusoutput('hadoop-daemon.sh start datanode')
    else:
        setCoreFilev2(nnip,nnport)
        setHdfsFilev2(heartbeat)
        commands.getstatusoutput('hadoop-daemon.sh start datanode')

    #perform jps checking