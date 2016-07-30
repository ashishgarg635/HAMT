#!/usr/bin/python

from xmlhandling import xmlparsing
import sys
import commands
import os
import cleanInstall

def setCoreFilev1(nnip,nnport):
    file = xmlparsing('/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,' namenode address')
    #file.appendProperty('hadoop.tmp.dir','/usr/local/hadoop/tmp',' temporary data')
    file.saveFile()

def setHdfsFilev1(replica,size):
    file = xmlparsing('/etc/hadoop/hdfs-site.xml')
    # setting datanode directory
    file.appendProperty('dfs.replication',replica,'Number of replications')
    file.appendProperty('dfs.block.size', size , 'Number of replications')
    file.saveFile()

def setMapredFilev1(jbip,jbport):
    file = xmlparsing('/etc/hadoop/mapred-site.xml')
    # setting job tracker ip
    file.appendProperty('mapred.job.tracker',jbip+':'+jbport)
    file.saveFile()

def setCoreFilev2(nnip,nnport):
    file = xmlparsing(os.getenv('HADOOP_HOME') + '/etc/hadoop/core-site.xml')
    file.appendProperty('fs.default.name','hdfs://'+nnip+':'+nnport,' namenode address')
    file.saveFile()

def setHdfsFilev2(replica,size):
    file = xmlparsing(os.getenv('HADOOP_HOME') + '/etc/hadoop/hdfs-site.xml')
    file.appendProperty('dfs.replication', replica, 'Number of replications')
    file.appendProperty('dfs.block.size', size, 'Number of replications')
    file.saveFile()

def setMapredFilev2(framework):
    #commands.getoutput('mv  '+os.getenv('HADOOP_HOME')+'/etc/hadoop/mapred-site.xml.template  '  \
    #                   + os.getenv('HADOOP_HOME')+'/etc/hadoop/mapred-site.xml')
    file = xmlparsing(os.getenv('HADOOP_HOME') +'/etc/hadoop/mapred-site.xml')
    file.appendProperty('mapreduce.framework.name',framework)
    file.saveFile()

def setYarnFile(rmip):
    file = xmlparsing(os.getenv('HADOOP_HOME')+'/etc/hadoop/yarn-site.xml')
    file.appendProperty('yarn.resourcemanager.resource-tracker.address',rmip+':8025')
    file.appendProperty('yarn.resourcemanager.scheduler.address', rmip + ':8030')
    file.appendProperty('yarn.resourcemanager.address', rmip + ':8032')
    file.saveFile()


def extraclean():
    # cleaning splunk
    commands.getstatusoutput('/opt/splunk/bin/splunk  stop')
    commands.getoutput('rpm --erase --allmatches  splunk')
    commands.getoutput('rm -rf /opt/splunk')

    # cleaning pig framework
    #commands.getoutput('mr-jobhistory-daemon.sh   start  historyserver')
    commands.getoutput('rm -rf /pig')


    #cleaning hive framework
    commands.getoutput('rm  -rf  /hive')


def packagename(token):
    return  commands.getoutput('ls  | grep '+token)

def extrainstall(rmip):
    os.chdir('/root/HadoopInstall/')
    print commands.getoutput('cp  -f  '+os.getenv('HADOOP_HOME')+'/etc/hadoop/mapred-site.xml.template    '+os.getenv('HADOOP_HOME')+'/etc/hadoop/mapred-site.xml')
    # Installing splunk
    commands.getoutput('rpm -i '+packagename('splunk'))
    status,output = commands.getstatusoutput('/opt/splunk/bin/splunk | grep "Splunk web interface" ')
    splunkaddress = None
    if status == 0:
        splunkaddress=output[ output.rfind(' ') :]

    # Installing pig framework
    commands.getoutput('tar -xzf '+packagename('pig'))
    print commands.getoutput('mv  -f  pig-0.12.1    /pig')
    if commands.getoutput('cat /root/.bashrc  | grep  PIG_HOME   ') == '':
        file = open('/root/.bashrc','a')
        file.write('export PIG_HOME=/pig \n')
        file.write('export PATH=$PIG_HOME/bin:$PATH\n')
        file.close()
        commands.getoutput('chcon --reference /root/.bash_profile    /root/.bashrc')
    file = None
    if hadoopversion == '1':
        file = xmlparsing('/etc/hadoop/mapred-site.xml')
    elif hadoopversion == '2':
        file = xmlparsing(os.getenv('HADOOP_HOME') + '/etc/hadoop/mapred-site.xml')
    file.appendProperty('mapreduce.jobhistory.address', rmip + ':10020')
    file.saveFile()



    # Installing Hive framework
    commands.getoutput('tar -xzf ' + packagename('hive'))
    print commands.getoutput('mv   -f   apache-hive-1.2.1-bin     /hive')
    if commands.getoutput('cat /root/.bashrc  | grep  HIVE_HOME   ') == '':
        file = open('/root/.bashrc', 'a')
        file.write('export HIVE_HOME=/hive \n')
        file.write('export PATH=$HIVE_HOME/bin:$PATH \n')
        if hadoopversion=='2':
            file.write('export  HADOOP_USER_CLASSPATH_FIRST=true')
        file.close()
        commands.getoutput(' chmod +x  /root/.bashrc ;  source  /root/.bashrc')
        commands.getoutput('chcon --reference /root/.bash_profile    /root/.bashrc')


def nis_server():
    status, _ = commands.getstatusoutput('rpm -q ypserv')
    if status != 0:
        print commands.getoutput('yum install ypserv -y')
    hostname = commands.getoutput('hostname')
    commands.getoutput('nisdomainname    '+hostname)
    commands.getoutput('systemctl  restart  ypserv')
    os.chdir('/var/yp')
    commands.getoutput('make')



if __name__ == '__main__':
    commands.getstatusoutput('setenforce 0')
    commands.getstatusoutput('systemctl stop firewalld')
    commands.getstatusoutput('iptables -F')
    hadoopversion = sys.argv[1]
    nnip = sys.argv[2]
    nnport = sys.argv[3]
    jobtracip = sys.argv[4]
    jobtracport = None
    rmip = jobtracip
    framework = None
    if hadoopversion=='1':
        jobtracport = sys.argv[5]
    else:
        framework = sys.argv[5]
    numReplica = sys.argv[6]
    blocksize = sys.argv[7]

    # cleaning the machine
    print 'extra cleaning'
    extraclean()
    print 'running clean and install'
    commands.getoutput('/root/HadoopInstall/cleanInstall.py  '+hadoopversion)
    #cleanInstall.clean()
    # installing hadoop
    #cleanInstall.install(hadoopversion)
    print 'extra install'
    extrainstall(rmip)

    print 'configuring ...'
    if hadoopversion=='1':
        setCoreFilev1(nnip,nnport)
        setHdfsFilev1(numReplica,blocksize)
        setMapredFilev1(jobtracip,jobtracport)
    else:
        setCoreFilev2(nnip, nnport)
        setHdfsFilev2(numReplica,blocksize)
        setMapredFilev2(framework)
        setYarnFile(rmip)

    #NIS Server configuration call
    nis_server()