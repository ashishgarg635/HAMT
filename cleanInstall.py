#!/usr/bin/python

import commands
import os
import sys

def clean():
    status,_ = commands.getstatusoutput('rpm -q jdk ')
    commands.getoutput('hadoop-daemon.sh   stop  namenode')
    commands.getoutput('hadoop-daemon.sh   stop  datanode')
    commands.getoutput('hadoop-daemon.sh   stop  jobtracker')
    commands.getoutput('hadoop-daemon.sh   stop  tasktracker')
    commands.getoutput('hadoop-daemon.sh   stop  secondarynamenode')
    # assuming that if jdk is not installed on the system then hadoop is also not bcz hadoop needs jdk to run
    os.chdir('/root/HadoopInstall/')
    if status:
        commands.getoutput('rpm -i jdk-7u79-linux-x64.rpm')
    #exporting JAVA_HOME variable
    commands.getoutput('export JAVA_HOME=/usr/java/jdk1.7.0_79')

    status,version = commands.getstatusoutput(' hadoop version | grep Hadoop | cut -d " " -f 2 | cut -d "."  -f 1')
    if version == '1':
        #Code for cleaning hadoop 1
        commands.getstatusoutput('rpm --erase --allmatches hadoop ')
        commands.getstatusoutput('rm  -rf  /etc/hadoop/')
        commands.getstatusoutput('rm  -rf  /etc/rc.d/init.d/hadoop-*')
        commands.getstatusoutput('rm  -rf  /etc/share/hadoop/')
        commands.getstatusoutput('rm  -rf  /var/log/hadoop')
        commands.getstatusoutput('rm  -rf  /var/run/hadoop/')
        commands.getstatusoutput('rm -rf /data')

    elif version == '2':
        #Code for cleaning hadoop 2
        commands.getoutput('yarn-daemon.sh   stop resourcemanager')
        commands.getoutput('yarn-daemon.sh   stop nodemanager')
        commands.getoutput('mr-jobhistory-daemon.sh   stop  historyserver')
        hadoop2dir = os.getenv('HADOOP_HOME')
        commands.getstatusoutput('rm  -rf  '+hadoop2dir)
        commands.getstatusoutput('rm  -rf  /data')
        readfile = open('/root/.bashrc','r')
        writefile = open('/root/.tem_bashrc','w')
        line = readfile.readline()
        while line != '':
            line = readfile.readline()
            if line.find('JAVA_HOME') == -1 and line.find('HADOOP_HOME') == -1:
                writefile.write(line)
        readfile.close()
        writefile.close()
        os.system('mv -f /root/.tem_bashrc    /root/.bashrc')                        # Shut off SELinux using command "setenforce 0"
        os.system('chcon --reference /root/.bash_profile  /root/.bashrc')  #fix for the above problem

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


def install(version):
    os.chdir('/root/HadoopInstall/')
    commands.getoutput('export JAVA_HOME=/usr/java/jdk1.7.0_79')
    if version=='1':
        commands.getstatusoutput('rpm -i hadoop-1.2.1-1.x86_64.rpm  --replacefiles ')
    elif version=='2':
        commands.getstatusoutput('tar -xzf hadoop-2.6.4.tar.gz')
        commands.getstatusoutput('mv -f  hadoop-2.6.4/    /H2')
        file = open('/root/.bashrc','a')
        file.write('export HADOOP_HOME=/H2 \n')
        #file.write('export JAVA_HOME='+os.getenv('JAVA_HOME')+'\n')
        file.write('export JAVA_HOME=/usr/java/jdk1.7.0_79' + '\n')
        file.write('export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH  \n')
        file.close()
        os.system('chcon --reference /root/.bash_profile /root/.bashrc')
        os.system('chmod +x /root/.bashrc; source /root/.bashrc')

extraclean()
clean()
install(sys.argv[1])