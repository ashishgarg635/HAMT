#!/usr/bin/python2
from tuned.utils.commands import commands

import ipgrab
import commands
import os

class hadoop:
    nodesips = None
    ipwithstats = dict()

    namenode = None
    nnport = '10001'

    secondarynamenode = None
    checkpointperiod = '30'

    jobtracker = None
    jbport = '9001'

    datanodes = list()
    dnheartbeat = '3'
    replication = '3'
    blocksize = '134217728'

    yarnframework = 'yarn'

    tasktracker = list()
    availablenodes = None

    client = None

    def __init__(self):
        _,hostip = ipgrab.gethostip()
        _,netmask = ipgrab.getnetmask(hostip)
        _,self.nodesips =ipgrab.getnodesip(hostip,netmask)
        self.availablenodes = list(self.nodesips)

    def calStats(self):
        for ip in self.nodesips:
            commands.getstatusoutput('ssh root@'+ip+'  rm -rf /root/HadoopInstall')
            commands.getstatusoutput('ssh root@'+ip+'  mkdir /root/HadoopInstall')
            commands.getstatusoutput('scp stats.py  '+ip+':/root/HadoopInstall/')
            _,stat = commands.getstatusoutput('ssh root@'+ip+'  /root/HadoopInstall/stats.py')
            self.ipwithstats[ip] = stat.split('/')


    def filetransfer(self,version):
        #Sending files to the namenode
        os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n         Sending files to Namenode ... \\n " 10  50')
        commands.getstatusoutput('scp  packages/jdk-7u79-linux-x64.rpm ' + self.namenode + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  cleanInstall.py ' + self.namenode + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  xmlhandling.py ' + self.namenode + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  setnamenode.py ' + self.namenode + ':/root/HadoopInstall/')
        if version == '1':
            commands.getstatusoutput('scp  packages/hadoop-1.2.1-1.x86_64.rpm ' + self.namenode + ':/root/HadoopInstall/')
        elif version == '2':
            commands.getstatusoutput('scp  packages/hadoop-2.6.4.tar.gz ' + self.namenode + ':/root/HadoopInstall/')

        #Sending files to the jobtracker or resource manager
        if version=='1':
            os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n         Sending files to Job Tracker ... \\n " 10  50')
        elif version=='2':
            os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n      Sending files to Resource Manager ... \\n " 10  50')

        commands.getstatusoutput('scp  packages/jdk-7u79-linux-x64.rpm ' + self.jobtracker + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  cleanInstall.py ' + self.jobtracker + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  xmlhandling.py ' + self.jobtracker + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  setjobtracker.py ' + self.jobtracker + ':/root/HadoopInstall/')
        if version == '1':
            commands.getstatusoutput('scp  packages/hadoop-1.2.1-1.x86_64.rpm ' + self.jobtracker + ':/root/HadoopInstall/')
        elif version == '2':
            commands.getstatusoutput('scp  packages/hadoop-2.6.4.tar.gz ' + self.jobtracker + ':/root/HadoopInstall/')

        #Sending files to the datanodes
        os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n         Sending files to Datanodes ... \\n " 10  50')
        for k in range(len(self.datanodes)):
            commands.getstatusoutput('scp  packages/jdk-7u79-linux-x64.rpm ' + self.datanodes[k] + ':/root/HadoopInstall/')
            commands.getstatusoutput('scp  cleanInstall.py ' + self.datanodes[k] + ':/root/HadoopInstall/')
            commands.getstatusoutput('scp  xmlhandling.py ' + self.datanodes[k] + ':/root/HadoopInstall/')
            commands.getstatusoutput('scp  setdatanode.py ' + self.datanodes[k] + ':/root/HadoopInstall/')
            if version == '1':
                commands.getstatusoutput('scp  packages/hadoop-1.2.1-1.x86_64.rpm ' + self.datanodes[k] + ':/root/HadoopInstall/')
            elif version == '2':
                commands.getstatusoutput('scp  packages/hadoop-2.6.4.tar.gz ' + self.datanodes[k] + ':/root/HadoopInstall/')


        # Sending files to the tasktracker
        if version=='1':
            os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n         Sending files to TaskTrackers ... \\n " 10  50')
        elif version=='2':
            os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n        Sending files to Node Managers ... \\n " 10  50')
        for k in range(len(self.tasktracker)):
            commands.getoutput('scp  packages/jdk-7u79-linux-x64.rpm ' + self.tasktracker[k] + ':/root/HadoopInstall/')
            commands.getstatusoutput('scp  cleanInstall.py ' + self.tasktracker[k] + ':/root/HadoopInstall/')
            commands.getstatusoutput('scp  xmlhandling.py ' + self.tasktracker[k] + ':/root/HadoopInstall/')
            commands.getstatusoutput('scp  settasktracker.py ' + self.tasktracker[k] + ':/root/HadoopInstall/')
            if version == '1':
                commands.getstatusoutput('scp  packages/hadoop-1.2.1-1.x86_64.rpm ' + self.tasktracker[k] + ':/root/HadoopInstall/')
            elif version == '2':
                commands.getstatusoutput('scp  packages/hadoop-2.6.4.tar.gz ' + self.tasktracker[k] + ':/root/HadoopInstall/')

        # Sending files to the secondary namenode
        os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n     Sending files to Secondary Namenode ... \\n " 10  50')
        commands.getstatusoutput('scp  packages/jdk-7u79-linux-x64.rpm ' + self.secondarynamenode + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  cleanInstall.py ' + self.secondarynamenode + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  xmlhandling.py ' + self.secondarynamenode + ':/root/HadoopInstall/')
        commands.getstatusoutput('scp  setsecondarynode.py ' + self.secondarynamenode + ':/root/HadoopInstall/')
        if version == '1':
            commands.getstatusoutput('scp  packages/hadoop-1.2.1-1.x86_64.rpm ' + self.secondarynamenode + ':/root/HadoopInstall/')
        elif version == '2':
            commands.getstatusoutput('scp  packages/hadoop-2.6.4.tar.gz ' + self.secondarynamenode + ':/root/HadoopInstall/')


        # Sending files to the client node
        os.system('dialog --backtitle " My Project " --title " Hadoop Clustering " --infobox "\\n\\n\\n     Sending files to Client node ... \\n " 10  50')
        commands.getoutput('scp  packages/jdk-7u79-linux-x64.rpm  ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  packages/apache-hive-1.2.1-bin.tar.gz  ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  packages/pig-0.12.1.tar.gz  ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  packages/splunk-6.3.1-f3e41e4b37b2-linux-2.6-x86_64.rpm  ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  packages/sqoop-1.4.5.bin__hadoop-1.0.0.tar.gz  ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  packages/dialog-1.2-4.20130523.el7.x86_64.rpm  ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  cleanInstall.py   ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  xmlhandling.py   ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  setclient.py   ' + self.client + ':/root/HadoopInstall/')
        commands.getoutput('scp  App.py   ' + self.client + ':/usr/bin/Happ')
        if version == '1':
            commands.getstatusoutput('scp  packages/hadoop-1.2.1-1.x86_64.rpm ' + self.client + ':/root/HadoopInstall/')
        elif version == '2':
            commands.getstatusoutput('scp  packages/hadoop-2.6.4.tar.gz ' + self.client + ':/root/HadoopInstall/')


