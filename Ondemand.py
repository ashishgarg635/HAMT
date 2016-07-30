#!/usr/bin/python

import commands
from os import system

class docker:

    imagenamev1 = 'ashishgarg7/hadoopv1'
    imagetagv1  = 'latest'
    imagenamev2 = 'ashishgarg7/hadoopv2'
    imagetagv2 = 'latest'
    nodes = list()
    nodeids = list()
    datanodes = list()
    tasktrackers = list()

    def __init__(self):
        pass


    def restart(self):
        commands.getoutput('systemctl restart docker')


    def createcontainer(self,version):

        containername = 'hadoopv' + version + 'node' + str(self.datanodes.__len__() + 1)
        conid = ''
        if version == '1':
            conid = commands.getoutput('docker run -d -it --privileged --name  '+ containername + '  '+ self.imagenamev1+':'+self.imagetagv1)
        elif version == '2':
            conid = commands.getoutput('docker run -d -it --privileged --name  '+ containername + '  '+ self.imagenamev2+':'+self.imagetagv2)
        self.nodes.append(containername)
        self.nodeids.append(conid)

        return containername


    def launch_datanode(self,name,version,nnip,nnport):
        commands.getoutput('docker exec -it --privileged ' + name  + '  /startnode    datanode   ' + nnip +'    ' + nnport)
        self.datanodes.append(name)

    def launch_tasktracker(self,name,version,jbip,jbport):
        if version == '1':
            commands.getoutput('docker exec -it --privileged ' + name + '  /startnode     tasktracker ' + jbip  + '      '+ jbport)
            self.tasktrackers.append(name)
        else:
            commands.getoutput('docker exec -it --privileged ' + name + '  /startnode     nodemanager ' + jbip + '      ' + jbport)
            self.tasktrackers.append(name)
