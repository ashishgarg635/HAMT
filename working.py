#!/usr/bin/python2

from os import system
from clustering import hadoop
from Ondemand import docker
import commands


# reading and values from a file 'filename'
def extract(filename):
    file = open(filename,'r')
    data = file.read()
    file.close()
    return data

# preparing a formatted string having indexes and ips for dialog 'menu' box
def preparestringformenu(items):
    ipstring = ''
    for i in range(len(items)):
        ipstring += str(i + 1) + '   " ' + items[i] + ' ( CPUs : ' + cluster.ipwithstats[items[i]][0] + ',' \
                    ' Clock Speed : ' + cluster.ipwithstats[items[i]][1] + 'MHz ,' \
                    ' Ram : ' + cluster.ipwithstats[items[i]][2] + 'M ,' + \
                    ' Disk : ' + cluster.ipwithstats[items[i]][3] + ' ) "  '
    return ipstring

# preparing a formatted string having indexes and ips for dialog 'checklist' box
def preparestringforchecklist(items):
    ipstring = ''
    for i in range(len(items)):
        ipstring += str(i + 1) + '   " ' + items[i] + ' ( CPUs : ' + cluster.ipwithstats[items[i]][0] + ',' \
                    ' Clock Speed : ' + cluster.ipwithstats[items[i]][1] + 'MHz ,' \
                    ' Ram : ' + cluster.ipwithstats[items[i]][2] + 'M ,' + \
                    ' Disk : ' + cluster.ipwithstats[items[i]][3] + ' ) "  ' + ' off '
    return ipstring


# Launch of the hadoop Clustering dialog screen
def welcome():
    system("dialog --ok-label ' Press to Enter ' --backtitle 'My Project'  --title 'Hadoop Cluserting'  \
            --msgbox  '\\n\\n\\n WELCOME TO Hadoop Clustering Management Tool'  10 50")
    system('dialog  --backtitle "My Project"  --title "Hadoop Cluserting" --passwordbox  \
                    " \\n Enter the password : \\n " 15 30  2> temp/pass.out')
    key = extract('temp/pass.out')

    if not key == 'om':
        system('dialog  --backtitle " My Project "  --title "Hadoop Cluserting"  --msgbox  \
         " \\n\\n     Invalid Password. Entry not allowed \\n "  9 50')
        return '-1','-1'

    system('dialog  --backtitle " My Project "  --title "Hadoop Clusterting"  \
        --menu " \\n\\n Choose a Hadoop Vesion \\n " 15 50 2 1 "Hadoop v1.2" 2 "Hadoop v2.6"  2> temp/hselect.out')
    versionchoice = extract('temp/hselect.out')

    return versionchoice

#getting mode from user
def getMode():
    system('dialog --backtitle " My Project " --title " Hadoop Clustering " --menu \
               "\\n\\nChoose a mode \\n " 13 50 3 1 "Manual" 2 "Automatic"  3 "On-demand" 2> temp/mode.out')
    mode = extract('temp/mode.out')

    return mode




def manual(versionchoice):

    # getting the statistics of the nodes
    cluster.calStats()

    #selection of namenode
    ipstring = preparestringformenu(cluster.availablenodes)
    system('dialog --backtitle " My Project " --title " Hadoop Clustering " --menu \
           "\\n\\nChoose a machine for Namenode \\n " 20 100 '+str(cluster.availablenodes.__len__())+'  '+ipstring+'  2> temp/namenode.out')
    cluster.namenode = cluster.availablenodes[ int(extract('temp/namenode.out') ) -1 ]
    cluster.availablenodes.remove(cluster.namenode)

    #Selection of job tracker or resource manager
    ipstring = preparestringformenu(cluster.availablenodes)
    if versionchoice=='1':
        system('dialog --backtitle " My Project " --title " Hadoop Clustering " --menu \
            "\\n\\nChoose a machine for Job Tracker \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/jobtracker.out')
    else:
        system('dialog --backtitle " My Project " --title " Hadoop Clustering " --menu \
            "\\n\\nChoose a machine for Resource Manager \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/jobtracker.out')
    cluster.jobtracker = cluster.availablenodes[ int(extract('temp/jobtracker.out')) - 1 ]
    cluster.availablenodes.remove(cluster.jobtracker)

    # Selection of datanodes
    ipstring = preparestringforchecklist(cluster.availablenodes)
    system('dialog --backtitle " My Project " --title " Hadoop Clustering " --checklist \
        "\\n\\nChoose machine for Datanodes \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/datanodes.out')
    selected = extract('temp/datanodes.out').split(' ')
    for i in range(selected.__len__()):
        cluster.datanodes.append( cluster.availablenodes[ int(selected[i]) - 1] )
    for i in range(cluster.datanodes.__len__()):
        cluster.availablenodes.remove(cluster.datanodes[i])

    # Selection of tasktrackers or nodemanagers
    ipstring = preparestringforchecklist(cluster.availablenodes)
    if versionchoice=='1':
        system('dialog --backtitle " My Project " --title " Hadoop Clustering " --checklist \
            "\\n\\nChoose machines for Task Trackers \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/tasktrackers.out')
    else:
        system('dialog --backtitle " My Project " --title " Hadoop Clustering " --checklist \
            "\\n\\nChoose machines for Node Managers \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/tasktrackers.out')
    selected = extract('temp/tasktrackers.out').split(' ')
    for i in range(selected.__len__()):
        cluster.tasktracker.append(cluster.availablenodes[ int(selected[i]) - 1])
    for i in range(cluster.tasktracker.__len__()):
        cluster.availablenodes.remove(cluster.tasktracker[i])


    # selection of secondary namenode
    ipstring = preparestringformenu(cluster.availablenodes)
    system('dialog --backtitle " My Project " --title " Hadoop Clustering " --menu \
               "\\n\\nChoose a machine for Secondary Namenode \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/namenodesecond.out')
    cluster.secondarynamenode = cluster.availablenodes[int(extract('temp/namenodesecond.out')) - 1]
    cluster.availablenodes.remove(cluster.secondarynamenode)

    # selection of secondary namenode
    ipstring = preparestringformenu(cluster.availablenodes)
    system('dialog --backtitle " My Project " --title " Hadoop Clustering " --menu \
                   "\\n\\nChoose a machine for Client  \\n " 20 100 ' + str(cluster.availablenodes.__len__()) + '  ' + ipstring + '  2> temp/client.out')
    cluster.client = cluster.availablenodes[int(extract('temp/client.out')) - 1]
    cluster.availablenodes.remove(cluster.client)

    # file transfer to the nodes
    cluster.filetransfer(versionchoice)

    # setting namenode, datanode, jobtracker, tasktracker, secondary namenode .....
    startsetup(versionchoice)
    system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n   NIS setup... "   8  30  ')
    nissetup()
    displaydetails(versionchoice)

def startsetup(versionchoice):
    # setting up secondary namenode
    system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building secondary namenode ... "   8  50  ')
    system('ssh root@' + cluster.secondarynamenode + '  /root/HadoopInstall/cleanInstall.py  ' + versionchoice)
    system('ssh root@' + cluster.secondarynamenode + '  /root/HadoopInstall/setsecondarynode.py  ' + versionchoice + '  ' + cluster.namenode + '  ' + cluster.nnport + '  ' + cluster.checkpointperiod)

    # setting up namenode
    system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building namenode ... "   8  50  ')
    system('ssh root@' + cluster.namenode + '  /root/HadoopInstall/cleanInstall.py  '+versionchoice )
    system('ssh root@' + cluster.namenode + '  /root/HadoopInstall/setnamenode.py  ' + versionchoice +'  ' + cluster.nnport + '  50070  '+cluster.secondarynamenode)

    # setting up job tracker or resource manager
    system('ssh root@' + cluster.jobtracker + '  /root/HadoopInstall/cleanInstall.py ' +versionchoice)
    if versionchoice=='1':
        system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building jobtracker... "   8  50  ')
        system('ssh root@' + cluster.jobtracker + '  /root/HadoopInstall/setjobtracker.py  ' + versionchoice + ' ' + cluster.namenode + '  ' + cluster.nnport + '  ' + cluster.jbport)
    else:
        system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building resource manager... "   8  50  ')
        system('ssh root@' + cluster.jobtracker + '  /root/HadoopInstall/setjobtracker.py  ' + versionchoice + '   ' + cluster.namenode + '  ' + cluster.nnport)

    # setting up datanodes
    for i in range(cluster.datanodes.__len__()):
        system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building datanodes ... "   8  50  ')
        system('ssh root@' + cluster.datanodes[i] + '  /root/HadoopInstall/cleanInstall.py '+ versionchoice)
        system('ssh root@' + cluster.datanodes[i] + '  /root/HadoopInstall/setdatanode.py  ' + versionchoice + '   ' + cluster.namenode + '  ' + cluster.nnport + '  ' + cluster.dnheartbeat)

    # setting up tasktrackers or node managers
    for i in range(cluster.tasktracker.__len__()):
        system('ssh root@' + cluster.tasktracker[i] + '  /root/HadoopInstall/cleanInstall.py  '+versionchoice)
        if versionchoice=='1':
            system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n      Building tasktracker ... "   8  50  ')
            system('ssh root@' + cluster.tasktracker[i] + '  /root/HadoopInstall/settasktracker.py  ' + versionchoice + '   ' + cluster.jobtracker + '  ' + cluster.jbport )
        else:
            system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building node manager... "   8  50  ')
            system('ssh root@' + cluster.tasktracker[i] + '  /root/HadoopInstall/settasktracker.py  ' + versionchoice +'   ' + cluster.jobtracker)

    # setting up client node
    #system('ssh root@' + cluster.client + '  /root/HadoopInstall/cleanInstall.py ' + versionchoice)
    system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n       Building Client ... "   8  50  ')
    if versionchoice=='1':
        system('ssh root@' + cluster.client + '   /root/HadoopInstall/setclient.py  ' + versionchoice + \
               '  '+ cluster.namenode +'  '+ cluster.nnport + '   ' + cluster.jobtracker+'  '+ cluster.jbport + \
               '  ' + cluster.replication + '  ' + cluster.blocksize)
    else:
        system('ssh root@' + cluster.client + '   /root/HadoopInstall/setclient.py  ' + versionchoice + \
               '  ' + cluster.namenode + '  ' + cluster.nnport + '   ' + cluster.jobtracker + '  ' + \
               cluster.yarnframework + '  ' + cluster.replication + '  ' + cluster.blocksize)




def automatic(versionchoice):
    # getting the statistics of the nodes
    cluster.calStats()
    cluster.namenode = cluster.availablenodes[0]
    cluster.availablenodes.remove(cluster.namenode)

    cluster.jobtracker = cluster.availablenodes[0]
    cluster.availablenodes.remove(cluster.jobtracker)

    cluster.secondarynamenode = cluster.availablenodes[0]
    cluster.availablenodes.remove(cluster.secondarynamenode)

    cluster.client = cluster.availablenodes[0]
    cluster.availablenodes.remove(cluster.client)

    # Taking number of datanodes
    system('dialog --backtitle " My Project " --title " Hadoop Clustering "   --inputbox    \
            "\\n\\nEnter the number of Datanodes \\n     Availables machines : '+str(cluster.availablenodes.__len__()) \
           +' " 12 50  2> temp/datanodes.out')
    numDN = extract('temp/datanodes.out')
    cluster.datanodes = cluster.availablenodes[:int(numDN)]
    del cluster.availablenodes[:int(numDN)]


    # Taking number of tasktrackers
    if versionchoice=='1':
        system('dialog --backtitle " My Project " --title " Hadoop Clustering "   --inputbox    \
            "\\n\\nEnter the number of Task Trackers \\n    Availables machines : '+ str(cluster.availablenodes.__len__()) \
               +' " 12 50  2> temp/tasktrackers.out')
    elif versionchoice=='2':
        system('dialog --backtitle " My Project " --title " Hadoop Clustering "   --inputbox    \
            "\\n\\nEnter the number of Node Managers \\n    Availables machines : '+str(cluster.availablenodes.__len__()) \
               +' " 12 50  2> temp/tasktrackers.out')
    numTT = extract('temp/tasktrackers.out')
    cluster.tasktracker = cluster.availablenodes[:int(numTT)]
    del cluster.availablenodes[:int(numTT)]

    # file transfer to the nodes
    cluster.filetransfer(versionchoice)

    # setting namenode, datanode, jobtracker, tasktracker, secondary namenode .....
    startsetup(versionchoice)

    # NIS setup
    system('dialog --backtitle  " My Project "  --title  " Hadoop Clustering "   --infobox  " \\n\\n   NIS setup... "   8  30  ')
    nissetup()

    displaydetails(versionchoice)



def displaydetails(version):
    string = ' " \\n\\n                 --- Cluster Details --- \\n\\n    HDFS Web Portal  --->  ' + cluster.namenode + ':' + '50070 \\n '
    if version=='1':
        string += '   Mapreduce Web Portal  --->  ' + cluster.jobtracker + ':' + '50030 \\n '
    elif version=='2':
        string += '   Yarn Web Portal  --->  ' + cluster.jobtracker + ':' + '8088 \\n '
    string += ' \\n    Namenode  :  ' + cluster.namenode + ' \\n    Datanodes  :  ' + ', '.join(cluster.datanodes)
    if version == '1':
        string += ' \\n    Job Tracker  :  ' + cluster.jobtracker + ' \\n    Task Trackers  :  ' + ', '.join(cluster.tasktracker)
    elif version == '2':
        string += ' \\n    Resource Manager  :  ' + cluster.jobtracker  + ' \\n    Node Managers  :  ' + ', '.join(cluster.tasktracker)
    string += ' \\n    Secondary Namenode  :  '+ cluster.secondarynamenode + ' \\n    Client  :  '+ cluster.client + ' " '
    system('dialog --backtitle " My Project " --title " Hadoop Clustering "   --msgbox  '+  string + ' 20 60 ')

def nissetup():
    nisdomainname = commands.getoutput('ssh root@'+ cluster.client + '    ypdomainname')
    nisclient(cluster.namenode,nisdomainname,cluster.client)
    nisclient(cluster.jobtracker, nisdomainname, cluster.client)
    nisclient(cluster.secondarynamenode, nisdomainname, cluster.client)
    for k in range(cluster.datanodes.__len__()):
        nisclient(cluster.datanodes[k], nisdomainname, cluster.client)
    for k in range(cluster.tasktracker.__len__()):
        nisclient(cluster.tasktracker[k], nisdomainname, cluster.client)



def nisclient(ip,domainname,serverip):
    commands.getoutput('ssh root@'+ip+'   systemctl stop  ypserv')
    status,_ = commands.getstatusoutput('ssh root@'+ip+'  rpm -q ypbind')
    if status != 0:
        print commands.getoutput('ssh root@'+ip+'   yum install ypbind -y')
    commands.getoutput('ssh root@'+ ip + '   authconfig   --update --enablenis  --nisdomain='+domainname+'   --nisserver='+serverip)

def ondemand(version):
    system('dialog  --backtitle " My Project "  --title "Hadoop Clusterting"  \
        --menu " \\n\\n Choose a type \\n " 15 50 2 1 " Datanode " 2 " Tasktracker "  2> temp/choice.out')
    choice = extract('temp/choice.out')

    if choice == '1':
        conname = dockerobj.createcontainer(version)
        dockerobj.launch_datanode(conname,version,cluster.namenode,cluster.nnport)
    elif choice == '2':
        conname = dockerobj.createcontainer(version)
        dockerobj.launch_datanode(conname, version, cluster.jobtracker, cluster.jbport)
    else:
        pass


cluster = hadoop()
dockerobj = docker()
dockerobj.restart()
Hversion = welcome()
while True:
    Mode = getMode()
    if Mode == '1':
        manual(Hversion)
    elif Mode == '2':
        automatic(Hversion)
    elif Mode == '3':
        ondemand(Hversion)
    else:
        break
#except Exception :
#    system('dialog --backtitle " My Project " --title " Hadoop Clustering " --msgbox \
#        "\\n\\n       Thank You ... \\n " 8  30')

