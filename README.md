HAMT(Hadoop Automation and Management Tool)
###########################################

HAMT is a tool for Hadoop cluster automation and management over Hadoop version 1 and 2 both. It allows the administrator to configure single-node as well as multi-node cluster of machines of Hadoop Framework to tackle the problem raised because of Big Data. It automates the process of installation and configuration of Hadoop version 1 and Hadoop version 2 along in support with other tools and frameworks such as Pig, Hive, HBase, etc, which work over Hadoop cluster. 
	
	HAMT provides two ways to the administrator to choose from, namely 
1.	Manual Configuration 
2.	Automatic Configuration 

In manual configuration, the administrator is provided with the statistics (number of cores, clock speed, RAM and free disk Space) of all the available machine present in the cluster. The administrator rightly chooses the NameNode, DataNode, Resource Manager, Node Manager, and Secondary Node.

In automatic configuration, the tool itself select the appropriate machines for the roles of Name Node, DataNode, Resource Manager and Node Manager. The administrator is free from analyzing the stats of different machines and whole setup and configuration goes with click.

HAMT also have another mode called “On-demand” in which the administrator can instantly launch DataNode and NodeManager as per requirement. To provide On-demand facility, it uses the awesome technology of “Dockers” that uses the resources by asking to the kernel of base machine. To deal with On-demand, the Docker images Hadoop 1 and Hadoop 2 have been created separately and loaded to the Docker Engine. Whenever any DataNode or NodeManager is to be launched, a new container is launched over the Docker Engine to provide that particular service.

HAMT provides the following features, frameworks and interfaces 
1.	Splunk (Operational Intelligence Tool)
2.	Apache Hive
3.	Apache Sqoop
4.	Apache Pig
5.	Happ ( TUI Interface )
6.	High Availability through NFS and Secondary NameNode. 

Splunk 
#######

The project HAMT also includes “Splunk” which provides the platform that provides the abilities for searching, monitoring and analyzing machine-generated Big Data through a web interface. Splunk is a horizontal technology used for application management, security and compliance as well as business and web analytics. Splunk uses its operational Intelligence to perform these marvelous tasks. 
Splunk captures, indexes and correlates real-time data in a searchable repository from which it can generate graphs, reports, alerts, dashboards, and visualizations. It is useful in making machine data accessible across an organization by identifying data patterns, providing metrics, diagnosing problems and providing intelligence for business operation.
It helps the administrator of Hadoop cluster to analyze the logs generated to handle Big Data and provide ease in monitoring and searching the important information.

Apache Hive 
############

HAMT also installs and configures the Framework “Apache Hive”. Hive is a data warehouse infrastructure built on top of Hadoop for providing data summarization, query and analysis. It supports analysis of large datasets stored in Hadoop’s HDFS and compatible file systems. It provides an SQL-like language called HiveQL with schema on read and transparently converts queries to MapReduce and Spark. 

Apache Sqoop
#############

Apache Sqoop is a tool designed to transfer data between Hadoop and relational database servers. It is used to import data from relational databases such as MySQL, Oracle to Hadoop HDFS and export from Hadoop file system to relational databases. 

Apache PIG
############

Apache Pig is a high-level platform for creating programs that run on Apache Hadoop. The language for this platform is called Pig Latin. Pig can execute its Hadoop jobs in MapReduce. It is used to analyze larger sets of data representing them as data flows. We can perform all the data manipulation operations in Hadoop using Pig.

Happ
########

Happ is another part of HAMT, a TUI (Terminal User Interface) application on the client node to provide ease to the administrator in managing and performing tasks on the Hadoop HDFS and can also run the MapReduce jobs without running any commands in the terminal.


High Availability
####################

NameNode is a single-point of failure which stores all the metadata of the HDFS and MapReduce jobs. If this NameNode is crashed, then the data stored on the Hadoop cluster cannot be accessed as no one other than NameNode knows where data blocks are stored. So, there leaves no means to recover the data. But if we preserve the metadata, then it is possible to retrieve the data from HDFS. To handle this situation, HAMT provides two options 
1.	Secondary NameNode
2.	NFS
# Secondary NameNode is a part of Hadoop framework which checkpoints the NameNode data at a particular period of time called checkpoint period. 
# Another method is to use NFS (Network File System) so that whenever any damage is happened to the NameNode, then the metadata is still preserved and can be used to recover the data blocks stored in HDFS.
