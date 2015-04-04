#!/bin/sh -e

# Stop Namenode, ResourceManager, Proxyserver, and Historyserver on enmcomp7
# Stop Datanode and NodeManager on enmcomp8 and enmcomp9 (slaves)
# Run this script on enmcomp7
# Run "jps" to check if the namenode has started on m7 and datanode has started on m8 and m9

master="enmcomp7" 	                       # Must be agree with fs.defaultFS in etc/hadoop/core-site.xml
slaves=`cat $HADOOP_PREFIX/etc/hadoop/slaves`  # Must be defined in etc/hadoop/slaves

#------------------------------------------------
# Stop Namenode on master and DataNode on slaves
#------------------------------------------------
ssh -t $master $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script hdfs stop namenode
for host in $slaves
do
    ssh -t $host $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script hdfs stop datanode
done

#-----------------------------------------------
# Stop ResourceManager on master
#-----------------------------------------------
ssh -t $master $HADOOP_YARN_HOME/sbin/yarn-daemon.sh stop resourcemanager

#-----------------------------------------------
# Stop NodeManager on slaves
#-----------------------------------------------
for host in $slaves
do
    ssh -t $host $HADOOP_YARN_HOME/sbin/yarn-daemon.sh stop nodemanager
done

#-----------------------------------------------
# Stop Proxyserver on master
#-----------------------------------------------
ssh -t $master $HADOOP_YARN_HOME/sbin/yarn-daemon.sh stop proxyserver

#-----------------------------------------------
# Stop Historyserver on master
#-----------------------------------------------
ssh -t $master $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh stop historyserver

#-----------------------------------------------
# Check if everything has stoped
#-----------------------------------------------
echo "Java processes on ${master}:"
ssh -t $master jps
for host in $slaves
do
    echo "Java processes on ${host}:"
    ssh -t $host jps
done




