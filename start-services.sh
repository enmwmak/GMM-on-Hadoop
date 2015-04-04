#!/bin/sh -e

# Start Namenode, ResourceManager, Proxyserver, and Historyserver on enmcomp7
# Start Datanode and NodeManager on enmcomp8 and enmcomp9 (slaves)
# Run this script on enmcomp7
# Run "jps" to check if the namenode has started on m7 and datanode has started on m8 and m9

master="enmcomp7"  					# Must be agree with fs.defaultFS in etc/hadoop/core-site.xml
slaves=`cat $HADOOP_PREFIX/etc/hadoop/slaves` 		# Must be defined in etc/hadoop/slaves

#------------------------------------------------------
# Remove the folder storing namenode and datanodes
#------------------------------------------------------
ssh -t $master rm -rf /tmp/hadoop-${LOGNAME}
for host in $slaves
do
    ssh -t $host rm -rf /tmp/hadoop-${LOGNAME}
done
rm $HADOOP_PREFIX/logs/* -rf

#------------------------------------------------------
# Format a namenode. It will erase all data in the HDFS
#------------------------------------------------------
$HADOOP_PREFIX/bin/hdfs namenode -format

#------------------------------------------------
# Start Namenode on master and DataNode on slaves
#------------------------------------------------
ssh -t $master $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_PREFIX/etc/hadoop --script hdfs start namenode
for host in $slaves
do
    ssh -t $host $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_PREFIX/etc/hadoop --script hdfs start datanode
done

#-----------------------------------------------
# Start ResourceManager on master
#-----------------------------------------------
ssh -t $master $HADOOP_YARN_HOME/sbin/yarn-daemon.sh start resourcemanager

#-----------------------------------------------
# Start NodeManager on slaves
# Note: To make thing works, the variable yarn.nodemanager.hostname in yarn-site.xml
#       should be equal to the hostname of the host running the nodemanager. Therefore,
#	yarn-site.xml should be host-dependent.
#-----------------------------------------------
for host in $slaves
do
    ssh -t $host $HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_YARN_HOME/etc/hadoop/${host} start nodemanager
done

#-----------------------------------------------
# Start Proxyserver on master
#-----------------------------------------------
ssh -t $master $HADOOP_YARN_HOME/sbin/yarn-daemon.sh start proxyserver

#-----------------------------------------------
# Start Historyserver on master
#-----------------------------------------------
ssh -t $master $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver

#-----------------------------------------------
# Check if everything has started
#-----------------------------------------------
echo "Java processes on ${master}:"
ssh -t $master jps
for host in $slaves
do
    echo "Java processes on ${host}:"
    ssh -t $host jps
done
