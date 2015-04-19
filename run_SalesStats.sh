#!/bin/sh -e

# Run jobs on Hadoop cluster
# Run this script on enmcomp7 (the host that run ResourceManager)

# Define the MapReduce class that contains the Map and Reduce tasks
#MRclass=MapRedSalesStats
#MRclass=MapRedMeanSales
MRclass=MapRedTotalSales

# Split the training data into a number of text files
cd $HADOOP_PREFIX/Workspace/MapReduce/matlab
./split_file.pl sales_figure.txt 16

# Compress the Map and Reduce classes into a jar file. This jar file is needed by bin/hadoop
cd $HADOOP_PREFIX/Workspace/MapReduce
jar cvf lib/parallel.jar -C bin/ .

# Copy training data and initial GMM model to HDFS
$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats; 
$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/mwmak/stats/input
$HADOOP_PREFIX/bin/hdfs dfs -rm -r /user/mwmak/stats/input
$HADOOP_PREFIX/bin/hdfs dfs -put $HADOOP_PREFIX/Workspace/MapReduce/matlab/input /user/mwmak/stats
sleep 1

# Run Hadoop job
$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats/output; sleep 1; cd $HADOOP_PREFIX/Workspace/MapReduce/bin; $HADOOP_PREFIX/bin/hadoop jar ../lib/parallel.jar parallel.stats.$MRclass /user/mwmak/stats/input /user/mwmak/stats/output; cd $HADOOP_PREFIX


