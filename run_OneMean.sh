#!/bin/sh -ex


# Run jobs on Hadoop cluster
# Run this script on enmcomp7 (the host that run ResourceManager)

cd $HADOOP_PREFIX/Workspace/MapReduce/matlab
./split_file.pl sales_figure.txt 32

cd $HADOOP_PREFIX/Workspace/MapReduce
jar cvf lib/parallel.jar -C bin/ .

$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/mwmak/stats/input
$HADOOP_PREFIX/bin/hdfs dfs -rm -r /user/mwmak/stats/input
$HADOOP_PREFIX/bin/hdfs dfs -put $HADOOP_PREFIX/Workspace/MapReduce/matlab/input /user/mwmak/stats
sleep 1

#$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats/output; sleep 1; cd $HADOOP_PREFIX/Workspace/MapReduce/bin; $HADOOP_PREFIX/bin/hadoop jar ../lib/parallel.jar parallel.stats.MapRedSalesStats /user/mwmak/stats/input /user/mwmak/stats/output; cd $HADOOP_PREFIX
#$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats/output; sleep 1; cd $HADOOP_PREFIX/Workspace/MapReduce/bin; time $HADOOP_PREFIX/bin/hadoop jar ../lib/parallel.jar parallel.wordcount.WordCount /user/mwmak/stats/input /user/mwmak/stats/output; cd $HADOOP_PREFIX
#$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats/output; sleep 1; cd $HADOOP_PREFIX/Workspace/MapReduce/bin; time $HADOOP_PREFIX/bin/hadoop jar ../lib/parallel.jar parallel.wordcount.WordCountInMapperComb /user/mwmak/stats/input /user/mwmak/stats/output; cd $HADOOP_PREFIX
$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats/output; sleep 1; cd $HADOOP_PREFIX/Workspace/MapReduce/bin; time $HADOOP_PREFIX/bin/hadoop jar ../lib/parallel.jar parallel.gmm.MapRedOneMean /user/mwmak/stats/input /user/mwmak/stats/output;


