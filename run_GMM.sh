#!/bin/sh -e

nIters=1	# Set to one for development. You need at least 10 for convergency
nMix=256

# Run jobs on Hadoop cluster
# Run this script on enmcomp7 (the host that run ResourceManager)

# Split the training data into a number of text files
cd $HADOOP_PREFIX/Workspace/MapReduce/matlab
mkdir -p input
./split_file.pl input_data.txt 16


# Init the GMM by calling the sequential version with 0 iteration (i.e., without EM step)
cd $HADOOP_PREFIX/Workspace/MapReduce/bin
java sequential.gmm.GMM 60 $nMix 0 ../matlab/input_data.txt ../matlab/gmm.txt

# Compress the Map and Reduce classes into a jar file. This jar file is needed by bin/hadoop
cd $HADOOP_PREFIX/Workspace/MapReduce
jar cvf lib/parallel.jar -C bin/ .

# Copy training data and initial GMM model to HDFS
$HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats; 
$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /user/mwmak/stats/input
$HADOOP_PREFIX/bin/hdfs dfs -put $HADOOP_PREFIX/Workspace/MapReduce/matlab/input /user/mwmak/stats
$HADOOP_PREFIX/bin/hdfs dfs -put $HADOOP_PREFIX/Workspace/MapReduce/matlab/gmm.txt /user/mwmak/stats

sleep 1

for i in `seq $nIters`
do
  echo "Iteration $i"
  $HADOOP_PREFIX/bin/hdfs dfs -rm -r -f /user/mwmak/stats/output; 
  sleep 1; 
  cd $HADOOP_PREFIX/Workspace/MapReduce/bin; 
  time $HADOOP_PREFIX/bin/hadoop jar ../lib/parallel.jar parallel.gmm.MapRedGMM /user/mwmak/stats/input /user/mwmak/stats/output;
done


