This Eclipse project contains Java implementations of the following algorithms:

1. EM for GMM (both parallel and sequential)
2. Global mean vector (both parallel and sequential)
3. Word count

The folder matlab/ contains the scripts and functions to generate the example multi-dim data.

The root directory also contains shell scripts for running the parallel version of EM, global mean,
and word count on a Hadoop cluster.

To run the sequential version of EM and global mean, read the header of
MapReduce/sequential/gmm/GMM.java
MapReduce/sequential/gmm/OneMean.java

M.W. Mak
March 2015

----------------------
http://readwrite.com/2013/09/30/understanding-github-a-journey-for-beginners-part-1

http://readwrite.com/2013/10/02/github-for-beginners-part-2

Steps for Github



cd ~/so/java/hadoop/Workspace/MapReduce

git init

git config --global user.name "enmwmak"

git config --global user.email "enmwmak@polyu.edu.hk"

git add .

git commit -m "first commit" 

git remote add origin https://github.com/enmwmak/GMM-on-Hadoop.git

git push -u origin master



