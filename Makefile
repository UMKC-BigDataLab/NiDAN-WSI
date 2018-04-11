#!/bin/bash
# $1 Operation: commit pull
# $2 Comment to be added in the commit
#


BRANCH = debarron


push:
	git add --all
	git commit -m "Updating the repo"
	git push origin ${BRANCH}

pull:
	git pull origin ${BRANCH}

clean: 
	sbt clean

build: 
	sbt assembly	



#opt=$1
#
## PUSH
#if [[ $opt == "-push" ]]
#then
#  message=$2
#
#  git add --all
#  git commit -m "$message"
#  git push origin dev
#
## PULL
#elif [[ $opt == "-pull" ]]
#then
#  git pull origin dev
#
## CLEAN
#elif [[ $opt == "-clean" ]]
#then
#  sbt clean
#
## COMPILE AND CREATE THE JAR
#elif [[ $opt == "-cc" ]]
#then
#  sbt compile package
#
## GENERATE FAT JAR
#elif [[ $opt == "-assembly" ]]
#then
#  sbt assembly
#
## CLEAN THE HDFS CLUSTER
## NOTE: This should ALWAYS run on the master node
#elif [[ $opt == "-clean-hdfs" ]]
#then
#	startNode=$2
#	endNode=$3
#
#	echo ">> Stoping the cluster"
#	$HADOOP_HOME/sbin/stop-all.sh
#
#	echo ">> Cleaning master's hdfs data"
#	rm -Rf $HADOOP_HOME/hadoop_data/hdfs/namenode/*
#
#	echo ">> Cleaning the worker nodes"
#	for node in `seq $startNode $endNode`; do ssh cp-$node "rm -Rf /usr/local/hadoop/hadoop_data/hdfs/datanode/*"; done
#
#	echo ">> Format master node (namenode)"
#	hdfs namenode -format
#
#	echo ">> Start the cluster"
#	$HADOOP_HOME/sbin/start-all.sh
#fi
#
#
#echo "Work is done"
#
