#!/bin/bash
# $1 base dir wich contains 2x2 4x4 8x8 16x16 32x32
# $2 f256 f1024 and so on, just to help out the output

baseDir=$1
fileSize=$2
partition="Y"
logApp=$3


dirArray=("2x2" "4x4" "8x8" "16x16" "32x32")
dimArray=(4 16 64 256 1024)

jarFile="nidand-project-1.0.jar"
class="UnionRDDTrial"
restart="./restartCluster.sh"

## CLEAN HADOOP
hadoop dfsadmin -safemode leave
hadoop dfs -rm -R -f /exp

for index in 0 1 2 3 4
do
	## -----------------------------------------------------
	## COLD
	## -----------------------------------------------------
	hadoop dfsadmin -safemode leave
	output="/exp/$fileSize/${dirArray[$index]}/$execution-outputCS.jpg"
	arguments="$baseDir/${dirArray[$index]} $output" 
	arguments="$arguments $partition ${dimArray[$index]}"
	sparkArgs="./runner.sh $jarFile $class $arguments"

	echo ">> Running COLD START"
	echo ">> Restarting services"
	eval $restart
	eval $sparkArgs

	echo " "
	echo ">> Copyin log"
	echo "##" >> "logCS-$partition-$fileSize-${dimArray[$index]}.txt"
	cat "log1.txt" >> "logCS-$partition-$fileSize-${dimArray[$index]}.txt"


	## -----------------------------------------------------
	## WARM
	## -----------------------------------------------------
	echo " "
	logWS="logWS-$partition-$fileSize-${dimArray[$index]}.txt"
	echo ">> Running WARM START"
	echo '##' > $logWS
	for execution in 1 2 3
	do

    	echo " "
		echo ">> Cleaning the HDFS "
    	hadoop dfsadmin -safemode leave
    	hadoop dfs -rm -R /exp

	    echo " "
	    output="/exp/$fileSize/${dirArray[$index]}/$execution-outputWS.jpg"
	  	arguments="$baseDir/${dirArray[$index]} $output" 
		arguments="$arguments $partition ${dimArray[$index]}"
		sparkArgs="./runner.sh $jarFile $class $arguments"

	    # It needs to flush hdfs so we can store new results
	    # Also, we need to move the log files to the big directory
	    # Since HDFS is stored in /

	    eval $sparkArgs
		echo ">> Copyin log"
		echo '##' >> $logWS
		cat "log1.txt" >> $logWS
	done ## FINISHED WARM START

	# COPY LOGS
	echo ">> Copying the execution logs"
	hadoop dfs -copyToLocal /tmp/logs/dl544/logs "LOG-${dimArray[$index]}"

done ## FINISHED THE EXPERIMENTS

echo ">> Spark magic finished "

echo ">> Copying logs "
#mkdir $logApp
#hadoop dfs -copyToLocal /tmp/logs/dl544/logs/ $logApp

echo ">> Deleting logs"
#hadoop dfs -rm -R -f /tmp/logs/dl544/logs/

echo ">> Experiments finished "

