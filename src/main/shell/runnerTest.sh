#!/bin/bash
# Program for executing the SPARK programs

# SPARK'S CONFIG
SPARK=spark-submit
MASTER=yarn-cluster
SPARK_CONF=--conf spark.akka.frameSize=1200 --conf spark.io.compression.codec=lzf

# DRIVER CONFIG
DRIVER_MEM=1g

# EXECUTOR CONFIG
EXEC_MEM=1g
EXEC_CORES=2

# CODE CONFIG
JAR_FILE=$1
CLASS=$2
CLASS_ARGS=${@:3}

# LOGS
LOG=log1.txt

echo ">> Configuration to be executed " 
echo "$JAR_FILE $CLASS $CLASS_ARGS"
echo " "

echo ">> Log created $LOG"
touch $LOG
echo "# " > $LOG

echo ">> Submiting work using $SPARK"
echo ">> Configuration is: $SPARK --master $MASTER $SPARK_CONF --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --executor-cores $EXEC_CORES $JAR_FILE --class $CLASS $CLASS_ARGS"
echo " "


echo ">> Submiting work using $SPARK" >> $LOG
echo ">> Configuration is: $SPARK --master $MASTER $SPARK_CONF --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --executor-cores $EXEC_CORES $JAR_FILE --class $CLASS $CLASS_ARGS" >> $LOG
echo " " >> $LOG

 
${SPARK} --master ${MASTER} ${SPARK_CONF} --driver-memory ${DRIVER_MEM} --executor-memory ${EXEC_MEM} --executor-cores ${EXEC_CORES} --class ${CLASS} ${JAR_FILE} ${CLASS_ARGS} 

#spark-submit --driver-memory 7g --class UnionRDDTrial  --master yarn-cluster --executor-memory 5g --executor-cores 15 

#/users/dl544/Nidan/NIDAN-Thesis/src/nidand-project-1.0.jar /users/dl544/imgSplit/ hdfs://128.110.152.140:9000/experiments/output.png

