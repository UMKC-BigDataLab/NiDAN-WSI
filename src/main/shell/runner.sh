#!/bin/bash
# Program for executing the SPARK programs
# UMKC, March 2016
# Daniel Lopez, dl544@mail.umkc.edu

APP_NAME="--name \"NIDAN-Project\""

# SPARK'S CONFIG
SPARK=spark-submit
SPARK_CONF_AKKA="--conf spark.akka.frameSize=1200" 
SPARK_CONF_COMPRESSION="--conf spark.io.compression.codec=lzf"
SPARK_CONF_RESULTS="--conf spark.driver.maxResultSize=0"
SPARK_CONF_PARALLEL="--conf spark.default.parallelism=8"


# EXTRA CONFIG
MASTER="--master yarn-cluster"
#MASTER="--master local"
DRIVER_MEM="--driver-memory 7g"
EXEC_MEM="--executor-memory 6g"
DRIVER_MEM="--driver-memory 20g"
EXEC_MEM="--executor-memory 20g"
EXEC_CORES="--executor-cores 2"
EXEC_NUM="--num-executors 2"

# CODE CONFIG
JAR_FILE="$1"
CLASS="--class $2"
CLASS_ARGS="${@:3}"

LOG=log1.txt

RUNNER="$SPARK \
  $APP_NAME \
  $DRIVER_MEM \
  $CLASS \
  $MASTER \
  $EXEC_MEM \
  $EXEC_CORES \
  $EXEC_NUM \
  $SPARK_CONF_AKKA \
  $SPARK_CONF_COMPRESSION \
  $SPARK_CONF_RESULTS \
  $SPARK_CONF_PARALLEL \
  $JAR_FILE \
  $CLASS_ARGS"


echo " "
echo ">> Submiting "
echo $RUNNER
echo " " > $LOG 
echo " " 
eval $RUNNER > $LOG


#echo ">> Configuration to be executed " 
#echo "$JAR_FILE $CLASS $CLASS_ARGS"
#echo " "

#echo ">> Log created $LOG"
#touch $LOG
#echo "# " > $LOG

#echo ">> Submiting work using $SPARK"
#echo ">> Configuration is: $SPARK --master $MASTER $SPARK_CONF --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --executor-cores $EXEC_CORES $JAR_FILE --class $CLASS $CLASS_ARGS"
#echo " "

#echo ">> Submiting work using $SPARK" >> $LOG
#echo ">> Configuration is: $SPARK --master $MASTER $SPARK_CONF --driver-memory $DRIVER_MEM --executor-memory $EXEC_MEM --executor-cores $EXEC_CORES $JAR_FILE --class $CLASS $CLASS_ARGS" >> $LOG
#echo " " >> $LOG

#${SPARK} --master ${MASTER} ${SPARK_CONF} --driver-memory ${DRIVER_MEM} --executor-memory ${EXEC_MEM} --executor-cores ${EXEC_CORES} --class ${CLASS} ${JAR_FILE} ${CLASS_ARGS} 
#spark-submit --driver-memory 7g --class UnionRDDTrial  --master yarn-cluster --executor-memory 5g --executor-cores 15 
#/users/dl544/Nidan/NIDAN-Thesis/src/nidand-project-1.0.jar /users/dl544/imgSplit/ hdfs://128.110.152.140:9000/experiments/output.png

