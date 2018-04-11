#!/bin/bash

# A script to run the experiments in the project


EXP_NUM=$1
INPUT_IMAGE=$2
OUTPUT_DIR=$3
PARTITIONS=$4
IMAGE_LEVEL=$5

EXEC_MEMORY="15G"
NUM_EXEC="6"
EXEC_CORES="2"
MASTER="spark://kc-scer-98sgjk2:7077"

JAR_FILE="target/scala-2.11/nidan.core-assembly-0.1.jar"
SPARK_CONF='--class nidan.test.SparkTileGeneratorTest --executor-memory '$EXEC_MEMORY' --num-executors '$NUM_EXEC' --executor-cores '$EXEC_CORES' --master '$MASTER' --conf spark.io.compression.codec=lzf --conf spark.rpc.message.maxSize=1024 --conf spark.speculation=false --conf spark.rdd.compress=true --conf spark.shuffle.file.buffer=256m --conf spark.memory.fraction=0.8 --conf spark.network.timeout=600s --conf spark.driver.maxResultSize=2g --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=8" --conf spark.executor.heartbeatInterval=60s --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=8" --conf spark.kryoserializer.buffer.max=1024m --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.broadcastTimeout=1200 --conf spark.sql.orc.compression.codec=snappy'

echo "## NIDAN CORE"
echo ">> Input image: $INPUT_IMAGE"
echo ">> Output dir: $OUTPUT_DIR"
echo ">> Partitions: $PARTITIONS"
echo ">> Extracting level: $IMAGE_LEVEL"
echo -e "\n\n"

COMMAND=""
case "$EXP_NUM" in
    "spark") echo ">> Running the local experiment"
        COMMAND="spark-submit $SPARK_CONF $JAR_FILE $INPUT_IMAGE $OUTPUT_DIR $PARTITIONS $IMAGE_LEVEL"
        ;;
    "local") echo ">> Running the spark distributed experiment"
        COMMAND="java -classpath $JAR_FILE nidan.test.LocalTileGeneratorTest $INPUT_IMAGE $OUTPUT_DIR $PARTITIONS $IMAGE_LEVEL"
esac


echo "## NIDAN CORE"
echo ">> Running: $COMMAND"
#eval $COMMAND

echo ">> Finished"
