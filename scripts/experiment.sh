#!/bin/bash
# $1 slide to append, e.g. slide1 or slide2 ..
# $2 database to append e.g. /nidan/orc/KUDB.Z10.orc

SPARK="spark-submit"                                                            

# The Spark's configuration file
echo -e '#--master local[*]
--master spark://ctl:7077
--driver-memory 50G
--executor-memory 55G
--executor-cores 8
--num-executors 14
--conf spark.io.compression.codec=lzf
--conf spark.rpc.message.maxSize=512
--conf spark.speculation=false
--conf spark.rdd.compress=true
--conf spark.shuffle.file.buffer=128m
--conf spark.memory.fraction=0.8
--conf spark.network.timeout=600s
--conf spark.driver.maxResultSize=1g
--conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=8"
--conf spark.executor.heartbeatInterval=60s
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=8"
--conf spark.kryoserializer.buffer.max=512m
--conf spark.kryoserializer.buffer=128m
# CONF SQL
--conf spark.sql.parquet.mergeSchema=true
--conf spark.sql.autoBroadcastJoinThreshold=-1
--conf spark.sql.broadcastTimeout=1200
--conf spark.sql.parquet.compression.codec=snappy
' > "test.txt"
                                                                                

CLASS="--class nidan.main.MainSparkSQL"
JAR_PATH="/users/dl544/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar"
ARGS="-11 /data/$1 $2 JPEG 1 ZINDEX PARTITIONED PARQUET"

# Generate the command to run
CMD="$SPARK "
while IFS='' read -r line || [[ -n "$CONF" ]]; do

	# Ignore those lines that have a '#' symbol
	if [[ ! $line == *"#"* ]]
	then
	    CMD="$CMD $line "
	fi
done < "test.txt"

CMD="$CMD $CLASS $JAR_PATH $ARGS"
echo "RUNNING => "
echo $CMD
eval $CMD
