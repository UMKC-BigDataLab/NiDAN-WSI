#!/bin/bash

ROOT="../../../"
LOG_PREFIX="$ROOT/logs/"
QUERIES_PREFIX="$ROOT/experiments/queries/ORC/"
RUN_PREFIX="$ROOT/experiments/queries/RUN/"

PY_PROCESS="../python/queries.py"
SPARK="spark-shell --master spark://ctl:7077 --driver-memory 50G  --executor-memory 45G  --executor-cores 8  --num-executors 16  --conf spark.sql.orc.filterPushdown=true --conf spark.io.compression.codec=lzf  --conf spark.rpc.message.maxSize=512  --conf spark.driver.maxResultSize=512M  --conf  spark.sql.parquet.compression.codec=uncompressed --conf spark.sql.broadcastTimeout=1200 --conf spark.network.timeout=700 --conf spark.executor.heartbeatInterval=60s --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" --conf spark.sql.autoBroadcastJoinThreshold=-1"

echo ">> Generating the scripts from file: $QUERIES_PREFIX"
for FILE in $(ls $QUERIES_PREFIX)
do
	input="$QUERIES_PREFIX/$FILE"
	output="$RUN_PREFIX/Q_$FILE.scala"

	echo -e "\t - Processing: $FILE Output: $output"
	python $PY_PROCESS $input $output
done
echo " "

echo ">> Processing all the runnable queries"
for QUERY in $(ls $RUN_PREFIX)
do
	LOG_FILE="$LOG_PREFIX/LOG_$QUERY"
	CMD="cat $RUN_PREFIX/$QUERY | $SPARK > $LOG_FILE 2>&1"

	echo -e "\t - Running: $CMD"
	eval $CMD
done
echo " "

echo "# DONE"
