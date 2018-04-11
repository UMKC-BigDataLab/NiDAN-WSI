#!/bin/bash
# $1 inputDir where the slides are (local)
# $2 outputDir where to copy the slides (local)
# $3 hdfs output dir
# $4 table dir
# $5 format
# $6 sorting

INPUT=$1
OUTPUT=$2
HDFS_DIR=$3
TABLE_DIR=$4
FORMAT=$5
SORTING=$6


ROOT="$HOME/nidan.core/"
LOG_DIR="$ROOT/logs/"

# Hack to generate the 
# log with timestamp
NOW_CMD=$(date +"%m-%d-%Y-%H-%M-%S")
TEMP="$ROOT/logs/.tempFile"
echo "$NOW_CMD" > $TEMP
NOW=$(head $TEMP)


for SLIDE_I in `seq 1 10`
do
	LOG_FILE="$LOG_DIR/nidan.saveslide-$SLIDE_I.$NOW.log"

	# Copy the data to /dev/data
	localSlideNo=`echo "$SLIDE_I % 7" | bc`
	if [ "$localSlideNo" -eq 0 ]
	then
			localSlideNo=7

			echo ">> Cleaning HDFS: $HDFS_DIR"
			hdfs dfs -rm -r -f "$HDFS_DIR/*"
	fi
	./rename_slides.sh $INPUT $OUTPUT $localSlideNo $SLIDE_I >> $LOG_FILE 2>&1

	# Copy to HDFS
	echo ">> Copy to HDFS: hdfs dfs -copyFromLocal $OUTPUT/slide$SLIDE_I $HDFS_DIR" 
	echo ">> Copy to HDFS: hdfs dfs -copyFromLocal $OUTPUT/slide$SLIDE_I $HDFS_DIR" >> $LOG_FILE
	hdfs dfs -copyFromLocal $OUTPUT/slide$SLIDE_I $HDFS_DIR

	# Append data to 
	HDFS_INPUT="$HDFS_DIR/slide$SLIDE_I"
	echo ">> Creating table: $TABLE_DIR in SparkSQL"
	echo ">> Creating table: $TABLE_DIR in SparkSQL" >> LOG_DIR
	./spark_save_table.sh $HDFS_INPUT $TABLE_DIR $FORMAT $SORTING $LOG_FILE

	echo ">> Cleaning temporal directory: $OUTPUT"
	rm -Rf "$OUTPUT/*"
done