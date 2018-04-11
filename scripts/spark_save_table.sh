#!/bin/bash
#$1 inputDir
#$2 outputDir
#$3 format
#$4 sorting
#$5 logfile (from the coordinatior)
source ./spark_configurations.sh

INPUT=$1
OUTPUT=$2
FORMAT=$3
SORTING=$4
LOG_FILE=$5

SPARK_PARAMS_FILE="spark-params.conf"
SPARK_CLI="spark-submit"

ROOT="$HOME/nidan.core/"
CLASS="--class nidan.main.MainSparkSQL"
JAR_PATH="$ROOT/target/scala-2.11/nidan.core-assembly-0.1.jar"
ARGS="-11 $INPUT $OUTPUT $FORMAT $SORTING"

# This function looks at spark_configurations.sh
# and retrieves the configurations
get_spark_conf()
{
	if [ $FORMAT = "ORC" ]
	then
		SPARK_CONF=$SPARK_SAVE_TABLE_ORC
	else
		SPARK_CONF=$SPARK_SAVE_TABLE_PARQUET
	fi

	# Generate the command to run
	echo -e "$SPARK_CONF" > $SPARK_PARAMS_FILE
	SPARK_CONF=""
	while IFS='' read -r line || [[ -n "$CONF" ]]; do

		# Ignore those lines that have a '#' symbol
		if [[ ! $line == *"#"* ]]
		then
			LINE="$(echo -e "${line}" | sed -e 's/^[[:space:]]*//')"
		    SPARK_CONF="$SPARK_CONF $LINE "
		fi
	done < "$SPARK_PARAMS_FILE"
}

echo ">> Setting Spark's configuration " 
echo ">> Setting Spark's configuration " >> $LOG_FILE

get_spark_conf
CMD="$SPARK_CLI $SPARK_CONF $CLASS $JAR_PATH $ARGS >> $LOG_FILE 2>&1"

echo ">> Spark's Command line ready"
echo ">> Spark's Command line ready" >> $LOG_FILE
echo "$CMD"
echo "$CMD" >> $LOG_FILE
eval $CMD

echo ">> spark_save_table.sh Finished"
echo ">> spark_save_table.sh Finished" >> $LOG_FILE