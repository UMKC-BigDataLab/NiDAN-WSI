#/bin/bash

# TODO Get rid of these constants
HDFS_BIN="/usr/local/hadoop/bin/"
HDFS_INPUT="/data/svs"
LOCAL_OUTPUT="/users/dl544/local/data"

while read FILE; do
    if [ ! -f "$FILE" ]
    then
        # Download the file from HDFS into local storage
        CMD="$HDFS_BIN/hdfs dfs -get '$HDFS_INPUT'/'$FILE' '$LOCAL_OUTPUT'/'$FILE'"
        eval $CMD
        echo "$FILE 0"
    else
        # The file is already here, do nothing
        echo "$FILE 1"
    fi
done
