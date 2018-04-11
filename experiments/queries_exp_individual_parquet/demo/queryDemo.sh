#!/bin/bash

LOG="./Log_$1.log"

old="val dataSource.*"
new="val dataSource = \"\/nidan\/parquet\/slide$1.prqt\""

sed -e "s/$old/$new/" -i demoQueriesToRun.scala

oldSlideNo="val currentSlide.*"
newSlideNo="val currentSlide = \"$1\""
sed -e "s/$oldSlideNo/$newSlideNo/" -i demoQueriesToRun.scala

echo "$ITER / 3, cat demoQueriesToRun.scala | ./initialize_spark_shell.sh > $LOG 2>&1"
cat demoQueriesToRun.scala | ./initialize_spark_shell.sh > $LOG 2>&1
cat Log_$1.log | grep 'Time elapsed' | tail -4 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'