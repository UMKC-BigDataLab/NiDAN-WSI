#!/bin/bash
spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=16g

