#!/bin/bash
spark-shell --master spark://ctl:7077 --driver-memory 50G  --executor-memory 45G  --executor-cores 8  --num-executors 16  --conf spark.sql.orc.filterPushdown=true --conf spark.io.compression.codec=lzf  --conf spark.rpc.message.maxSize=512  --conf spark.driver.maxResultSize=512M  --conf  spark.sql.parquet.compression.codec=uncompressed --conf spark.sql.broadcastTimeout=1200 --conf spark.network.timeout=700 --conf spark.executor.heartbeatInterval=60s --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" --conf spark.sql.autoBroadcastJoinThreshold=-1
