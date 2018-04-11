SPARK_SAVE_TABLE_ORC='--master spark://ctl:7077 
		--driver-memory 50G 
		--executor-memory 20G 
		--executor-cores 8  
		--num-executors 14 
		--conf spark.sql.parquet.compression.codec=snappy 
		--conf spark.io.compression.codec=lzf 
		--conf spark.rpc.message.maxSize=512 
		--conf spark.driver.maxResultSize=512M 
		--conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=8 -XX:+UseCompressedOops" 
		--conf spark.sql.broadcastTimeout=1200 
		--conf spark.network.timeout=700 
		--conf spark.executor.heartbeatInterval=60s 
		--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=8 -XX:+UseCompressedOops" 
		--conf spark.sql.autoBroadcastJoinThreshold=-1 
		--conf spark.kryoserializer.buffer.max=1024m 
		--conf spark.kryoserializer.buffer=128m 
		--conf spark.speculation=false 
		--conf spark.sql.tungsten.enabled=true 
		--conf spark.rdd.compress=false 
		--conf spark.suffle.compress=true 
		--conf spark.sql.shuffle.partitions=2000 
		--conf spark.shuffle.consolidateFiles=true 
		'

SPARK_SAVE_TABLE_PARQUET='#--master local[*]
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
		'

SPARK_QUERY_TABLE='--master spark://ctl:7077 --driver-memory 50G  
		--executor-memory 45G  
		--executor-cores 8  
		--num-executors 16  
		--conf spark.sql.orc.filterPushdown=true --conf spark.io.compression.codec=lzf  
		--conf spark.rpc.message.maxSize=512  
		--conf spark.driver.maxResultSize=512M  
		--conf spark.sql.parquet.compression.codec=uncompressed 
		--conf spark.sql.broadcastTimeout=1200 
		--conf spark.network.timeout=700 
		--conf spark.executor.heartbeatInterval=60s 
		--conf spark.driver.extraJavaOptions="-XX:+UseG1GC" 
		--conf spark.executor.extraJavaOptions="-XX:+UseG1GC" 
		--conf spark.sql.autoBroadcastJoinThreshold=-1
		'