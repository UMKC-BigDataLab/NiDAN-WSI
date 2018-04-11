for option in "ORC,ROWORDER" "ORC,ZORDER" "PARQUET,ROWORDER" "PARQUET,ZORDER" 
do 
	format=$(echo $option | cut -d',' -f1)
	sorting=$(echo $option | cut -d',' -f2)
	out=""
	
	if [ $format = "PARQUET" ]
	then 
		out="/nidan/parquet/KUDB10.$sorting.parquet"
	else 
		out="/nidan/orc/KUDB10.$sorting.orc"
	fi
	
	echo ">> Running: $format $sorting writing in $out"
	
	./experiment_coordinator.sh /proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/ /dev/data/ /data/ $out $format $sorting
	
	echo ">> Cleaning HDFS"
	hdfs dfs -rm -r -f /data/*

	echo ">> Cleaning /dev/data"
	rm -Rf /dev/data/slide*
done
