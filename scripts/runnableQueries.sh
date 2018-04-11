 PREF_SRC="../experiments/queries/RUN/"
 PREF_DEST="../experiments/queries/READY/"

 # TABLE_PREF="KUDB10.ROWORDER.parquet"
 TABLE_PREF="KUDB10"


 for file in $(ls ../experiments/queries/RUN/*_10*)
 do
 	INPUT="$file"

 	for option in "SINGLE_TABLE,ORC" "SINGLE_TABLE,PARQUET" "BIG_LOGICAL_TABLE,ORC" "BIG_LOGICAL_TABLE,PARQUET"
 	do
 		style=$(echo $option | cut -d',' -f1)
 		format=$(echo $option | cut -d',' -f2)
 		
 		if [ "$format" = "ORC" ]
 		then
 			lowf="orc"
 		else
 			lowf="parquet"
 		fi

 		fileName=$(echo "$file" | sed -e 's/..\/experiments\/queries\/RUN\///')

 		# Start with roworder
 		TABLE_DIR="/nidan/$lowf/$TABLE_PREF.ROWORDER.$lowf"
 		SCRIPT="$PREF_DEST/$fileName-$style-$format.scala"
 		echo "ARGS $INPUT $style $TABLE_DIR $format $SCRIPT"


 		./spark_query_table.sh $INPUT $style $TABLE_DIR $format $SCRIPT
 	done
 done

