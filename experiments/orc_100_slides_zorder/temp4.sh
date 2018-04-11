for numslides in `seq 1 100` 
do
	if [[ ( "$numslides" -eq 1 )|| ( "$numslides" -eq 10 ) || ( "$numslides" -eq 20 ) ||( "$numslides" -eq 30 ) || ( "$numslides" -eq 40 ) || ( "$numslides" -eq 50 ) ||  ( "$numslides" -eq 60 ) ||  ( "$numslides" -eq 70 ) ||  ( "$numslides" -eq 80 )  ||( "$numslides" -eq 90 )  ||( "$numslides" -eq 100 ) ]]
	then
		for numtiles in 1 2 4 8
		do 
			python query-generator.py 100 256 ${numtiles} ${numslides} queries_${numtiles}_${numslides}		
			echo "$(cat head.scala)" | tee "zqueriesToRun.scala" "rowqueriesToRun.scala"

			for queryNumber in `seq 1 100`
			do
				lines_select_1=$((${queryNumber}*3-(2)))
				lines_select_2=$((${queryNumber}*3-(1)))
				sed -n ${lines_select_1},${lines_select_2}p queries_${numtiles}_${numslides}.roworder > temp100queriesroworder.scala
				cat temp100queriesroworder.scala tail.scala >> "rowqueriesToRun.scala"
			done
			
			LOGR="./Log_RowOrder_${numslides}_Slides_${numtiles}_Tiles.log"
	
			echo "cat rowqueriesToRun.scala | ./initialize_spark_shell.sh > $LOGR 2>&1"
			cat rowqueriesToRun.scala | ./initialize_spark_shell.sh > $LOGR 2>&1
		done
	fi
done
