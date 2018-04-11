#!/bin/bash

PREFIX="/proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/"
#PREFIX="/home/digvijayky/Documents/256tiles"

#for slideIter in `seq 1 90`
for slideIter in `seq 1 90`
do	
	localSlideNo=`echo "$slideIter % 7" | bc`
	if [ "$localSlideNo" -eq 0 ]
	then
			localSlideNo=7
	fi
	
	localSlideName="slide$localSlideNo"
	experimentSlide="slide$slideIter"
	slideList="./slideList.txt"
	renamedSlideList="./renamedSlideList.txt"

	if [ "$slideIter" -gt 7 ]
	then
		echo ">> Changing the name of the Slides, from $localSlideName to $experimentSlide"
		mv $PREFIX/$localSlideName $PREFIX/$experimentSlide
		ls $PREFIX/$experimentSlide/*.JPEG > $slideList

		echo ">> Changing the name of all the individual files in $experimentSlide"
		for f in $(cat $slideList)
		do
			tile=$(echo ${f#*svs_})
			newTile="Slide_${slideIter}.svs_$tile"

			# Too much output, lets ignore it
			#echo ">>Command: mv $PREFIX/$f $PREFIX/$newTile"
			# mv $PREFIX/$experimentSlide/$f $PREFIX/$experimentSlide/$newTile
			mv $f $PREFIX/$experimentSlide/$newTile
		done
		ls $PREFIX/$experimentSlide/*.JPEG > $renamedSlideList
	else
		experimentSlide="$localSlideName"
	fi

	echo ">> Append the slides to orc files, Running the command: "
	echo "time spark-submit  --name Nidan  --master local[*] --driver-memory 30G  --executor-memory 30G  --executor-cores 8  --num-executors 8  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL ~/orcFormat/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar  -11 /proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/  /nidan/orc/allSlides JPEG 1 ZINDEX GROUP"

	time spark-submit  --name Nidan  --master local[*] --driver-memory 30G  --executor-memory 30G  --executor-cores 8  --num-executors 8  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL ~/orcFormat/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar  -11 /proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/  /nidan/orc/allSlides/ JPEG 1 ZINDEX GROUP

	# time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL ~/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar  -11 $PREFIX/$experimentSlide  /nidan/parquet/Slides-uncompressed-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP
	
	# Check if we need to run the query or just append data
	if [[ ( "$slideIter" -le 5 ) || ( "$slideIter" -eq 10 ) || ( "$slideIter" -eq 20 ) ||( "$slideIter" -eq 30 ) || ( "$slideIter" -eq 40 ) || ( "$slideIter" -eq 50 ) ||  ( "$slideIter" -eq 60 ) ||  ( "$slideIter" -eq 70 ) ||  ( "$slideIter" -eq 80 )  ||( "$slideIter" -eq 90 ) ]]
	then	
		
		# Run the experiment 3 times
		for ITER in 1 2 3
		do
			LOG="./Log_${slideIter}_${ITER}.log"
			
			# old="val dataSource.*"
			# new="val dataSource = \"\/nidan\/orc\/allSlides\/imageId=${slideIter}.svs\""			
			# sed -e "s/$old/$new/" -i queriesToRun.scala

			echo "$ITER / 3, cat queriesToRun.scala | ./initialize_spark_shell.sh > $LOG 2>&1"
			cat queriesToRun.scala | ./initialize_spark_shell.sh > $LOG 2>&1	
			hdfs dfs -du -h /nidan/orc/allSlides >> $LOG 2>&1 			

			echo ">> Removing JPEGS"
			rm *.JPEG	
		done
	fi
	

	# rm -rf $slideIter/
	# Instead of delete them from the disk, we can rename them back to $localSlideNo
	
	if [ "$slideIter" -gt 7 ]
	then
		mv $PREFIX/$experimentSlide $PREFIX/$localSlideName
		ls $PREFIX/$localSlideName > $slideList

		echo ">> Changing the name of all the individual files in $localSlideName"
		for f in $(cat $slideList)
		do
			tile=$(echo ${f#*svs_})
			newTile="Slide_${localSlideNo}.svs_$tile"

			# Too much output, lets ignore it
			#echo ">>Command: mv $PREFIX/$f $PREFIX/$newTile"
			mv $PREFIX/$localSlideName/$f $PREFIX/$localSlideName/$newTile
		done
	fi
done