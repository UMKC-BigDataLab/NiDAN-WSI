#!/bin/bash

PREFIX="/proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/"

for slideIter in `seq 1 100`
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
			mv $f $PREFIX/$experimentSlide/$newTile
		done
		ls $PREFIX/$experimentSlide/*.JPEG > $renamedSlideList
	else
		experimentSlide="slide$slideIter"
	fi

	echo ">> Append the slides to orc files, Running the command: "
	echo "time spark-submit  --name Nidan  --master local[*] --driver-memory 30G  --executor-memory 30G  --executor-cores 8  --num-executors 8  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL ~/orcTrialInfinity/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar  -11 /proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/  /nidan/orc/individualORC/$experimentSlide JPEG 1 ZINDEX GROUP"

	time spark-submit  --name Nidan  --master local[*] --driver-memory 30G  --executor-memory 30G  --executor-cores 8  --num-executors 8  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL ~/orcTrialInfinity/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar  -11 /proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/$experimentSlide  /nidan/orc/individualORC/$experimentSlide JPEG 1 ZINDEX GROUP

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
