#!/bin/bash

cd "./logs/"
for numslides in `seq 1 90` 
do
	if [[ ( "$numslides" -eq 1 )|| ( "$numslides" -eq 10 ) || ( "$numslides" -eq 20 ) ||( "$numslides" -eq 30 ) || ( "$numslides" -eq 40 ) || ( "$numslides" -eq 50 ) ||  ( "$numslides" -eq 60 ) ||  ( "$numslides" -eq 70 ) ||  ( "$numslides" -eq 80 )  ||( "$numslides" -eq 90 ) ]]
	then	
		for numTiles in `seq 1 8`
		do 
			if [[ ( "$numTiles" -eq 1 ) || ( "$numTiles" -eq 2 ) || ( "$numTiles" -eq 4 ) ||( "$numTiles" -eq 8 ) ]]
				then
						cat Log_ZOrder_${numslides}_Slides_${numTiles}_Tiles.log| grep 'Time elapsed' | tail -100 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//' > tempZ.log
						tail -100 tempZ.log > Times_Log_ZOrder_${numslides}_Slides_${numTiles}_Tiles.log
						cat Log_RowOrder_${numslides}_Slides_${numTiles}_Tiles.log | grep 'Time elapsed' | tail -100 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//' > tempR.log
						tail -100 tempR.log > Times_Log_RowOrder_${numslides}_Slides_${numTiles}_Tiles.log
			fi
		done
	fi
done
