#!/bin/bash

cd "./logs/"
for slideIter in `seq 1 7`
do	
	if [[ ( "$slideIter" -le 7 ) ]]
	then	
		for numTiles in `seq 1 8`
		do 
			if [[ ( "$numTiles" -eq 1 ) || ( "$numTiles" -eq 2 ) || ( "$numTiles" -eq 4 ) ||( "$numTiles" -eq 8 ) ]]
				then
					for ITER in 1 2 3
					do
						cat Log_ZOrder_${numTiles}_Tiles_${slideIter}_${ITER}.log | grep 'Time elapsed' | tail -100 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//' > tempZ.log
						tail -100 tempZ.log > Times_Log_ZOrder_${numTiles}_Tiles_${slideIter}_${ITER}.log
						
						cat  Log_RowOrder_${numTiles}_Tiles_${slideIter}_${ITER}.log | grep 'Time elapsed' | tail -100 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//' > tempR.log
						tail -100 tempR.log > Times_Log_RowOrder_${numTiles}_Tiles_${slideIter}_${ITER}.log
					done
			fi
		done
	fi
done
