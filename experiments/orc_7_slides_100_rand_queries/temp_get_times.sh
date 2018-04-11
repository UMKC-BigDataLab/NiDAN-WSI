#!/bin/bash

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
						echo "tail -100 Times_Log_ZOrder_${numTiles}_Tiles_${slideIter}_${ITER}.log" >> temp_zorder.txt
						echo "tail -100 Times_Log_RowOrder_${numTiles}_Tiles_${slideIter}_${ITER}.log" >> temp_roworder.txt
					done
			fi
		done
	fi
done
