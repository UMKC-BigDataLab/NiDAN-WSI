#!/bin/bash

for numslides in `seq 1 90` 
do
	if [[ ( "$numslides" -eq 1 )|| ( "$numslides" -eq 10 ) || ( "$numslides" -eq 20 ) ||( "$numslides" -eq 30 ) || ( "$numslides" -eq 40 ) || ( "$numslides" -eq 50 ) ||  ( "$numslides" -eq 60 ) ||  ( "$numslides" -eq 70 ) ||  ( "$numslides" -eq 80 )  ||( "$numslides" -eq 90 ) ]]
	then	
		for numTiles in `seq 1 8`
		do 
			if [[ ( "$numTiles" -eq 1 ) || ( "$numTiles" -eq 2 ) || ( "$numTiles" -eq 4 ) ||( "$numTiles" -eq 8 ) ]]
			then
						echo "cat Log_ZOrder_${numslides}_Slides_${numTiles}_Tiles.log | grep 'Time elapsed' | tail -200 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'" >> temp_zorder.txt
						echo "cat Log_RowOrder_${numslides}_Slides_${numTiles}_Tiles.log | grep 'Time elapsed' | tail -200 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'" >> temp_roworder.txt
			fi
		done
	fi
done

