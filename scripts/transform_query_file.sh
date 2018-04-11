#!/bin/bash
# $1 input
# $2 output

# Take a file with content like:
# val dataSource = "/nidan/orc/individualORC/slide1"
# val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 39 ", 1))
# 
# And produce:
# SLIDE QUERY
# 1		"SELECT imageBytes FROM data WHERE  partitionIndex = 39 "

INPUT=$1
OUTPUT=$2

echo -e "SLIDE \t QUERY"
# Use the number 7 to refer the file
exec 7< $INPUT
while read dataSource <&7
do
	read query <&7
	read garbage <&7

	cleanDS=$(echo "$dataSource" | sed -e 's/val dataSource = \"\/nidan\/orc\/individualORC\/slide//' | sed -e 's/\"//')
	cleanQ=$(echo "$query" | sed -e 's/val queries = List((\"//' | sed -e 's/\".*//')

	echo -e "$cleanDS,$cleanQ" >> $OUTPUT
done

# Close the file
exec 7<&-
echo ">> Finished with $INPUT"