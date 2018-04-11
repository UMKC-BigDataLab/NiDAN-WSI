#!/bin/bash
# $1 input directory
# $2 output directory
# $3 old slide number
# $4 new slide number

# Ex:
# rename_slides /data/old/ ~/new/ 1 100
# Will copy /data/old/slide1 to ~/new/slide100 with all the data changed

INPUT_DIR=$1
OUTPUT_DIR=$2
OLD_SLIDE=$3
NEW_SLIDE=$4

# Copy the source to the destination
SOURCE="$INPUT_DIR/slide$OLD_SLIDE"
DEST="$OUTPUT_DIR/slide$NEW_SLIDE"
echo ">> Copy $SOURCE to $DEST"
cp -r $SOURCE $DEST

# Rename the files
for file in $(ls $DEST)
do
	tile=$(echo ${file#*svs_})
	NEW_NAME="Slide_${NEW_SLIDE}.svs_$tile"

	mv "$DEST/$file" "$DEST/$NEW_NAME"
done
FILES=$(ls $DEST | wc -l)
echo ">> Renamed $FILES files"
