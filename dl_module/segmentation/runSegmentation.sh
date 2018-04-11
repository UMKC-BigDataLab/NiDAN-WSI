#!/bin/bash
Images=/home/digvijayky/Desktop/deepmask/inputTiles224/*
for i in $Images
do 
	echo "Processing $i file..."
	/home/digvijayky/torch/install/bin/th computeProposals.lua /home/digvijayky/Desktop/deepmask/pretrained/sharpmask -img "$i"
	mv "res.jpg" "${i##*inputTiles224/}"
done