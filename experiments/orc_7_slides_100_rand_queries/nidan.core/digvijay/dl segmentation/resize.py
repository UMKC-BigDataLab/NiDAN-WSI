#!/usr/bin/bash python

import PIL
import glob
import os
from PIL import Image
for tile in glob.glob(os.path.join(os.path.dirname(__file__), "originalTilesSlide1/*.jpeg")):
	img = Image.open(tile)
	img = img.resize((224, 224), PIL.Image.ANTIALIAS)
	with open(os.path.join(os.path.dirname(__file__), "inputTiles224/"+os.path.basename(tile)), "wb") as resizedtile:
		img.save(resizedtile, subsampling=0, quality=100)
