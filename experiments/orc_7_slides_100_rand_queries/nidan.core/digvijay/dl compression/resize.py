#!/usr/bin/bash python

import PIL
from PIL import Image

img = Image.open("/home/digvijayky/Desktop/dl/Slide 6.svs_3_1_0_0_0_2739_2368_0_0.png")
h_crop = int(img.size[0])%32
v_crop = int(img.size[1])%32
cropped_h_size = img.size[0] - h_crop
cropped_v_size = img.size[1] - v_crop
img = img.resize((1024, 768), PIL.Image.ANTIALIAS)
img.save("/home/digvijayky/Desktop/dl/rex.jpeg", subsampling=0, quality=90)
