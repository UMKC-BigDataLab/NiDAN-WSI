package nidan.io

import java.awt.image.BufferedImage


trait NidanImageSplitter {
  def splitImage(imagePath:String, widthSplits:Int, heightSplits:Int):NidanImage
  def readWhole(imagePath:String, widthSplits:Int, heightSplits:Int)
  def assembleImage(nidanImg:NidanImage):BufferedImage

  // TODO Read a split image
}