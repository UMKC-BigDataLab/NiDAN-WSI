package nidan.io

case class NidanImage(
    patientId:Int, 
    imageId:Int, 
    format:Int,
    width:Int,
    height:Int,
    splits:Array[NidanImageSplit]
)
object NidanImage{
  def writeWholeImage() = ???
  def readWholeImage() = ???
  def assembleImage() = ???
  
  def writeSplittedImage() = ???
  def readSplittedImage() = ???
}