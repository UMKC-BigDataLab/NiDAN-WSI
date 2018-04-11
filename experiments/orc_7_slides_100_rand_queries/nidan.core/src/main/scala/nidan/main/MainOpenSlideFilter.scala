package nidan.main

import java.io.File
import org.openslide.OpenSlide
import java.io.IOException
import java.awt.image.BufferedImage
import java.awt.image.DataBufferInt
import org.apache.commons.imaging.Imaging
import org.apache.commons.imaging.ImageFormats

object MainOpenSlideFilter {
  def main(args: Array[String]): Unit = {
    writeSingleTIFFFromLevel(args)
  }
  
  def writeSingleTIFFFromLevel(args:Array[String]) = {
    val inputFile = args(0)
//    val level = args(1).toInt
    
    val OS = new OpenSlide(new File(inputFile))
    
    val levels = Seq(2, 3, 4)
    
    for(level <- levels){
      val width = OS.getLevelWidth(level)
      val height = OS.getLevelHeight(level)
      println(s"Level $level W $width H $height")
      
      println("Creating the Buffered Image")
      val buff = new BufferedImage(width.toInt, height.toInt, BufferedImage.TYPE_INT_ARGB)
      val g = buff.getGraphics
      val array = buff.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
      
      println("Painting the image")
      OS.paintRegionARGB(array, 0L, 0L, level, width.toInt, height.toInt)
      g.drawImage(buff, 0, 0, width.toInt, height.toInt, null)
      Imaging.writeImage(buff, new File(s"./Result_$level.TIFF"), ImageFormats.TIFF, null)
      
      println("Image was saved")
    }
    OS.close
    
  }
  
  def printOSLevels(args:Array[String]) = {
    val inputDir = new File(args(0))
    
    // Get the files in the Directory
    println(s"#DEBUG Filter files")
    for(file <- inputDir.listFiles){
      try{
          val os = new OpenSlide(file)
          println(s"\t >>> ${file.getName} HAS ${os.getLevelCount} LEVELS")
        
      }catch{
        case io:IOException => println(s"#ERROR [Invalid File] ${file.getName}")
        case e:Exception => 
          println(s"#ERROR ")
          e.printStackTrace()
      }
   } 
  }
}