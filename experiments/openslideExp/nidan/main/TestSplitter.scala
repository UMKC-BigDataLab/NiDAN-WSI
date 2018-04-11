package nidan.main

import java.awt.image.DataBufferByte
import java.awt.image.DataBufferInt
import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.imaging.ImageFormats
import org.apache.commons.imaging.Imaging

import nidan.io.LinearSplit
import java.awt.image.BufferedImage
import java.io.FileOutputStream

object TestSplitter {
  def main(args: Array[String]): Unit = {
    val option = args(0)
    
    option match{
      case "-1" => main_Split(args)
      case "-2" => stitchImageFromPieces(args)
    }
    
    
  }
  
  def main_Split(args: Array[String]): Unit = {
    val input = args(1)
    val partitions = args(2).toInt
    val outputDir = args(3)
    val output = "genericImg.tiff"
    
    val img = Imaging.getBufferedImage(new File(input))
    val bytesOriginal = Imaging.writeImageToBytes(img, ImageFormats.TIFF, null)
//      img.getData.getDataBuffer.asInstanceOf[DataBufferByte].getData
    val w = img.getWidth
    val h = img.getHeight
    
    println(s">>> Original (${w},${h})")
    
    var dumInt = 0

    val splits = LinearSplit.getSplitItems(w, h, partitions)
    for(split <- splits){
      val auxOutput = s"$outputDir/$dumInt$output"
      
      // Are the dimensions the same?
      val width = split.rectangle.getWidth.toInt
      val height = split.rectangle.getHeight.toInt
      val xy1 = split.XY1
      val xy2 = split.XY2
      
      val buffImg = new BufferedImage(width, height, img.getType)
      val g = buffImg.getGraphics
      
      g.drawImage(img, 0, 0, 
        width, height, 
        xy1._1, xy1._2, xy2._1, xy2._2,  null
      )
      g.dispose()
      
      Imaging.writeImage(buffImg, new File(auxOutput), ImageFormats.TIFF, null)
      dumInt += 1
          
    }
  }
  
  case class Item(name:String, img:BufferedImage)
  
  def stitchImageFromPieces(args:Array[String]) = {
    val inputDir = args(1)
    val output = args(2)
    
    val imgArray = new ArrayBuffer[Item]()
    for(imgFile <- new File(inputDir).listFiles()){
      imgArray.append(Item(imgFile.getName, Imaging.getBufferedImage(imgFile)))
    }
    
    val partitions = imgArray.length
    val rowCol = Math.sqrt(imgArray.length).toInt
    val imgType = imgArray.last.img.getType
    var totalW = 0
    var totalH = 0
    for(img <- imgArray.toArray){
      totalW += img.img.getWidth.toInt
  		  totalH += img.img.getHeight.toInt
    }
    totalW /= rowCol
  		totalH /= rowCol
    
  		println(s">>> Image Size $totalW $totalH")
  		val finalImg = new BufferedImage(totalW, totalH, imgType)
    val gr = finalImg.createGraphics()
    
    val sortImg = imgArray.sortWith(_.name.split("gen")(0).toInt < _.name.split("gen")(0).toInt)
    
    var x, y = 0
  		for(img <- sortImg.toArray){
  		  val index = img.name.split("generic")(0).toInt
  		  val xy = LinearSplit.getXYFromIndex(index+1, partitions)
  		  println(s">>> Image ${img.name} Index: $index coord: (${xy._1} ${xy._2})")
  		 
  		  val wBuff = img.img.getWidth.toInt
  		  val hBuff = img.img.getHeight.toInt
  		  
  		  // A comment to change the file
  		  gr.drawImage(img.img, x, y, null)
  		  
  		  x = x + wBuff
  		  if( x >= finalImg.getWidth){
  		    x = 0
  		    y = hBuff
  		  }
  		}
    gr.dispose
    
    val out = new FileOutputStream(new File(output))
    Imaging.writeImage(finalImg, out, ImageFormats.TIFF, null)
    out.close()
    
  }
  
//  finalImg.createGraphics().drawImage
//  (buffImages[num], chunkWidth * j, chunkHeight * i, null);
//                num++;
  
  
//  gr.drawImage(image, 0, 0, 
//      chunkWidth, chunkHeight, 
//      chunkWidth * y, chunkHeight * x, 
//      chunkWidth * y + chunkWidth, 
//      chunkHeight * x + chunkHeight, 
//      null);
//                gr.dispose();
//  
  
  
  /*
   * finalImg.createGraphics().
   * drawImage(buffImages[num], chunkWidth * j, 
   * chunkHeight * i, null);
                num++;
   * */
  
  
  
  def testZcoord(args: Array[String]): Unit = {
    val pieces = 16
    val indeces = new ArrayBuffer[Int]()
    val zIndeces = new ArrayBuffer[Int]()
    val coords = Array(
                  (0,0),(0,1),(0,2),(0,3),
                  (1,0),(1,1),(1,2),(1,3),
                  (2,0),(2,1),(2,2),(2,3),
                  (3,0),(3,1),(3,2),(3,3)
                  )
                  
    println("\n\nTest of the function getIndexFromXY")
    for(coord <- coords){
      val index = LinearSplit.getIndexFromYX(coord._1, coord._2, pieces)
      indeces.append(index)
      
      println(s"[${coord._1}, ${coord._2}] => $index")
    }
    
    println("\n\nTest of the function getXYFromIndex")
    for(index <- indeces.toArray){
      val coord = LinearSplit.getXYFromIndex(index, pieces)
      
      println(s"[${coord._1}, ${coord._2}] => $index")
    }
    
    println("\n\nTest of the function getZIndexFromXY")
    for(coord <- coords){
      val zindex = LinearSplit.getZIndexFromXY(coord._1, coord._2, pieces)
      zIndeces.append(zindex)
      
      println(s"[${coord._1}, ${coord._2}] => $zindex")
    }
    
    println("\n\nTest of the function getXYFromZIndex")
    for(zi <- zIndeces.toArray){
      val coord = LinearSplit.getXYFromZIndex(zi, pieces)
      println(s"[${coord._1}, ${coord._2}] => $zi")
    }
  }
}