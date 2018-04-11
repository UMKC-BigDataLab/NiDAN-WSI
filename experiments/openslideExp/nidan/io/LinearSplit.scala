package nidan.io

import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import javax.imageio.ImageIO
import nidan.utils.NidanUtils.getDimensionsArray
import nidan.utils.NidanUtils.getHeightBounds
import nidan.utils.NidanUtils.getWidthBounds


// TODO Define the Splitter trait
trait NidanSplitter{
  
}

case class NidanRectangle(width:Int,
    height:Int,
    topX:Long,
    topY:Long,
    buttomX:Long,
    buttomY:Long
    )




case class NidanSplitItem(
	    index:Int,
	    rectangle:NidanRectangle,
	    rowCol:(Int,Int),
	    zindex:Int
	    )
    
case class SplitItem(
	    index:Int,
	    rectangle:Rectangle,
	    rowCol:(Int,Int),
	    XY1:(Int,Int),
	    XY2:(Int,Int),
	    zindex:Int
	    )

object LinearSplit {
  
  def getZIndexFromXY(row:Int, col:Int, pieces:Int):Int = {
    val i = f"${Integer.toBinaryString(row).toInt}%04d".toCharArray()
    val j = f"${Integer.toBinaryString(col).toInt}%04d".toCharArray()
    
    var index = 0
    val str = new StringBuilder()
    for (index <- 0 to i.length-1){
      str.append(i(index))
      str.append(j(index))
    }
    
    return Integer.parseInt(str.toString, 2)
  }
  
  def getZIndexFromXY(row:Int, col:Int, pieces:Long):Int = {
    val i = f"${Integer.toBinaryString(row).toInt}%064d".toCharArray()
    val j = f"${Integer.toBinaryString(col).toInt}%064d".toCharArray()
    
    var index = 0
    val str = new StringBuilder()
    for (index <- 0 to i.length-1){
      str.append(i(index))
      str.append(j(index))
    }
    
    return Integer.parseInt(str.toString, 2)
  }
  
  def getXYFromZIndex(zIndex:Int, pieces:Int):(Int,Int) ={
    val zaux = f"${Integer.toBinaryString(zIndex).toInt}%08d"
	  val z = zaux.toCharArray()
    var index = 0
    
    val strRow = new StringBuilder()
    val strCol = new StringBuilder()
	  
	  while(index < z.length-1){
	    strRow.append(z(index))
	    strCol.append(z(index+1))
	    
	    index = index + 2
	  }
	  
    val row = Integer.parseInt(strRow.toString, 2)
  		val col = Integer.parseInt(strCol.toString, 2)
    
    return (row,col)
  }
  
  def getIndexFromYX(row:Int, col:Int, pieces:Int):Int = {
    val arraySize = Math.sqrt(pieces).toInt
    val index = ((row * arraySize) + col) + 1
    
    return index
  }
  
  def getIndexFromYX(row:Int, col:Int, pieces:Long):Int = {
    val arraySize = Math.sqrt(pieces).toInt
    val index = ((row * arraySize) + col) + 1
    
    return index
  }
  
  def getXYFromIndex(index:Int, pieces:Int):(Int,Int) = {
    val rows, cols = Math.sqrt(pieces).toInt
    
    // Index is base 1
    val adjIndex = index-1
    
    val row = adjIndex / rows
    val col = adjIndex % rows
    
    return (row, col)
  }
  
  
	// TODO Change the following functions, to obtain only the rectangles reading will be done in spark 
  @throws(classOf[ClassNotFoundException])
	def readWhole(imagePath:String, pieces:Int, imageFormat:String, imageFormatID:Int):NidanImage = {
	  val arraySize = pieces / 2
	  val stream = ImageIO.createImageInputStream(new File(imagePath))
    val reader = ImageIO.getImageReaders(stream)
	  
    if(!reader.hasNext()) throw new ClassNotFoundException
	  
	  val imgReader = reader.next()
    imgReader.setInput(stream)
    val param = imgReader.getDefaultReadParam
    val width = imgReader.getWidth(imgReader.getMinIndex)
    val height = imgReader.getHeight(imgReader.getMinIndex)
    
	  println(s">> Reading image $imagePath as ($arraySize, $arraySize) matrix")
	  println(s">> Reading image dimensions ($width, $height)")
	  
    var col, row, index = 0
    val patientId, imageId = -1
    
    // 1:equi-size-width, 2:equi-size-height, 3:extra-width, 4:extra-height
    val gaps = getDimensionsArray(width, height, arraySize)
    val imageSplits:Array[NidanImageSplit] = Array()
    
    for(row <- 0 to arraySize-1){ 
    	  val hRect = getHeightBounds(row, arraySize, gaps)
    	  
    		for(col <- 0 to arraySize-1){
    		  index = getIndexFromYX(row, col, pieces)  
    		  val wRect = getWidthBounds(col, arraySize, gaps) 
    		  
    			println(s"LowCoorner: (${wRect._1} ${hRect._1}) MaxCorner: (${wRect._2} ${hRect._2})")
    			val region = new Rectangle(wRect._1, hRect._1, gaps(0), gaps(1))
    		  param.setSourceRegion(region)
    		  
    		  val buffer = imgReader.read(imgReader.getMinIndex, param)
    		  val arrayBuffer = new ByteArrayOutputStream()
    			ImageIO.write(buffer, imageFormat, arrayBuffer)
    			
    			imageSplits :+ NidanImageSplit(wRect._1, hRect._1, index, arrayBuffer.toByteArray)
    		}
    }
	  
	  NidanImage(patientId, imageId, imageFormatID, width, height, imageSplits)
	}
	
  
  
  
  
  
  def getData(img:(String,PortableDataStream), totalPieces:Int, imageFormat:String):RDD[NidanImageSplit] = {
    
    val stream = ImageIO.createImageInputStream(img._2.open())
    val reader = ImageIO.getImageReaders(stream)
    
    if(!reader.hasNext()) throw new ClassNotFoundException
    
    val imgReader = reader.next()  
    imgReader.setInput(stream)
    
    val width = imgReader.getWidth(imgReader.getMinIndex)
    val height = imgReader.getHeight(imgReader.getMinIndex)
    println(s">>> W: $width H: $height")
    
    val splits = getSplits(width, height, totalPieces)
      .map{case (id, rec) => (id, rec, img._2, imageFormat)}
    val RDDsplits = NidanContext.sparkContext.parallelize(splits)
    
    val imgSplits = RDDsplits.map{ imagePiece =>
      val id = imagePiece._1
      val rectangle = imagePiece._2
      val readerStream = imagePiece._3
      val format = imagePiece._4
      
      val streamM = ImageIO.createImageInputStream(readerStream.open())
      val readerS = ImageIO.getImageReaders(streamM).next
      
      val param = readerS.getDefaultReadParam
      param.setSourceRegion(rectangle)
      val buffer = readerS.read(readerS.getMinIndex, param)
      
  		  val arrayBuffer = new ByteArrayOutputStream()
  			ImageIO.write(buffer, format, arrayBuffer)
  			
  			NidanImageSplit(
  			    rectangle.getWidth.toInt, 
  			    rectangle.getHeight.toInt, 
  			    id, 
  			    arrayBuffer.toByteArray
      )
    }
    
    return imgSplits
    
  }
  
  def getSplitsWithRowCol(width:Int, height:Int, totalPieces:Int)
  :Array[(Int,Rectangle,Int,Int)] = {
	  val arraySize = Math.sqrt(totalPieces).toInt
    var col, row, index = 0
    
    // 1:equi-size-width, 2:equi-size-height, 3:extra-width, 4:extra-height
    val gaps = getDimensionsArray(width, height, arraySize)
    
    var splits = ArrayBuffer[(Int,Rectangle,Int,Int)]() 
    for(row <- 0 to arraySize-1){ 
    	  val hRect = getHeightBounds(row, arraySize, gaps)
    	  
    		for(col <- 0 to arraySize-1){
    		  index = getIndexFromYX(row, col, totalPieces)  
    		  val wRect = getWidthBounds(col, arraySize, gaps)
    		  val h = hRect._2 - hRect._1
    		  val w = wRect._2 - wRect._1
    		  
    		  splits += ((index, new Rectangle(wRect._1, hRect._1,w, h), row, col))
    		}
    }
	  
	  return splits.toArray
  }
  
	def getSplits(width:Int, height:Int, totalPieces:Int):Array[(Int,Rectangle)] = {
//	  val arraySize = totalPieces / 2
	  val arraySize = Math.sqrt(totalPieces).toInt
	  
	  
    var col, row, index = 0
    
    // 1:equi-size-width, 2:equi-size-height, 3:extra-width, 4:extra-height
    val gaps = getDimensionsArray(width, height, arraySize)
    
    var splits = ArrayBuffer[(Int,Rectangle)]() 
    for(row <- 0 to arraySize-1){ 
    	  val hRect = getHeightBounds(row, arraySize, gaps)
    	  
    		for(col <- 0 to arraySize-1){
    		  index = getIndexFromYX(row, col, totalPieces)  
    		  val wRect = getWidthBounds(col, arraySize, gaps)
    		  val h = hRect._2 - hRect._1
    		  val w = wRect._2 - wRect._1
    		  
    		  splits += ((index, new Rectangle(wRect._1, hRect._1,w, h)))
    		  println(s"LowCoorner: (${wRect._1} ${hRect._1}) MaxCorner: (${wRect._2} ${hRect._2})")
    		}
    }
	  
	  return splits.toArray
	}
  
	def getSplitItems(width:Int, height:Int, totalPieces:Int):Array[SplitItem] = {
	  val arraySize = Math.sqrt(totalPieces).toInt
    var col, row, index = 0
    
    // 1:equi-size-width, 2:equi-size-height, 3:extra-width, 4:extra-height
    val gaps = getDimensionsArray(width, height, arraySize)
    
    var splits = ArrayBuffer[(SplitItem)]() 
    for(row <- 0 to arraySize-1){ 
    	  val hRect = getHeightBounds(row, arraySize, gaps)
    	  
    		for(col <- 0 to arraySize-1){
    		  index = getIndexFromYX(row, col, totalPieces)
    		  val zindex = getZIndexFromXY(row, col, totalPieces)
    		  val wRect = getWidthBounds(col, arraySize, gaps)
    		  val h = hRect._2 - hRect._1
    		  val w = wRect._2 - wRect._1
    		  
    		  val item = SplitItem(
    		      index, new Rectangle(wRect._1, hRect._1,w, h),
    		      (row, col), (wRect._1, hRect._1),
    		      (wRect._2, hRect._2), zindex
    		  )
    		  
    		  splits += ((item))
    		  println(s"LowCoorner: (${wRect._1} ${hRect._1}) MaxCorner: (${wRect._2} ${hRect._2})")
    		}
    }
	  
	  return splits.toArray
	}
	
	
	
	
	// Defenitively function
		def getSplitItemsF(width:Long, height:Long, totalPieces:Long):Array[NidanSplitItem] = {
	  // TODO Change this to LONG
		val arraySize = Math.sqrt(totalPieces).toInt		
    var col, row, index = 0
    
    // 1:equi-size-width, 2:equi-size-height, 3:extra-width, 4:extra-height
    val gaps = getDimensionsArray(width, height, arraySize)
    
    var splits = new ArrayBuffer[(NidanSplitItem)]() 
    for(row <- 0 to arraySize-1){ 
    	  val hRect = getHeightBounds(row, arraySize, gaps)
    	  
    		for(col <- 0 to arraySize-1){
    		  index = getIndexFromYX(row, col, totalPieces)
    		  val zindex = getZIndexFromXY(row, col, totalPieces)
    		  val wRect = getWidthBounds(col, arraySize, gaps)
    		  val h = hRect._2 - hRect._1
    		  val w = wRect._2 - wRect._1
    		  
    		  val rectangle = new NidanRectangle(w.toInt, h.toInt, wRect._1, hRect._1, wRect._2, hRect._2)
    		  val item = new NidanSplitItem(index, rectangle, (row, col), zindex)
    		  splits.append((item))
    		}
    }
		
	  return splits.toArray
	}
	
	
	
	
  def splitImage(imagePath:String, widthSplits:Int, heightSplits:Int):NidanImage = ???
  def assembleImage(nidanImg:NidanImage):BufferedImage = ???
}