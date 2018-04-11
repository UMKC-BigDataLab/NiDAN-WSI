package nidan.main

import java.io.FileOutputStream
import java.io.BufferedInputStream
import javax.imageio.ImageIO
import nidan.io.LinearSplit
import org.apache.hadoop.io.BytesWritable
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.LogManager
import java.io.File
import nidan.io.NidanImageSplit
import java.io.ByteArrayOutputStream
import nidan.io.NidanContext._

object MainSpark {
  lazy val log = LogManager.getRootLogger
  
  def main(args: Array[String]): Unit = {
    val option = args(0)
    
    option match {
      case "-RSpHdfs-SHdfs" => readSplitsHDFS_SaveHDFS(args)
      case "-RSeqHDFS-Pi" => readSequence_PrintInfo(args)
      case "-RHdfs-SL" => readSequence_SaveImage(args)
      case "-RFHdfs-SHdfs" => fileReadSplitsHDFS_SaveHDFS(args)
      
      case _ => println("He needs some milk")
    }
  }
  
  def readFromStream(stream:BufferedInputStream):Array[Byte]={
    val temp = new ArrayBuffer[Byte]()
    val window = new Array[Byte](1024)
    
    var x = stream.read(window)
    while(x != -1){
      window.foreach(temp.append(_))
      x = stream.read(window)
    }
    
    return temp.toArray
  }
  
  def parsePath(file:String):String = {
    var newFile = "" 
    newFile = if (file.startsWith("f:")) file.replace("f:", "file:///") else file
    newFile = if (file.startsWith("h:")) file.replace("h:", "hdfs:///") else file
    
    return newFile
  }
  
  def readSequence_SaveImage(args:Array[String]):Unit = {
    val input = args(1)
    val output = parsePath(args(2))
    
    val file = sparkContext.sequenceFile[Int,BytesWritable](input)
    val sizes = file.map{ item => 
      val len = item._2.getLength
      var bytes = item._2.copyBytes()
      (len, bytes)
    }
    sizes.foreach(it => log.warn(s"The size is: ${it._1}"))
    
    // Get the bytes
    val binaryFile = sizes.sortBy(_._1).map(t => t._2).collect
    val outputFile = new FileOutputStream(new File(output))
    
    // Seems to need the metadata
    binaryFile.foreach( item => outputFile.write(item))
    
    
    outputFile.flush
    outputFile.close
    
  }
  
  def readSequence_PrintInfo(args:Array[String]):Unit = {
    val input = args(1)
    
    val file = sparkContext.sequenceFile[Int,BytesWritable](input)
    val sizes = file.map{ item => 
      val len = item._2.getLength
      var bytes = item._2.copyBytes()
      (len, bytes)
    }
    
    sizes.foreach(it => log.warn(s"The size is: $it._1"))
  }
  
  def fileReadSplitsHDFS_SaveHDFS(args:Array[String]) = {
    val inputDir = args(1)
    val outputDir = args(2)
    val partitions = args(3).toInt
    
    val binaryFiles = sparkContext.binaryFiles(inputDir, partitions)
    log.warn("# Input directory read")
    
    // Read the file, use a dummy number to write the sequence file
    val datax = binaryFiles.map{ item =>
      val stream = item._2.open()
      val index = item._1.split("_").last.replace(".bin", "").toInt
      
      log.warn("# Worker node, file split read")
      val buff = new Array[Byte](1024)
      val workerBuff = new ArrayBuffer[Byte]()
      while(stream.read(buff) != -1) 
        buff.copyToBuffer(workerBuff)
        
      (index, workerBuff.toArray)
    }
    
    datax.saveAsSequenceFile(outputDir)
    log.warn("# File saved as sequence file")
  }
  
  def readSplitsHDFS_SaveHDFS(args:Array[String]):Unit = {
    val outputFile = args(1)
    val splits = args(2).toInt
    val inputDir = args(3)
    val inputFormat = args(4)
    
    val metaData = sparkContext.textFile(inputDir + "/metadata.txt", splits).map(stringTupleToIntTuple(_))
    val binaryFiles = sparkContext.binaryFiles(inputDir, splits)
    .filter(item => item._1.endsWith(inputFormat))
    .map{ case (file, stream) =>
      val index = file.split("_").last.replace(".jpg", "").toInt
      (file, stream, index)
    }
    
    log.warn("#>>> DEBUG Info")
    binaryFiles.foreach(f => log.warn(s"# The file: ${f._1} index: ${f._3}"))
    metaData.foreach(m => log.warn(s"# The index: ${m._1}"))
    binaryFiles.count()
    log.warn("#>>> End DEBUG")
    
    val datax = binaryFiles.map{ imgSplit =>
      val stream = imgSplit._2
      val splitId = imgSplit._3
      
      val input = stream.open()
      val window = new Array[Byte](1024)
      val buffer = new ArrayBuffer[Byte]()
      while(input.read(window) != -1){
        buffer ++= window
      }
      input.close()
      (splitId, buffer.toArray)
    }.saveAsSequenceFile(outputFile)
    
    
  }
  
  
  

  
  def stringTupleToIntTuple(tuple:String):(Int, Int, Int, Int,Int) = {
    val nums = tuple.split(" ")
    val intVals = nums.map(str => str.toInt)
    (intVals(0), intVals(1), intVals(2), intVals(3), intVals(4))
  }
  
  
  def tupleToString(tuple:(Int, Int, Int, Int, Int)) : String = {
    val str = tuple._1 + " " + tuple._2 + " " + tuple._3 + " " + tuple._4 + " " + tuple._5
    return str
  }
  
  
  // Save the splits in HDFS, read and split the image from the master
  def splitAndSaveImageFromMaster(args:Array[String]): Unit = {
    val hdfsPath = args(1)
    val totalPieces = args(2).toInt
    val imageFormat = args(3)
    
    // Create the stream
    val img = sparkContext.binaryFiles(hdfsPath).first
    val stream = ImageIO.createImageInputStream(img._2.open())
    val reader = ImageIO.getImageReaders(stream)
    
    // Terminate if no reader was found
    if(!reader.hasNext()) throw new ClassNotFoundException
    
    // Get reader and prepare image
    val imgReader = reader.next()  
    imgReader.setInput(stream)
    val width = imgReader.getWidth(imgReader.getMinIndex)
    val height = imgReader.getHeight(imgReader.getMinIndex)
    println(s">>> W: $width H: $height")
    
    // Create the rectangles
    // Get the individual split to be sotred in HDFS
    val splits = LinearSplit.getSplits(width, height, totalPieces)
      .map{case (id, rec) => (id, rec, img._2, imageFormat)}
    
    // Almost store the data in HDFS
    val imgSplits = splits.map{ imagePiece =>
      val id = imagePiece._1
      val rectangle = imagePiece._2
      val readerStream = imagePiece._3
      val format = imagePiece._4
      
      val streamM = ImageIO.createImageInputStream(readerStream.open())
      val readerS = ImageIO.getImageReaders(streamM).next
      
      // Manipulate the image and generate a buffer
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
    
    // Store in HDFS
    val splitsRDD = sparkContext.parallelize(imgSplits)
    
    splitsRDD.map{ split =>
      val aDir = "/fileHDFS"
      val format = "jpg"
      
      val imageDir = aDir + "image" + split.index.toString + format
      
    }
    
    // Return
  }
  
  
}