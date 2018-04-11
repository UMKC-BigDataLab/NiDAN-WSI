package nidan.main

import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileReader
import java.io.FileWriter

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import javax.imageio.IIOImage
import javax.imageio.ImageIO
import javax.imageio.ImageWriteParam
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import javax.imageio.stream.FileImageOutputStream
import nidan.io.LinearSplit
import nidan.io.NidanContext._
import nidan.io.NidanContext
import nidan.io.NidanImageSplit
import nidan.io.NidanImageSplitter
import nidan.models.entities.Patient
import java.io.InputStreamReader
import java.io.InputStream
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.hadoop.io.BytesWritable
import java.io.FileOutputStream

object MainSystem {
  lazy val log = LogManager.getRootLogger
  
  def main(args: Array[String]): Unit = {
    val option = args(0)
    
    // TODO: Indicate which option uses scala or spark
    option match {
      case "-rL-spL" => readFromLocal_SplitOnLocal(args)
      case "-rspL-sL" => readSplitFromLocal_SaveLocal(args)
      case "-rspL-sL-NC" => readSplitFromLocal_SaveLocal_NoCompresion(args)
      case "-rspHDFS-sHDFS" => readSplitsHDFS_SaveHDFS(args)
      case "-rSeqHDFS" => readSequence_PrintInfo(args)
      case "-rHDFS-sL" => readSequence_SaveImage(args)
      case "-rfL-sFL" => fileReadFromLocal_SaveLocal(args)
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
    
    
    // Get the jpg files
//    val imageSplits = sparkContext.binaryFiles(inputDir, splits).filter(item => item._1.endsWith(inputFormat))
//    val metaData = sparkContext.textFile(inputDir + "/metadata.txt", splits).map(stringTupleToIntTuple(_))
//    
//    val datax = imageSplits.map{imgSplit =>
//      val stream = imgSplit._2
//      val splitId = 3
//      
//      val input = stream.open()
//      val window = new Array[Byte](1024)
//      val buffer = new ArrayBuffer[Byte]()
//      while(input.read(window) != -1){
//        buffer ++= window
//      }
//      input.close()
//      (splitId, buffer.toArray)
//      
//    }.saveAsSequenceFile(outputFile)
//    
//    
  }
  
  def mainOld(args: Array[String]): Unit = {
    // Create a patient and an image from the std input
    
    val option = args(0)
    
    val patientId = args(1)
    val patientDOB = args(2)
    val patientGender = args(3)
    
    val imageId = args(4)
    val imageResolution = args(5)
    val imageMagnification = args(6)
    val imageZoom = args(7)
    val imageDate = args(8)
    val imagePath = args(9)
    val imageFile = args(10)
    
    
    println(">>> Nidan System")
    
    println(">>> New User")
    val newPatient = Patient(patientId.toLong, patientDOB, patientGender)
    println(">>> User created and added")
    
    println(s">>> About to process: $imagePath")
    val filesHDFS = sparkContext.binaryFiles(imagePath)
    println(s">>> Image readed")
    
    

    
    
    
    
//    println(s">>> About to split: $imagePath")
//    val images = LinearSplit.getData(filesHDFS, 4, "jpg")
//    println(">>> Image got splitted")
//    println(s">>> Image count is: ${images.count()}")
    
  }
  
  def readMetaData(file:File):Array[(Int, Int, Int, Int,Int)] = {
    val content = new BufferedReader(new FileReader(file))
    val lines = ListBuffer[String]()
    var line:String = content.readLine()
    
    while(line != null){
      lines += (line)
      line = content.readLine()
    }
    content.close()
    
    val r = lines.toArray.map(stringTupleToIntTuple(_))
    
//    { tuple =>
//      val nums = tuple.split(" ")
//      val intVals = nums.map(str => str.toInt)
//      (intVals(0), intVals(1), intVals(2), intVals(3), intVals(4))
//    }
    
    return r
  }
  
  def stringTupleToIntTuple(tuple:String):(Int, Int, Int, Int,Int) = {
    val nums = tuple.split(" ")
    val intVals = nums.map(str => str.toInt)
    (intVals(0), intVals(1), intVals(2), intVals(3), intVals(4))
  }
  
  // Read splits create image
  def readSplitFromLocal_SaveLocal(args:Array[String]):Unit = {
    println(">>> Nidan [Read splits from local - Restore image in local]")
    
    val outputImage = args(1)
    val splits = args(2).toInt
    val inputDir = args(3)
    val inputFormat = args(4)
    
    println("# Prints the metadata of the file")
    val metadata = new File(inputDir)
      .listFiles
      .filter(x => x.isFile && x.getName.endsWith("meta"))
      .last
    
    println("# Reading the metadata")
    val metaValues = readMetaData(metadata).sortBy(_._1)
    metaValues.foreach(println)
    
    println("# Select only the relevant files")
    val files = new File(inputDir)
      .listFiles
      .filter(x => x.isFile && x.getName.endsWith(inputFormat) && !x.getName.contains("NOTL"))
      .map{ x =>
        val index = x.getName.split("_")(1).replace(".jpg", "").toInt
        (index, x)
      }.sortBy(_._1)
    println("# Files are selected and sorted")
    
    println("# Create the complete image")
    val finalSize = metaValues.filter(item => item._1 == 0).last
    val finalImage = new BufferedImage(finalSize._2, finalSize._3, BufferedImage.TYPE_3BYTE_BGR)
    
    for(image <- files){
      val index = image._1
      val imageFile = image._2
      val buffer = ImageIO.read(imageFile)
      println(s"# Image: $index read it")
      
      // Get dimmensions
      val dim = metaValues.filter(item => item._1 == index).last
      finalImage.createGraphics().drawImage(buffer, dim._4, dim._5, null)
      println(s"# Image: $index added to the stream")
    }
        
    ImageIO.write(finalImage, inputFormat, new File(outputImage))  
    println(s"# Image $outputImage saved")
    println(">>> END")
    
  }
  
  // This uses a file, not an image 
  def fileReadFromLocal_SaveLocal(args:Array[String]) = {
    val path = args(1)
    val splits = args(2).toInt
    
    val file = new File(path)
    val splitSize = file.length() / splits
    val mod = file.length() % splits
    
    val sizes = Array[Long](splits).map(_ => splitSize)
    sizes(splits-1) = sizes.last + mod
    
    sizes.foreach(println)
  }
  
  // Read splits create image
  def readSplitFromLocal_SaveLocal_NoCompresion(args:Array[String]):Unit = {
    println(">>> Nidan [Read splits from local - Restore image in local]")
    
    val outputImage = args(1)
    val splits = args(2).toInt
    val inputDir = args(3)
    val inputFormat = args(4)
    
    println("# Prints the metadata of the file")
    val metadata = new File(inputDir)
      .listFiles
      .filter(x => x.isFile && x.getName.endsWith("meta"))
      .last
    
    println("# Reading the metadata")
    val metaValues = readMetaData(metadata).sortBy(_._1)
    metaValues.foreach(println)
    
    println("# Select only the relevant files NOT compressed")
    val filesNC = new File(inputDir)
      .listFiles
      .filter(x => x.isFile && x.getName.endsWith(s"-NOTL.$inputFormat"))
      .map{ x =>
        val index = x.getName.split("_")(1).replace("-NOTL.jpg", "").toInt
        (index, x)
      }.sortBy(_._1)
    println("# Files are selected and sorted")
    
    println("# Create the complete image")
    val finalSize = metaValues.filter(item => item._1 == 0).last
    
    // Avoid losing information
    val finalImageNC = new BufferedImage(finalSize._2, finalSize._3, BufferedImage.TYPE_3BYTE_BGR)
    
    val jpgParam = new JPEGImageWriteParam(null)
    jpgParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    jpgParam.setCompressionQuality(1f)
    
    val writer = ImageIO.getImageWritersByFormatName(inputFormat).next
    writer.setOutput(new FileImageOutputStream(new File(outputImage)))
    for(image <- filesNC){
      val index = image._1
      val imageFile = image._2
      val buffer = ImageIO.read(imageFile)
      println(s"# Image: $index read it")
      
      // Get dimmensions
      val dim = metaValues.filter(item => item._1 == index).last
      finalImageNC.createGraphics().drawImage(buffer, dim._4, dim._5, null)
      println(s"# Image: $index added to the stream")
    }
    
    val imageII = new IIOImage(finalImageNC, null, null)
    writer.write(null, imageII, jpgParam)
    println(">>> END")
    
  }
  
  
  
  
  // Create the code to split on local
  def readFromLocal_SplitOnLocal(args:Array[String]):Unit = {
    println(">>> NIDAN [Read local image, split, and save in local dir]")
    
    val outputLocalDir = args(1)
    val totalSplits = args(2).toInt
    val inputImageDir = args(3)
    val imageFormat = args(4)
    
    // TODO this reads the whole image, change it later to create a buffer
    // Change it, OOM error
    println(s"# Reading: $inputImageDir")
//    val imageFile = ImageIO.read(new File(inputImageDir))
//    val h = imageFile.getHeight
//    val w = imageFile.getWidth
    
    val imis = new FileInputStream(inputImageDir)
    val imageS = ImageIO.createImageInputStream(imis)
    val imageR = ImageIO.getImageReaders(imageS)
    val imgReader = imageR.next()  
    imgReader.setInput(imageS)
    
    val w = imgReader.getWidth(imgReader.getMinIndex)
    val h = imgReader.getHeight(imgReader.getMinIndex)
    println("# Image read")
    
    // Modification: Add metadata of the image, first tuple => (w,h,0,0), next image rectangles
    val metadata = ListBuffer[(Int,Int,Int,Int,Int)]()
    metadata += ((0,w, h, -1, -1))
    
    val sampleName = "testImage_"
    val rectangles = LinearSplit.getSplits(w, h, totalSplits).sortBy(_._1)
    for(rectangle <- rectangles){
      val area = rectangle._2
      val index = rectangle._1
      val outputImage = s"$outputLocalDir/$sampleName$index.$imageFormat"
      val outputImageW = s"$outputLocalDir/$sampleName$index-NOTL.$imageFormat"
      
      val param = imgReader.getDefaultReadParam
      param.setSourceRegion(area)
      
      val buffer = imgReader.read(imgReader.getMinIndex, param)
  			ImageIO.write(buffer, imageFormat, new File(outputImage))
      println(s"# Split saved in: $outputImage")
      
      // Avoid loosing info
      val jpgParam = new JPEGImageWriteParam(null)
      jpgParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
      jpgParam.setCompressionQuality(1f)
      val imgII = new IIOImage(buffer, null, null)
      
      val writer = ImageIO.getImageWritersByFormatName(imageFormat).next
      writer.setOutput(new FileImageOutputStream(new File(outputImageW)))
      writer.write(null, imgII, jpgParam)
      // Avoid loosing info
      
      // Modification: Adding the metadata of each rectangle
      val areaWidth = area.getWidth.toInt
      val areaHeight = area.getHeight.toInt
      val areaXCoord = area.getX.toInt
      val areaYCoord = area.getY.toInt
      metadata += ((index, areaWidth, areaHeight, areaXCoord, areaYCoord))
    }
    
    // Modification: Saving the metadata
    println("# Saving metadatada")
    val outputMetadata = s"$outputLocalDir/$sampleName.meta"
    val printer = new BufferedWriter(new FileWriter(new File(outputMetadata)))
    metadata.foreach{item =>
      val str = tupleToString(item)
      println(s"# Str: $str")
      printer.write(str + "\n")
    }
    printer.close()
    
    println(">>> End")
  }
  
  def tupleToString(tuple:(Int, Int, Int, Int, Int)) : String = {
    val str = tuple._1 + " " + tuple._2 + " " + tuple._3 + " " + tuple._4 + " " + tuple._5
    return str
  }
  
  
  
  
  // For this image should be already splitted
  def readFromLocal_SaveHDFS(args:Array[String]):Unit ={
    val inputDir = args(0)
    val outputDir = args(1)
    val splits = args(2).toInt
    
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