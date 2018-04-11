package nidan.main

import javax.imageio.IIOImage
import java.io.FileInputStream
import java.io.BufferedWriter
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import java.io.BufferedReader
import java.io.FileWriter
import java.awt.image.BufferedImage
import java.io.BufferedInputStream
import javax.imageio.ImageIO
import nidan.io.LinearSplit
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.io.FileReader
import javax.imageio.stream.FileImageOutputStream
import javax.imageio.ImageWriteParam
import java.io.FileOutputStream

object MainScala {
  
  def main(args: Array[String]): Unit = {
    val option = args(0)
    
    option match {
      case "-RL-SpL" => readFromLocal_SplitOnLocal(args)
      case "-RSpL-SL" => readSplitFromLocal_SaveLocal(args)
      case "-RSpL-SL-NC" => readSplitFromLocal_SaveLocal_NoCompresion(args)
      case "-RFL-SFL" => fileReadFromLocal_SaveLocal(args)
      case "-RSpFL-SFL" => fileReadSplitFromLocal_SaveLocal(args)
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
  
  def readMetaData(file:File):Array[(Int, Int, Int, Int,Int)] = {
    val content = new BufferedReader(new FileReader(file))
    val lines = ListBuffer[String]()
    var line:String = content.readLine()
    
    while(line != null){
      lines += (line)
      line = content.readLine()
    }
    content.close()
    
    return lines.toArray.map(stringTupleToIntTuple(_))
  }
  
  def stringTupleToIntTuple(tuple:String):(Int, Int, Int, Int,Int) = {
    val nums = tuple.split(" ")
    val intVals = nums.map(str => str.toInt)
    (intVals(0), intVals(1), intVals(2), intVals(3), intVals(4))
  }
  
  def fileReadSplitFromLocal_SaveLocal(args:Array[String]) = {
    println(">>> Nidan [Local]")
    
    val input = args(1)
    val output = args(2)
    
    val outFile = new FileOutputStream(output)
    val files = new File(input).listFiles()
    for(file <- files){
      val buff = new Array[Byte](file.length.toInt)
      val bytes = new FileInputStream(file)
      
      println(s"# Read file ${file.getName}")
      bytes.read(buff)
      bytes.close
      
      outFile.write(buff)
      println(s"# ${buff.length} bytes added to the outstream")
    }
    outFile.close
    
    println(">>> EOP")
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
    println(">>> Nidan [Local]")
    
    val path = args(1)
    val splits = args(2).toInt
    val outFormat = args(3)
    
    println("# The arguments are:")
    args.foreach(ar => println(s"# $ar"))
    
    val file = new File(path)
    val splitSize = (file.length() / splits).toInt
    val mod = (file.length() % splits).toInt
    val sizes = new Array[Int](splits)
    
    println(s"The size os the arrray is: ${sizes.length}")
    
    // TODO: Test with the map approach to get cleaner code
    var i = 0
    for(i <- 0 to (splits - 1)){
      sizes(i) = splitSize.toInt
    }
    sizes(splits-1) = splitSize + mod   
    
    println("# Print the size of each file split")
    sizes.foreach(x => println(s"# $x"))
    
    // Open a file 
    val stream = new FileInputStream(file)
    var index = 1
    for(si <- sizes){
      // Read
      val buffer = new Array[Byte](si)
      stream.read(buffer)
      println("# File read index: " + index)
      
      // Write
      val outFile = s"${outFormat}_${index}.bin"
      val output = new FileOutputStream(new File(outFile))
      output.write(buffer)
      index = index + 1
      println("# File wrote name: " + outFile)
    }
    
    println("# End of the program")
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
  
  
  
  
   
  
}