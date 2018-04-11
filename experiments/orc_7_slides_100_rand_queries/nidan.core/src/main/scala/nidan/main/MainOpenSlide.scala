package nidan.main

import java.awt.Dimension
import java.awt.image.BufferedImage
import java.awt.image.DataBufferInt
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.imaging.ImageFormat
import org.apache.commons.imaging.ImageFormats
import org.apache.commons.imaging.Imaging
import org.openslide.OpenSlide

import javax.imageio.IIOImage
import javax.imageio.ImageIO
import javax.imageio.ImageWriteParam
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import javax.imageio.stream.FileImageOutputStream
import nidan.io.LinearSplit
import nidan.io.NidanRectangle
import nidan.io.NidanSplitItem
import nidan.utils.NidanUtils



object MainOpenSlide {

  val exit = (level:Int) => System.exit(level)
  var openSlideImg:OpenSlide = null
  def main(args: Array[String]): Unit = {
    
    val option = args(0)
    
    option match{
      case "1" =>{
        
        val inputDir = args(1)
        val thresholdMB = args(2).toLong // 4MB
        val imageFormat = ImageFormats.valueOf(args(3))
        
        generateAllTilesFromSVS(inputDir, thresholdMB, imageFormat)
      }
      
      case "2" =>{
        val input = args(1)
        val level = args(2).toInt
        val numberOfTiles = args(3).toInt
        val format = ImageFormats.valueOf(args(4))
        
        generateTilesFromSVS(input, level, numberOfTiles, format)
      }
      
      case "3" =>{
        val input = args(1)
        val level = args(2).toInt
        val numberOfTiles = args(3).toInt
        val output = args(4)
        val format = ImageFormats.valueOf(args(5))
        
        stitchTilesFromSVS(
            input, 
            level, 
            numberOfTiles, 
            output, 
            format)
      }
      
      
    }
    
    
    
    
    
  }
  
def time[T](proc: => T): (T,Double) = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    val time = (end-start)/1000000000.0 
    (res, time)
  }
  
  def getMappingIOFiles(inputDir:String):Array[String] ={
    val result = new ArrayBuffer[String]()
    
    println(s"Current file: $inputDir")
    
    val dirs = new File(inputDir).list
    for (dir <- dirs){
      val patientFiles = new File(s"$inputDir/$dir").listFiles
      val patientFilesStr = patientFiles.map(file => file.getAbsolutePath)
      
      result.append(patientFilesStr : _*)
    }
    
    result.toArray
  }
  
  def getMappingIOFiles1(inputDir:String):Array[String] ={
    
    val patientFiles = new File(s"$inputDir").listFiles  
    
    return patientFiles.map(f => f.getCanonicalPath)
  }
  
  var log2 = (number:Long) => Math.log10(number)/Math.log10(2.0)
  
  // Threshold as power of two
  
  def getTotalPartitions(size:Long, threshold:Long):Long = {
    val blocks = Math.ceil(size/threshold).toLong
    
    val oneSide = Math.ceil(blocks/2).toInt
    
	  val exponent = Math.ceil(log2(oneSide))
	  
	  return Math.pow(2, exponent).toLong
  }
  
  val log4 = (x:Long) => Math.log10(x)/Math.log10(4)
  
  val partitionsFromSize = (size:Long) => { 
    val partitions = Math.pow(4, log4(size).floor).toLong
    
    val result = if (partitions < 1) 1 else partitions
    result
  }
  
  // Check the partitions are being created
  def getPartitionsResume(data:ArrayBuffer[(String,Int,Array[NidanSplitItem],Long)]) ={
    val strMessage = new StringBuilder()
    strMessage.append(s"ITEM \t LEVEL \t COUNT\n")
    
    data.groupBy(f => f._1)
    .map{case (inputStr, partitions) => (inputStr, partitions, strMessage)}
    .map{case (inputStr, partitions, strMessage) =>
      val parts = partitions.groupBy(p => p._2)
      parts.foreach{item => strMessage.append(s"$inputStr \t ${item._1} \t ${item._2(0)._3.length}\n")}
    }
    
    println(strMessage.toString)
  }
  
  def nidanRectangleToString(rec:NidanRectangle): String ={
    return s"${rec.height},${rec.topX},${rec.topY},${rec.buttomX},${rec.buttomY}"
  }
  def nidanRectangleFromString(str:String):NidanRectangle ={
    val values = str.split(",")
    return NidanRectangle(
        values(0).toInt,
        values(1).toInt,
        values(2).toLong,
        values(3).toLong,
        values(4).toLong,
        values(5).toLong
        )
  }
  
//  def splitToString(Nida)
  
  // The next code is hardcoded in a sense
  // We asume the input is a directory that has more directories
  // We asume that each subdirectory has SVS images
  
  /* It works like this:
	   * Image_i:
	   * 	levels:[
	   * 		0:[
	   * 			{tile1,rectange},
	   * 			{tile2,rectange},
	   * 			{tile3,rectange},
	   * 		],
	   * 		1:[
	   * 			{tile1,rectange},
	   * 			{tile2,rectange},
	   * 			{tile3,rectange},
	   * 		]
	  
	  */
  
  
  
 
  
  def getPartitionMeta(svsPath:String, byteRes:Int, threshold:Long):
  Seq[(Int,Dimension,Long,Long)] = {
    try{
      val svsImage = new OpenSlide(new File(svsPath))
      
      val levelSizes = (0 to svsImage.getLevelCount -1).map{ level =>
        val width = svsImage.getLevelWidth(level)
        val height = svsImage.getLevelHeight(level)
        val size = width * height * byteRes
        val partitions = partitionsFromSize(size / threshold)
        
        val dimension = new Dimension()
        dimension.setSize(svsImage.getLevelWidth(level), svsImage.getLevelHeight(level))
        
        (level, dimension, size, partitions)
      }
      
      svsImage.close
      return levelSizes
    }
    catch{
      case e:Exception => return null
    }
  }
  
  
  def getSplitFromImageMeta(metadata:Seq[(Int,Dimension,Long,Long)]):
  Seq[(Int, Dimension, Long, Seq[NidanSplitItem])]={
    val splits = metadata.map{ case (level, dimension, size, partitions) =>
      val width = dimension.getWidth
      val height = dimension.getHeight
      
      val result = if(partitions == 1){
        val rectangle = NidanRectangle(width.toInt, height.toInt,0L, 0L,width.toInt, height.toInt)
        Array((NidanSplitItem(1, rectangle, (0,0), 0)))
      }else{
        LinearSplit.getSplitItemsF(width.toLong, height.toLong, partitions)
      }
      
      (level, dimension, size, result.toSeq)
    }
    
    return splits
  }
  // 64MB = 67108864
  
  def printNidanSplitSequence(data:Seq[(NidanSplitItem, String)]) = {
    for(d <- data){
      val index = d._1.index
      val rowCol = d._1.rowCol
      val zIndex = d._1.zindex
      val rectangle = d._1.rectangle
      
      println(s"\t  >> $index $zIndex ${d._2}")
    }
  }
  
  def getTileOutputName(item:NidanSplitItem, inputFile:String, level:Int):String = {
    val index = item.index
    val rowCol = item.rowCol
    val zIndex = item.zindex
    val rectangle = item.rectangle
    
    val newName = inputFile +
      s"_${level}_${index}_${zIndex}_${rowCol._1}_${rowCol._2}" + 
      s"_${rectangle.buttomX}_${rectangle.buttomY}" +
      s"_${rectangle.topX}_${rectangle.topY}.png"
      
      newName
  }
  
  def tileOutputName(item:NidanSplitItem, inputFile:String, level:Int, format:ImageFormats):String = {
    val index = item.index
    val rowCol = item.rowCol
    val zIndex = item.zindex
    val rectangle = item.rectangle
    
    val newName = inputFile +
      s"_${level}_${index}_${zIndex}_${rowCol._1}_${rowCol._2}" + 
      s"_${rectangle.buttomX}_${rectangle.buttomY}" +
      s"_${rectangle.topX}_${rectangle.topY}.${format.toString}"
      
      newName
  }
  
  def stitchTilesFromSVS(input:String, level:Int, numberOfTiles:Int, output:String, imgFormat:ImageFormats) = {
    val inputFile = new File(input)
    val svsImage = new OpenSlide(inputFile)
    
    println(s">> Output will be written in $output")
    val LOG = (msg:String) => println(msg)
    
    // Random level currentry not supported
    val imageLevels = svsImage.getLevelCount
    if ( level < 0 || level > imageLevels){
      println(s">> ERROR level $level is not between 0 and $imageLevels")
      exit(1)
    }
    
    // Hardcode this part, but change it later
    val height = svsImage.getLevelHeight(level)
  		val width = svsImage.getLevelWidth(level)
  		val dimension = new Dimension
  		dimension.setSize(width, height)
  		
    val partitions = 64
    val tileMeta = Seq((level, dimension, width * height, partitions.toLong))
    
    // Get only the number of tiles that we're interested in and set the name
    val tileSplitList = getSplitFromImageMeta(tileMeta).map{item =>
        val splits = item._4
          .filter(s => s.index <= numberOfTiles)
          .map(split => (split, tileOutputName(split, input, level, imgFormat)))
        (item._1, item._2, item._3, splits)
    }
    
    val m = tileSplitList.flatMap(t => t._4.map(ti => (ti._1, ti._2, t._1)))
    val list = m.map{item => 
      val img = getBufferedImage(item._1.rectangle, level, svsImage)
      (img, img.getWidth, img.getHeight)
    }.toArray
    
    var tW, tH = 0
    for(item <- list){
      tW += item._2
      tH += item._3
    }
//    val tWidthHeight = list.aggregate((0, 0))((a, b) =>
//      (a._1 + b._2, a._2 + b._3),
//      (x, y) => (x._1 + y._1, x._2 + y._2)
//    )
    LOG(s">> Aggregate generated, final W, H => ${tW} ${tH}")
    
    // Is a 2x2 matrix
    val imgType = list.last._1.getType
    val buffer = new BufferedImage(
      tW/2, 
      tH/2, 
      list.last._1.getType
    )
    
    NidanUtils.stitchImages(list, buffer, LOG)
    LOG(">> Image was stitched")
    
    ImageIO.write(buffer, imgFormat.toString, new File(output))
    LOG(s">> Image was saved as $output")
    
  }
  
  /**
   * Generates a given set of tiles from an input SVS image 
   * and saves them as images in a indicated image format.
   * Tiles may be from the same level or not. The resulting tiles
   * will be stored in the input directory.
   * @author debarron 
   * 
   * @param input The SVS image that the program will read
   * @param level The level that will be consider, if -1 then it should be random
   * @param numberOfTiles The number of tiles that will be extracted from the file
   * @param format The image format to save the tiles  
   * */
  def generateTilesFromSVS(
      input:String, 
      level:Int, 
      numberOfTiles:Int, 
      format:ImageFormats
      ) = {
    
    val inputFile = new File(input)
    val output = inputFile.getParent
    val svsImage = new OpenSlide(inputFile)
    
    println(s">> Output will be written in $output")
    
    // Random level currentry not supported
    val imageLevels = svsImage.getLevelCount
    if ( level < 0 || level > imageLevels){
      println(s">> ERROR level $level is not between 0 and $imageLevels")
      
      exit(1)
    }
    
    // Hardcode this part, but change it later
    val height = svsImage.getLevelHeight(level)
  		val width = svsImage.getLevelWidth(level)
  		val dimension = new Dimension
  		dimension.setSize(width, height)
  		
    val partitions = 64
    val tileMeta = Seq((level, dimension, width * height, partitions.toLong))
    
    // Get only the number of tiles that we're interested in and set the name
    val tileSplitList = getSplitFromImageMeta(tileMeta).map{item =>
        val splits = item._4
          .filter(s => s.index <= numberOfTiles)
          .map(split => (split, tileOutputName(split, input, level, format)))
        (item._1, item._2, item._3, splits)
    }
    
    val m = tileSplitList.flatMap(t => t._4.map(ti => (ti._1, ti._2, t._1)))
    m.foreach(item =>
      saveImageFromOpenSlide(
            item._1.rectangle, 
            item._3, 
            item._2, 
            svsImage, 
            format)
    )
    
  }

  
   // BINGO This is the function for the OpenSlideExp 
  /**
    * Save all the tiles from the SVS image and store them in a local
   	* directory, where the SVS image is stored.
   	* 
   	* @param inputDir The input directory for the function
   	* @param outputDir The output directory
   	* @param thresholdMB The value used to define the cut
   	* @imageFormat The format to store the tiles
   	* @returns Nothing, it will save all the images in the outputdir
   	*   
   	*/
  def generateAllTilesFromSVS(
      inputDir:String,
      thresholdMB:Long,
      imageFormat:ImageFormats
  ) = {
    
    val byteRes = 1
    
    // Get the list of files to read
    val listOfFiles = getMappingIOFiles(inputDir)
    .map{inputFile => (inputFile)}
    
    // Get the metadata for each of the levels
    val svsImagesMetadataPre = listOfFiles.map{file => 
      val levelsSizes = getPartitionMeta(file, byteRes, thresholdMB)
      (file, levelsSizes)
    }
    
    // Inform if there were errors
    svsImagesMetadataPre.filter(_._2 == null).foreach{ item =>
      println(s">> #ERROR while open file: ${item._1}")
    }
    
    val svsImagesMetadata = svsImagesMetadataPre.filter(_._2 != null)
        
    // Get the splits for the images
    val tiles = svsImagesMetadata.map{ case(input, meta) =>   
      val tiles = getSplitFromImageMeta(meta)
      (input, tiles)
    }
    
    // Get access to each file from the list
    val fileTiles = tiles.map{
    case (input, seqTiles) =>
        
        // Get access to all the levels from the each file input
        val seqTilesOutput = seqTiles.map{
          case (level, dimension, size, seqNidanItem) =>
            
            // Get each rectangle with its outputFileName
            val outputFile = seqNidanItem.map(
            item => (item, tileOutputName(item, input, level, imageFormat)))
            (level, dimension, size, outputFile)
          }
        (input, seqTilesOutput)
    }
    

    // The number of tiles are:
    // BINGO Magic starts here
    val listOfTiles = List(1, 2, 4, 8, 16, 32, 64, 128)

    println("# Writing the image")
    var openSlideImage:OpenSlide = null
    for(meta <- fileTiles){
      val input = meta._1
      val metaData = meta._2
      
      println(s">> Meta from: $input")
      openSlideImage = new OpenSlide(new File(input))
      val m1 = metaData.flatMap(f => f._4.map(i => (i._1, i._2, f._1)))
      
      // BINGO
      for(tiles_i <- listOfTiles){
      val m = m1.filter(_._1.zindex <= tiles_i)
      
      // Generate and save the tiles
      time{m.foreach{ item =>
        saveImageFromOpenSlide(
            item._1.rectangle, 
            item._3, 
            item._2, 
            openSlideImage, 
            imageFormat)
            
        println(s"\t>> ${item._3} ${item._2}")
      }}

      }
      openSlideImage.close
      System.gc()
      
      println("\n\n")
    }         
  }

  
  
  def saveAllImagesToLocalTiff(args:Array[String]) = {
    val inputDir = args(1)
    val outputDir = args(2)
    val thresholdMB = args(3).toLong // 4MB
    val splitLogger = args(4)
    
    val listOfFiles = getMappingIOFiles(inputDir)
    .map{inputFile => (inputFile, inputFile.replace(inputDir, outputDir))}

    
//    val levelsByFile = listOfFiles.map{file => getArrayLevels_BySVSImage(file)}
    
    println("#BEGIN List of files to be proccessed")
    listOfFiles.foreach(files => println(s"\t >>> Inqueued: ${files._1}"))
    println("#END\n")
     
    val typeImg = BufferedImage.TYPE_INT_ARGB
    val imgByteRes = 1
	  val formatImg = ImageFormats.TIFF
	  val finalSplits = new ArrayBuffer[(String,Int,Array[NidanSplitItem],Long)]()

	  
	  
	  // Analize the images
    for(file <- listOfFiles){
    	  try{
        openSlideImg = new OpenSlide(new File(file._1))
        var level = 0
        
        // OpenSlide's levels go from 0 to count - 1
        println("#BEGIN Image partitioning in: " + file._1)
        for (level <- 0 to openSlideImg.getLevelCount -1){
          val width = openSlideImg.getLevelWidth(level)
          val height = openSlideImg.getLevelHeight(level)  
          val size = width * height * imgByteRes 
          
          val partitions = partitionsFromSize(size / thresholdMB)
          val partitionsCreated = if (partitions > 1){  
            LinearSplit.getSplitItemsF(width, height, partitions)
          }else{
            val rectangle = NidanRectangle(width.toInt, height.toInt,0L, 0L,width.toInt, height.toInt)
            Array((NidanSplitItem(1, rectangle, (0,0), 0)))
          }
          
          finalSplits.append((file._1, level, partitionsCreated, partitionsCreated.length))
          println(s"\t >>> LEVEL $level " +
              s"Partitions REQUIRED ${partitionsCreated.length} " +
              s"CREATED $partitionsCreated")
        
        }
        println("#END\n")
        openSlideImg.close
        
      }catch{
        case io:IOException => println(s"#ERROR [Invalid File] OpenSlide can't process ${file._1} file") 
        case e:Exception => 
          println(s"#ERROR occurred in file: ${file._1}\n")
          e.printStackTrace()
      }
    }
    
    // Write the data
    val dataToWrite = finalSplits
    .groupBy(f => f._1)
    .map{ case (inputStr, partitions) =>
      
      val startTime = System.nanoTime
      
      val inputFile = new File(inputStr)
      openSlideImg = new OpenSlide(inputFile)
      val result = ArrayBuffer[(NidanRectangle,Int,String)]()
      
      println(s"#DEBUG Saving splits for image: ${inputStr}")
    	  for((x, level, splits, totalSplits) <- partitions){
        val output = new File(inputFile.getCanonicalPath.replace(inputDir, outputDir).replace(inputFile.getName, ""))
        val outputStr = s"${output.getCanonicalPath}/${inputFile.getName}_${level}_${totalSplits}_"
        if(!output.exists()) output.mkdirs()
        
        val levelWidht = openSlideImg.getLevelWidth(level)
        val levelHeight = openSlideImg.getLevelHeight(level)
     
        
        splits.foreach{split =>
          val fig = split.rectangle
          val outputFile = outputStr + s"${split.index}_${split.zindex}_${fig.width}_${fig.height}_${levelWidht}_${levelHeight}.TIFF"
            
          result.append((fig, level, outputFile))
        }
        
        println(s"\t >>> LEVEL ${level} PROCESSED ${totalSplits} ")
      }
      println("#END\n")
      
      openSlideImg.close
      (inputStr,result.toArray)      
    }
    
    val writer = new FileWriter(new File(splitLogger))
    // Write the result to a file
    for ((inputStr, spArray) <- dataToWrite){
      writer.write(s"#\n$inputStr\n")
      
      for(split <- spArray){
        val rectangle = split._1
        val index = split._2
        val outFile = split._3
        
        writer.write(
            nidanRectangleToString(rectangle) +
            "$" + index + 
            "$" + outFile + "\n"
            )
      }
      writer.write("#\n")
      writer.flush
    }
    writer.close
    
    // Write the data in disk  
    try{
      println(s"#DEBUG Saving the images")
      val dataSaved = dataToWrite.map{ case (inputStr, splitsArray) =>
        val startTime = System.nanoTime
        
        val OS = new OpenSlide(new File(inputStr))
        val levelCount = OS.getLevelCount
        
        for((fig, level, outFile) <- splitsArray){
          saveTIFFImageFromOpenSlide(fig, level, outFile, OS)
          println(s"\t >>> LEVEL $level SAVED $outFile")
        }
        
        OS.close
        val endTime = System.nanoTime
        
        println(s"#DATA F-${inputStr} L-${levelCount} S-${splitsArray.length} NT-${endTime-startTime}")
      }
      println(s"#END\n")
    }catch{
    case e:Exception => e.printStackTrace 
    }
         
  }
  
  
  def saveTIFFImageFromLogger(args:Array[String]) = {
    val loggerFile = args(1)
    val offSet = args(2).toInt
    
    var actualPosition = 0
    val reader= new BufferedReader(new FileReader(new File(loggerFile)))
    
    while(actualPosition -1 < offSet){
      val line = reader.readLine
      actualPosition += (if (line.equals("#")) 1 else 0)
    }
    
    // Read all the splits from 1 SVS then process the data
    var noMoreInput = false
    while(!noMoreInput){
      val inputFile = reader.readLine
      if(inputFile == null) noMoreInput = true
      else{
        var endInput = false
       
        val buffer = new ArrayBuffer[(NidanRectangle,Int,String)]()
        while(!endInput){
          val newLine = reader.readLine
          if(newLine.equals("#")) endInput = true
          else{
            val values = newLine.split("$")
            
            val rectangle = nidanRectangleFromString(values(0))
            val index = values(1).toInt
            val output = values(2)
            
            buffer.append((rectangle,index,output))
          }
        }
        
        val OS = new OpenSlide(new File(inputFile))
        val levelCount = OS.getLevelCount
      
        println(s"#DEBUG Saving the images")
        val startTime = System.nanoTime  
        for((fig, level, outFile) <- buffer){
          try{
            saveTIFFImageFromOpenSlide(fig, level, outFile, OS)
            println(s"\t >>> LEVEL $level SAVED $outFile")
    
          }catch{
            case e:Exception => e.printStackTrace 
          }
        }      
        val endTime = System.nanoTime     
        println(s"#DATA F-${inputFile} L-${levelCount} S-${buffer.length} NT-${endTime-startTime}")
        println(s"#END\n")
        OS.close
      }
    }
   
  }
  
  def getBufferedImage(fig:NidanRectangle, level:Int, source:OpenSlide):BufferedImage={
    val outImg = new BufferedImage(fig.width, fig.height, BufferedImage.TYPE_INT_ARGB)
    val g = outImg.getGraphics
    val dataArray = outImg.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
        
    source.paintRegionARGB(dataArray, fig.topX, fig.topY, level, fig.width, fig.height)
    g.drawImage(outImg, 0, 0, fig.width, fig.height, null)
    
    outImg      
  }
  
  def saveImageFromOpenSlide(fig:NidanRectangle, 
      level:Int, 
      outFile:String, 
      OS:OpenSlide, 
      imgFormat:ImageFormats):Boolean ={
    var result = true
    try{
      val outImg = new BufferedImage(fig.width, fig.height, BufferedImage.TYPE_INT_ARGB)
      val g = outImg.getGraphics
      val dataArray = outImg.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
        
      OS.paintRegionARGB(dataArray, fig.topX, fig.topY, level, fig.width, fig.height)
      g.drawImage(outImg, 0, 0, fig.width, fig.height, null)
      
      
      // TODO START Replace by flushBufferedImage
      if(imgFormat.toString == "JPEG"){
        val jpegParams = new JPEGImageWriteParam(null)
        jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
        jpegParams.setCompressionQuality(0.9f)
        
        val writer = ImageIO.getImageWritersByFormatName(imgFormat.toString).next()
        writer.setOutput(new FileImageOutputStream(new File(outFile)))
        writer.write(null, new IIOImage(outImg.getRaster, null, null), jpegParams);
        
      }else{
        Imaging.writeImage(outImg, new File(outFile), imgFormat, null)
      }
      // END
      
      g.dispose()
    }
    catch{
      case e:Exception => 
        println(s"#ERROR Error ocurred in ${outFile.toString}\n")
        e.printStackTrace()
        result = false
    }
    
    return result
  }
  
  def flushBufferedImage(img:BufferedImage, output:String, imgFormat:ImageFormats)={
   if(imgFormat.toString == "JPEG"){
        val jpegParams = new JPEGImageWriteParam(null)
        jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
        jpegParams.setCompressionQuality(0.9f)
        
        val writer = ImageIO.getImageWritersByFormatName(imgFormat.toString).next()
        writer.setOutput(new FileImageOutputStream(new File(output)))
        writer.write(null, new IIOImage(img.getRaster, null, null), jpegParams);
        
      }else{
        Imaging.writeImage(img, new File(output), imgFormat, null)
      } 
  }
  
  
  
  
  
  
  def saveTIFFImageFromOpenSlide(fig:NidanRectangle, level:Int, outFile:String, OS:OpenSlide):Boolean ={
    var result = true
    try{
      val outImg = new BufferedImage(fig.width, fig.height, BufferedImage.TYPE_INT_ARGB)
      val g = outImg.getGraphics
      val dataArray = outImg.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
        
      OS.paintRegionARGB(dataArray, fig.topX, fig.topY, level, fig.width, fig.height)
      g.drawImage(outImg, 0, 0, fig.width, fig.height, null)
      Imaging.writeImage(outImg, new File(outFile), ImageFormats.PNG, null)
      
      g.dispose()
    }
    catch{
      case e:Exception => 
        println(s"#ERROR Error ocurred in ${outFile.toString}\n")
        e.printStackTrace()
        result = false
    }
    
    return result
  }
  
  
  
  def saveTIFFImageFromOpenSlide(fig:NidanRectangle, level:Int, outFile:String):Boolean ={
    var result = true
    try{
      val outImg = new BufferedImage(fig.width, fig.height, BufferedImage.TYPE_INT_ARGB)
      val g = outImg.getGraphics
      val dataArray = outImg.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
        
      openSlideImg.paintRegionARGB(dataArray, fig.topX, fig.topY, level, fig.width, fig.height)
      g.drawImage(outImg, 0, 0, fig.width, fig.height, null)
      Imaging.writeImage(outImg, new File(outFile), ImageFormats.TIFF, null)
    }
    catch{
      case e:Exception => 
        println(s"#ERROR Error ocurred in ${outFile.toString}\n")
        e.printStackTrace()
        result = false
    }
    
    return result
  }
  
  
  
  
  
  
  
  
  
  def call_saveTIFFImageLocally(args:Array[String]) = {
    val inputDir = args(1)
    val outputDir = args(2)
    
    val fileList = new File(inputDir).listFiles
    for (file <- fileList){
      val inputFile = file.getAbsolutePath
      val outputFile = s"$outputDir/${file.getName}"
      val newArgs = Array("buu", inputFile, outputFile)
      
      saveTIFFImageLocally(newArgs)
    }
  }
  
  
  
  def saveZoomImage(args:Array[String]) = {
    try{
      
      val input = args(1)
      val w = args(2).toInt
      val h = w
      val level = args(3).toInt
      
      val output = new File(s"$input.zoom")
      
      val os = new OpenSlide(new File(input))
      val levels = os.getLevelCount
      println(s"Image has $levels levels")
      
      val buff = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
      val g = buff.getGraphics
      val data = buff.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
      
      os.paintRegionARGB(data, 0L, 0L, 3, w, h)
      g.drawImage(buff, 0, 0, w, h, null)
      
      Imaging.writeImage(buff, output, ImageFormats.TIFF, null)
      println("Result image was writen")
    }
    catch{
      case io:IOException => println("Error with the file")
      case e:Exception => 
        println("Error with something else: ")
        e.printStackTrace()
    }
    
    
  }
  
  
  def saveTIFFImageLocally(args:Array[String]):Unit={
    val input = args(1)
    val out = args(2)
    
    try{  
      val x = new OpenSlide(new File(input))
      val levels = x.getLevelCount
      
      // Repations between levels and image sizes
      var i = 0
      println(s"Level \t #Imgs \t W \t H")
      for ( i <- 0 to levels - 1){
        val h = x.getLevelHeight(i)
        val w = x.getLevelWidth(i)
        
        println(s"$i \t $w \t $h")
      }
      
      val format:ImageFormat = ImageFormats.TIFF
      val keys = x.getAssociatedImages.keySet
      for (key <- keys.toArray()){
        val fileOutput = new File(s"$out.img_$key.tiff")
        val buffer = x.getAssociatedImages.get(key).toBufferedImage
        
        Imaging.writeImage(buffer, fileOutput, format, null)
      }
    }
    catch{
      case io:IOException => println("Error while open the image")  
      case e:Exception => println("An error occurred")
    }
    
  }
  
}
