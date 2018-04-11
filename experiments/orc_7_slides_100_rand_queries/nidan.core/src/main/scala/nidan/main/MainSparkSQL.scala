package nidan.main

import java.awt.Rectangle
import java.awt.image.BufferedImage
import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe

import org.apache.commons.imaging.ImageFormats
import org.apache.commons.imaging.Imaging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.sql.SaveMode

import nidan.io.LinearSplit
import nidan.io.NidanContext
import nidan.io.SplitItem
import org.apache.spark.sql.DataFrame
import java.awt.image.DataBufferByte
import java.io.ByteArrayOutputStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import scala.io.Source
import java.nio.file.Files
import java.nio.file.{Path => jPath, Paths => jPaths} 
import org.apache.log4j.Appender
import java.io.FileWriter
import java.io.FileOutputStream
import javax.imageio.ImageIO
import javax.imageio.stream.ImageInputStream
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import javax.imageio.ImageWriteParam
import javax.imageio.IIOImage
import javax.imageio.stream.FileImageOutputStream
import java.io.ByteArrayInputStream
import nidan.utils.NidanUtils
import java.io.FileInputStream

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration

case class NidanSQLRecord(
  var imageId:String,
  var imageLevel:Int,
  var imagePartitions:Int,
  var partitionIndex:Int,
  var partitionZIndex:Int,
  var partitionWidth:Int,
  var partitionHeight:Int,
  var imageWidht:Long,
  var imageHeight:Long,
  var imageBytes:Array[Byte]
)

object NidanSQLRecord{
  def schema():Seq[String] ={
     val s = Seq(
        "imageId", "imageLevel", "imagePartitions", "partitionIndex", 
        "partitionZIndex", "partitionWidth", "partitionHeight", 
        "imageWidht", "imageHeight", "imageBytes"
      )
      return s
   }
 }


 
 
 object MainSparkSQL {
  def main(args: Array[String]): Unit = {
    // List all the options that we'll have
    // Read from HDFS save parquet
    // Process a query, what will be the output
    // This is nonsense, lets do OpenSlide once
    
    val option = args(0)
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    log.warn(s">>> $option")
    
    option match {
      case "-1" => readFromHDFS_GenerateParquet(args)
      
      case "-2" => readFromParquet_ProcessSQL(args)
      
      case "-3" => readFromHDFS_GenerateParquet_Zorder(args)
      
      case "-4" => readFromParquetZOrder(args)
      
      case "-5" => readFromHDFS_GenerateParquet_ZorderSplitItem(args)
      
      case "-6" => getImageFromPatient(args)
      
      case "-7" => NIDAN_Option7(args)
      
      // TODO Change the code
      case "-11" =>{
        val inputDir = args(1) 
        val parquetFile = args(2) 
        val format = ImageFormats.valueOf(args(3)) 
        val step = args(4).toInt 
        val sortOption = args(5)
        val storageOption = args(6)
        
        NIDAN_Option11(inputDir, parquetFile, format, step, sortOption, storageOption)
      }
      
      case "-12" => NIDAN_Option12(args)
      
      case "-13" =>{
        val inputFile = args(1)
        val tableName = args(2)
        val query = args(3)
        val outputFile = args(4)
        val imageFormat = ImageFormats.valueOf(args(5))
        
        NIDAN_Option13(inputFile, tableName, query, outputFile, imageFormat)
      }
    }
  }
 
  val calculateBoundingBox_FromBufferedImage = (data:Array[Byte]) => {
    val img = Imaging.getBufferedImage(data)
    (img.getWidth, img.getHeight)
  }
    
  
  /* Reconstruct an image from an SQL query
   * */
  def NIDAN_Option7(args:Array[String]) = {
    val input = args(1)
    val query = args(2)
    val tableName = args(3)
    val output = args(4)
    
  }
  
  
  def getImageFromPatient(args:Array[String]) = {
    val input = args(1)
    val tableName = args(2)
    val patientId = args(3).toInt
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    val log = NidanContext.log
    
    val data = sql.read.parquet(input)
    data.createOrReplaceTempView(tableName)
    
    val patientSplits = sql
    .sql(s"SELECT * " +
          s"FROM $tableName " +
          s"WHERE PatientId = $patientId " +
          "ORDER BY Index"
    )
    
    patientSplits.show
    //val names = Seq("PatientId", "Index", "Bytes", "Partitions")
    
    // Add the bounding box
    val dataRDD = patientSplits
        .rdd
        .map{row =>
          val index = row.getAs[Int]("Index")
          val bytes = row.getAs[Array[Byte]]("Bytes")
          
          (index, bytes)
        }
    
    val stuff = dataRDD
      .sortByKey(false, 16)
      .map{case (k, v) => 
        calculateBoundingBox_FromBufferedImage(v)
      }
    
    var W = 0
    var H = 0
    val d = stuff.collect()
    log.warn(">>> Bounding box calculated")
    for (box <- d){
      W += box._1
      H += box._2
      log.warn(s">>> Width ${box._1} Height ${box._2}")
    }
    
    log.warn(s">>> Box Width ${W} Height ${H}")
    
  }
  
  def readFromHDFSDir_GenerateParquet_ZOrder(args:Array[String]) = {
    val inputDir = args(1)
    val output = args(2)
    val partitions = args(3).toInt
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    val log = NidanContext.log
    val hdfs = FileSystem.get(NidanContext.getHDFSConf())
    log.setLevel(Level.WARN)
    
    val names = Seq("PatientId", "Index", "Bytes", "Partitions")
    
    val fileList = hdfs.listStatus(new Path(inputDir))
    
    var patientId:Int = 1
    for (file <- fileList){
      log.warn(">>> File to process: " + file.getPath.getName)
      val fileString = file.getPath.toString
      val buffImage = getBufferedImageFromFile(sc, fileString, partitions)
      
      val splits = sc.parallelize( 
        LinearSplit.getSplitsWithRowCol(
          buffImage.getWidth, 
          buffImage.getHeight, 
          partitions
      ), partitions)
      .map{case (k,rect,row,col) => (k,rect,row,col,partitions)}
      log.warn(">>> Splits were parallelized")
      
      // TODO Change, send filedir instead
      val BSBufferedImage = sc.broadcast(getBytesFromBufferedImage(buffImage))
      log.warn(">>> Image was broadcasted")

      val splitsRDD = splits.map{ case (key, fig, row, col, pieces) =>
        val zIndex = LinearSplit.getZIndexFromXY(row, col, pieces)
        val bytes = BSBufferedImage.value
        
        val regionImage = getBufferedImageFromRegion(fig, bytes)
        val bytes2 = getBytesFromBufferedImage(regionImage)
        
        (zIndex, bytes2, pieces)
      }.map{case (k,v,n) => (patientId, k, v, n)}
      log.warn(">>> Splits were generated")
      
      val df = sql.createDataFrame(splitsRDD).toDF(names :_*)
      df.write.mode(SaveMode.Append).format("parquet").parquet(output)
      log.warn(">>> Data was appended to the parquet file")
        
      patientId = patientId + 1
    }
    
    log.warn(">>> END")
  }
  
  def readFromParquetZOrder(args:Array[String])={
    val input = args(1)
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    
    val data = sql.read.parquet(input)
    data.show
  }
  
  def readFromParquet_ProcessSQL(args:Array[String]) = {
    val input = args(1)
    val sqlQuery = args(2)
    val tableName = args(3)
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    println(s">>> sqlQuery")
    
    val data = sql.read.parquet(input)
    data.createOrReplaceTempView(tableName)
    
    val sqlProcessedQuery = sqlQuery.map(s => if (s == '#') ' ' else s)
    println(sqlQuery)
    println(sqlProcessedQuery)
    
    val result = sql.sql(sqlProcessedQuery)
    result.show()
    
  }
  
  val getBufferedImageFromFile = (sc:SparkContext, fileDir:String, partitions:Int) => {
    val imageByteArray = sc.binaryFiles(fileDir, partitions).first._2.toArray
    Imaging.getBufferedImage(imageByteArray)
  }
  
  val getBufferedImageFromRegion = (region:Rectangle, imageByteArray:Array[Byte]) =>{
    val buffImage = Imaging.getBufferedImage(imageByteArray)
    val colorModel = buffImage.getColorModel
    val regionImage = buffImage.getData(region).createCompatibleWritableRaster
    
    new BufferedImage(colorModel, regionImage, colorModel.isAlphaPremultiplied, null)
  }
  
  val getBufferedImageFromSplitItem = (split:SplitItem, iba:Array[Byte]) =>{
    val originalImg = Imaging.getBufferedImage(iba)
    
    val width = split.rectangle.getWidth.toInt
    val height = split.rectangle.getHeight.toInt
    val xy1 = split.XY1
    val xy2 = split.XY2
      
    val buffImg = new BufferedImage(width, height, originalImg.getType)
    val g = buffImg.getGraphics
      
    g.drawImage(originalImg, 0, 0, 
      width, height, xy1._1, xy1._2, xy2._1, xy2._2, null
    )
    g.dispose()
    
    buffImg
  }
  
  val PNGgetBytesFromBufferedImage = (img:BufferedImage) => { 
    Imaging.writeImageToBytes(img, ImageFormats.PNG, null)
  }
  
  
  val bytesFromFile = (inputFile:String) =>{
    Files.readAllBytes(jPaths.get(inputFile))
  }
  
  
  val bytesFromBufferedImage = (img:BufferedImage, format:ImageFormats) => { 
    Imaging.writeImageToBytes(img, format, null)
  }
  
  
  val getBytesFromBufferedImage = (img:BufferedImage) => { 
    Imaging.writeImageToBytes(img, ImageFormats.TIFF, null)
  }
  
  def readFromHDFS_GenerateParquet_ZorderSplitItem(args:Array[String]) = {
    val input = args(1)
    val output = args(2)
    val partitions = args(3).toInt
    val patientId = args(4).toInt
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val sql = NidanContext.sqlContext
    
    // Load the ba using Imaging
    val bufferedImage = getBufferedImageFromFile(sc, input, partitions)
    val imageByteArray = getBytesFromBufferedImage(bufferedImage)
    
    // Generate the splits using the image
    val h = bufferedImage.getHeight
    val w = bufferedImage.getWidth
    val splits = LinearSplit.getSplitItems(w, h, partitions)
    
    // Broadcast image, paralleliza data
    val BSBufferedImage = sc.broadcast(imageByteArray)
    val PSplits = sc.parallelize(splits, partitions)
        .map{case item => 
          
          val originalImg = BSBufferedImage.value
          val regionImg = getBufferedImageFromSplitItem(item, originalImg)
          val bytes = getBytesFromBufferedImage(regionImg)
          
          (patientId, item.index, bytes, partitions, item.zindex)
        }
    
    val names = Seq("PatientId", "Index", "Bytes", "Partitions", "Zindex")
     
    val df = sql.createDataFrame(PSplits).toDF(names :_*)
    df.write.mode(SaveMode.Append).format("parquet").parquet(output)

    log.warn(">>> End")
  }

//    val schema = Seq(
//      "imageId", "imageLevel", "imagePartitions", "partitionIndex", 
//      "partitionZIndex", "partitionWidth", "partitionHeight", 
//      "imageWidht", "imageHeight", "imageBytes"
//    )
//  
  
  def metadataFromFileName(fileName:String):NidanSQLRecord ={
    val values = fileName.replace(".JPEG", "").split("_")
    val elements = values.length
    
    val topY = values(elements - 1).toLong
    val topX = values(elements - 2).toLong
    val buttomY = values(elements - 3).toLong
    val buttomX = values(elements - 4).toLong
    val col = values(elements - 5).toInt
    val row = values(elements - 6).toInt
    val zIndex = values(elements - 7).toInt
    val index = values(elements -8).toInt
    val level = values(elements -9).toInt
    val imageId = values(elements -10)
    
    NidanSQLRecord(
      imageId, level, 
      30, index, zIndex,
      0, 0, 0, 0,
      null
    )
  }
  
  // Write an image out of 4 tiles
  // the query retrieves 4 tiles
  def NIDAN_Option13(
      inputFile:String, 
      tableName:String,
      query:String,
      outputFile:String,
      imageFormat:ImageFormats) = {
 
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    val logger = NidanContext.log
    logger.setLevel(Level.WARN)
    val LOG = (message:String) => logger.warn(message)
    
    import sql.implicits._
    
    sql.read.parquet(inputFile).createOrReplaceTempView(tableName)
    LOG(s">> The file $inputFile was readed")
    
    // Hardcoded for JPEG
    ImageIO.setUseCache(false)
    val resultArray = sql.sql(query).map{row => row.getAs[Array[Byte]](0)}.collect
    
    // DEBUG
    LOG(s">> Images from SPARK")
    for(img <- resultArray){
      LOG(s">> \t ${img.length} bytes")
    }
    
    val result = resultArray.map{item =>       
      val buffer = ImageIO.read(new ByteArrayInputStream(item))
      LOG(s">> \t Read image from bytestream")
      
      (buffer, buffer.getWidth, buffer.getHeight)
    }
    
    val tWidthHeight = result.aggregate((0, 0))((a, b) =>
      (a._1 + b._2, a._2 + b._3),
      (x, y) => (x._1 + y._1, x._2 + y._2)
    )
    LOG(s">> Aggregate generated, final W, H => ${tWidthHeight._1} ${tWidthHeight._2}")
    
    // Is a 2x2 matrix
    val imgType = result.last._1.getType
    val buffer = new BufferedImage(
      tWidthHeight._1/2, 
      tWidthHeight._2/2, 
      result.last._1.getType
    )
    
    NidanUtils.stitchImages(result, buffer, LOG)
    
    LOG(s">> Saving JPEG image in $outputFile")
    ImageIO.write(buffer, imageFormat.toString, new File(outputFile))
    LOG(s">> Image saved")
  }
  
  val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
    
    val validate = if (new File(output).length == bytes.size) true else false
    
    (validate)
  }
  
  val writeToHDFS = (in:(Array[Byte], Configuration, String)) =>{
	  val bytes = in._1 
	  val fs = FileSystem.get(in._2) 
	  val output = in._3
	  
    val outpath = new Path(output)
    val file = fs.create(outpath)
    
    file.write(bytes);
    file.close
    
    val validate = if (fs.getFileStatus(outpath).getLen == bytes.length) true else false
    
    (validate)
  }
 
  
  
  // To retrive data from SparkSQL (one image)
  def NIDAN_Option12(args:Array[String]) = {
    val inputTable = args(1)
    val tableName = args(2)
    val query = args(3)
    val outputFile = args(4)
    val format = args(5)
    val collectFlag = args(6).toBoolean
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    val logger = NidanContext.log
    logger.setLevel(Level.WARN)
    val LOG = (message:String) => logger.warn(message)
    
    import sql.implicits._
    
    val table = sql.read.parquet(inputTable)
    table.cache
    table.createOrReplaceTempView(tableName)
    LOG(s">> Caching the data and creating the temp table $tableName")
    
    val result = sql.sql(query).map(_.getAs[Array[Byte]](0))
       .rdd
       .zipWithIndex
       .map{case (bytes, index) => (bytes, index, s"${outputFile}_${index}.$format")}
       
    val count = result.count
    
    val failed = if (collectFlag){
      val dataTiles = result.collect
      dataTiles.map(writeToLocal).filter(_ => false).size
    }else{
      val resultHDFS = result.map(i => (i._1, NidanContext.getHDFSConfSingleton, i._3))
      resultHDFS.map(writeToHDFS).filter(_ => false).count
    }
    
    LOG(s">> The operation threw $failed/$count failures")    
  }
  
  // To store the data
  /**
   * Save a list of image files stored in a directory
   * into a parquet file in HDFS using Spark. The
   * resulting parquet file will create a partition in
   * the file for each image in the directory.
   * 
   * @param inputDir The directory containing the image files
   * @param parquetFile The new parquet file to store the image files
   * @param format The format used on every image (JPEG, PNG, TIFF)
   * @param step In case you need to skip elements in the array, otherwise set it to 1
   * @param sortOption Indicate a sorting technique in the data (INDEX, ZINDEX, RANDOM)
   * @param storageOption Indicates to group the tiles in a giving threshold or not (GROUP SEQUENTIAL) 
   * **/
  def NIDAN_Option11(
      inputDir:String, 
      parquetFile:String, 
      format:ImageFormats, 
      step:Int, 
      sortOption:String,
      storageOption:String
    ) = {
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    val logger = NidanContext.log
    logger.setLevel(Level.WARN)
    val LOG = (message:String) => logger.warn(message)
    
    import sql.implicits._
    
    
    // Sorting the information from the array in an RDD
    val partitions = 2
    val patientFileList = MainOpenSlide.getMappingIOFiles1(inputDir).map{strFile => 
      (new File(strFile), metadataFromFileName(strFile))
    }
    
    LOG(s">> Using $partitions partitions in the system")
    LOG(">> Generating the SortRDD")
    val itemCount = patientFileList.length
    val rddItems = sc.parallelize(patientFileList, partitions)
    val rddPatientFileList = sortOption match{
      case "RANDOM" => rddItems.takeSample(false, itemCount)
      case "INDEX" => rddItems.sortBy(r => (r._2.imageLevel, r._2.partitionIndex)).collect()
      case "ZINDEX" => rddItems.sortBy(r => (r._2.imageLevel, r._2.partitionZIndex)).collect()
    }
    LOG(s">> SortRDD has been generated with option $sortOption")
    
    rddItems.unpersist(false)
    LOG(s">> Saving memory by unpersisting RDDs")
    
    LOG(">> Starting to generate the Parquet file")
    val pageSize = 1
    var progress = 0 
    val totalTiles = rddPatientFileList.length
    
    // 128MB HDFS BlockSize
    val threshold = 133217728
//    val threshold = 134217728
   
    LOG(s">> Storing using $storageOption option")
    storageOption match{
      case "SEQUENTIAL" => processDataSlidesSequentially(rddPatientFileList, parquetFile, sql, LOG)
      case "GROUP" => processDataSlidesGrouped(rddPatientFileList, parquetFile, sql, LOG, threshold)
    }
    
    sc.stop
  }
  
  
  /**
   * sliders one by one iterator
   */
  def processDataSlidesGrouped(
      sliders:Array[(File, NidanSQLRecord)],
      parquetFile:String,
      sql:SparkSession,
      LOG:(String) => Unit,
      threshold:Long
      ):Unit={
    
    import sql.implicits._
    
    val totalTiles = sliders.size
    val sc = sql.sparkContext
    val partitions = sc.getExecutorMemoryStatus.size
    var progress = 0
    
    var currentSize = 0L
    val buffer = new ArrayBuffer[NidanSQLRecord]()
    
    for(slide <- sliders){
      
      val batchAux = slide._2
      batchAux.imageBytes = bytesFromFile(slide._1.getAbsolutePath)
      val aproxBlockSize = batchAux.imageBytes.length + 100
      
      if(threshold <= (currentSize + aproxBlockSize)){
        val rddParquetData = sc.parallelize(buffer.toList, partitions)
        val table = sql.createDataFrame(rddParquetData).toDF(NidanSQLRecord.schema :_ *)
        // table.write.mode(SaveMode.Append).parquet(parquetFile)
        //table.write.mode(SaveMode.Append).format("orc").save(parquetFile)
        // table.write.mode(SaveMode.Append).format("orc").partitionBy("imageId", "imageLevel").save(parquetFile)
        table.write.mode(SaveMode.Append).format("orc").save(parquetFile)

        progress += buffer.size 
        LOG(s"\t>> Append to $parquetFile Progress: $progress/$totalTiles")
        
        currentSize = 0
        buffer.clear
      }
      
      buffer.append(batchAux)
      currentSize += aproxBlockSize
      
      System.gc
    }
    
    if(progress < totalTiles){
      val rddParquetData = sc.parallelize(buffer.toList, partitions)
      val table = sql.createDataFrame(rddParquetData).toDF(NidanSQLRecord.schema :_ *)
      // table.write.mode(SaveMode.Append).parquet(parquetFile)
      //  table.write.mode(SaveMode.Append).format("orc").save(parquetFile)
        // table.write.mode(SaveMode.Append).format("orc").partitionBy("imageId","imageLevel").save(parquetFile)
      table.write.mode(SaveMode.Append).format("orc").save(parquetFile)

      
      progress += buffer.size 
      LOG(s"\t>> Append to $parquetFile Progress: $progress/$totalTiles")
      buffer.clear
    }
  }
  
  def processDataSlidesSequentially(
      sliders:Array[(File, NidanSQLRecord)],
      parquetFile:String,
      sql:SparkSession,
      LOG:(String) => Unit
      ):Unit={
    
    val totalTiles = sliders.size
    val sc = sql.sparkContext
    val partitions = sc.getExecutorMemoryStatus.size
    var progress = 0
    
    for(slide <- sliders){
      System.gc
      
      val batch = slide
      
      // Generate the RDD[record] and read the file, then the parquet file
      val rddParquetData = sc.parallelize(List(batch).map{i =>
        val record = i._2
        record.imageBytes = bytesFromFile(i._1.getAbsolutePath)
        (record)
      }, partitions)
        
      val table = sql.createDataFrame(rddParquetData).toDF(NidanSQLRecord.schema :_ *)
      // table.write.mode(SaveMode.Append).parquet(parquetFile)
        // table.write.mode(SaveMode.Append).format("orc").save(parquetFile)
      table.write.mode(SaveMode.Append).format("orc").save(parquetFile)

      // Print all the images to be added:
      progress += 1
      LOG(s"\t>> Append to $parquetFile Progress: $progress/$totalTiles")
      
      
      table.unpersist(false)
      rddParquetData.unpersist(false)
      
      System.gc
    } 
  }
  
 
  
  // Form
  /*
   * inputFile +
      s"_${level}_${index}_${zIndex}_${rowCol._1}_${rowCol._2}" + 
      s"_${rectangle.buttomX}_${rectangle.buttomY}" +
      s"_${rectangle.topX}_${rectangle.topY}.png"
      
   * */
  
  def recordFromFile(inputFile:File, imageFormat:ImageFormats):NidanSQLRecord = {
    val values = inputFile.getName.replace(s".$imageFormat", "") split("_")
    val elements = values.length
    
    val topY = values(elements - 1).toLong
    val topX = values(elements - 2).toLong
    val buttomY = values(elements - 3).toLong
    val buttomX = values(elements - 4).toLong
    val col = values(elements - 5).toInt
    val row = values(elements - 6).toInt
    val zIndex = values(elements - 7).toInt
    val index = values(elements -8).toInt
    val level = values(elements -9).toInt
    val imageId = values(elements -10)
    
    //val imageBytes = bytesFromBufferedImage(Imaging.getBufferedImage(inputFile), imageFormat)
    
    val imageBytes = bytesFromFile(inputFile.getAbsolutePath)
    
    NidanSQLRecord(
      imageId, level, 
      30, index, zIndex,
      0, 0, 0, 0,
      imageBytes
    )
  }
  
  
  def readFromHDFS_GenerateParquet_Zorder(args:Array[String]) = {
    val input = args(1)
    val output = args(2)
    val partitions = args(3).toInt
    val patientId = args(4).toInt
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val sql = NidanContext.sqlContext
    
    // Load the ba using Imaging
    val bufferedImage = getBufferedImageFromFile(sc, input, partitions)
    val imageByteArray = getBytesFromBufferedImage(bufferedImage)
    
    // Generate the splits using the image
    val h = bufferedImage.getHeight
    val w = bufferedImage.getWidth
    val splits = LinearSplit.getSplitsWithRowCol(w, h, partitions)
    
    log.warn(">>> DEBUG")
    for (s_i <- splits){
      val fig = s_i._2
      log.warn(s">>> Split ${s_i._1} Rect ${fig.getX} ${fig.getY} ${fig.getWidth} ${fig.getWidth}")
    }
    
    // Broadcast image, paralleliza data
    val BSBufferedImage = sc.broadcast(imageByteArray)
    val PSplitsAux = sc.parallelize(splits, partitions)
    val PSplits = PSplitsAux.map{case (k,rect,row,col) => (k,rect,row,col,partitions)}
    
    
    // Map the splits and image to all the datanodes
    val splitsRDD = PSplits.map{ case (k, f, row, col, pieces) =>
      val key = k
      val zIndex = LinearSplit.getZIndexFromXY(row, col, pieces)
      val fig = f
      
      val bytes = BSBufferedImage.value
      val regionImage = getBufferedImageFromRegion(f, bytes)

      val bytes2 = getBytesFromBufferedImage(regionImage)
      
      (zIndex, bytes2, pieces)
    }.map{case (k,v,n) => (patientId, k, v, n)}
    
    val names = Seq("PatientId", "Index", "Bytes", "Partitions")
    val df = sql.createDataFrame(splitsRDD).toDF(names :_*)
    df.write.mode(SaveMode.Append).format("parquet").parquet(output)

    log.warn(">>> End")
  }
  
  def readFromHDFS_GenerateParquet(args:Array[String]) = {
    val input = args(1)
    val output = args(2)
    val partitions = args(3).toInt
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val sql = NidanContext.sqlContext
    
    // Read the image as a byte array
    val imageFile = sc.binaryFiles(input, partitions).first()
    val imageByteArray = imageFile._2.toArray()
    log.warn(">>> DEBUG")
    log.warn(">>> Image loaded as an array, size: " + imageByteArray.length)
    
    // Load the ba using Imaging
    val bufferedImage = Imaging.getBufferedImage(imageByteArray)
    
    // Generate the splits using the image
    val h = bufferedImage.getHeight
    val w = bufferedImage.getWidth
    val splits = LinearSplit.getSplits(w, h, partitions)
    
    log.warn(">>> DEBUG")
    for (s_i <- splits){
      val fig = s_i._2
      log.warn(s">>> Split ${s_i._1} Rect ${fig.getX} ${fig.getY} ${fig.getWidth} ${fig.getWidth}")
    }
    
    // Broadcast image, paralleliza data
    val BSBufferedImage = sc.broadcast(imageByteArray)
    val PSplits = sc.parallelize(splits, partitions)
    
    // Map the splits and image to all the datanodes
    val splitsRDD = PSplits.map{ case (k, f) =>
      val key = k
      val fig = f
      
      val bytes = BSBufferedImage.value
      val buff = Imaging.getBufferedImage(bytes)
      
      val c = buff.getColorModel
      val raster = buff.getData(fig).createCompatibleWritableRaster()
      val img2 = new BufferedImage(c, raster, c.isAlphaPremultiplied, null)
      
      val bytes2 = Imaging.writeImageToBytes(img2, ImageFormats.TIFF, null)
      (k, bytes2)
    }
        
    // Collect the splits and save them as a parquet file
    val splitsArray = splitsRDD.collect
    log.warn(">>> DEBUG")
    for (s_i <- splitsArray){
      log.warn(s"K ${s_i._1} Size ${s_i._2.length}")
    }
    
    val names = Seq("Id", "Bytes")
    val df = sql.createDataFrame(splitsRDD).toDF(names :_*)
    df.write.mode(SaveMode.Append).format("parquet").parquet(output)

    log.warn(">>> End")
  }
  
  
  def getHDFSFileSize(path:String) : Long ={
    val hdfsConf = NidanContext.getHDFSConf()
    val fileStatus = FileSystem.get(hdfsConf).getFileStatus(new Path(path))
    
    return fileStatus.getLen
  }
  
  // TODO: Find a more elegant solution
  // This is not at all elegant, but it will take me somewhere
  // Usee this as a pre-procesing stage of the image
  def baseLine_ReadHDFSFile_SaveSparkSequenceFile(args:Array[String]) = {
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val sql = NidanContext.sqlContext
    
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to HDFSSequenceFile")
    
    val input = args(1)
    val output = args(2)
    val partitions = args(3).toInt
    val fileSize = args(4).toLong
    
    val sizes = sc.parallelize(getSizesMap(fileSize, partitions), partitions)
      .map{ case (index, size, key) => (index, size, key, input)}
    
    val partitionRDD = sizes.map(it => fileToKeyArrayPair_Stream(it._1, it._2, it._3, it._4))
    val sizesPRDD = partitionRDD
        .flatMap(it => it._2)
        .zipWithIndex()
        .map{case (v, k) => (k, v)}
        
    // TODO Avoid it, and only focus on the parquet file
//    sizesPRDD.saveAsSequenceFile(output)
    
    val names = Seq("Id", "Blob")
    val sqlSizesPRDD = sql.createDataFrame(sizesPRDD)
      .toDF(names : _*)
      .write
      .format("parquet")
      .parquet(output)
      
    
    
    
    log.warn(">>> According to the computations: ")
    log.warn(s">>> Original file size $fileSize ")
    
  }
  
  // TODO Remember to settle the key at the end
  val fileToKeyArrayPair_Stream = (index:Long, size:Long, key:Long, fileDir:String) => {
      val conf = NidanContext.getHDFSConf
      println(">>> Configuration created")
      
      val fs = FileSystem.get(conf)
      val hdfsInputFile = fs.open(new Path(fileDir))
      println(">>> File System accessed")
      
      if (index > 0) hdfsInputFile.skip(index-1)
      
      val arrays = new ArrayBuffer[Array[Byte]]()
      var bytesRemaining = size
      val sizeLimit = Int.MaxValue / 4 // Test this as max value
      
      while (bytesRemaining > 0){
        val memorySize = if (bytesRemaining > sizeLimit) sizeLimit else bytesRemaining.toInt
        val buffer = new Array[Byte](memorySize)
        
        val bytesRead = hdfsInputFile.read(buffer)
        arrays.append(buffer.slice(0, bytesRead))
        bytesRemaining = bytesRemaining - bytesRead
        println(s">>> Chunk read. R[$bytesRead] E[$memorySize]")
      }
      
      hdfsInputFile.close
      (key, arrays.toArray)
  }
  
  // index, size, key
  def getSizesMap(arrayLen:Long, splits:Int) : Array[(Long,Long,Long)]={
    val sizesMap = new Array[(Long,Long,Long)](splits)
    val gap = arrayLen / splits
    val extra = arrayLen % splits
    
    var i = 1
    sizesMap.update(0, (0, gap + extra, 0))
    for(i <- 1 to splits -1){
      sizesMap.update(i, (i*gap, gap, i.toLong))
    }
    
    return sizesMap
  }
  
  
}
