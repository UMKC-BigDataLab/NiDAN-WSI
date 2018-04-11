/*
 * UMKC BigData Research Lab
 * Author: Daniel E. Lopez Barron
 * 
 * This class deals with the BaseLine Approach:
 * Having a WSI, treat it as a regular file, split it
 * into sequence equi-sized chunks and store them in
 * HDFS.
*/
package nidan.main

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import nidan.io.NidanContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.ByteWritable
import java.io.FileInputStream
import java.io.File
import java.io.FileOutputStream
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import java.io.BufferedOutputStream
import org.apache.hadoop.io.LongWritable
import java.io.ByteArrayOutputStream
import org.apache.spark.sql.Row
import nidan.models.Configuration
import org.apache.spark.sql.SaveMode



object MainBaseLine {
  
  def main(args: Array[String]): Unit = {
    val option = args(0)
    
    option match{
      case "-1" => baseLine_ReadHDFSFile_SaveSparkSequenceFile(args)
        /* BASE LINE CASE
         * Read a normal file stored in HDFS
         * Save a Spark SequenceFile with (key, Array[Byte])
        */
      case "-2" => baseLine_ReadSparkSequenceFile_SaveLocalFile(args)
        /* BASE LINE CASE
         * Read a Spark SequenceFile with (key, Array[Byte])
         * Save a regular File in the local file system
        */
      case "-3" => call_baseLine_ReadHDFSFile_SaveSparkSequenceFile(args)
        /* BASE LINE CASE
         * Read from an HDFS Directory a sequence of images and
         * save them as different sequence files in HDFS
        */
      case "-4" => call_baseLine_ReadSparkSequenceFile_SaveLocalFile(args)
        /* BASE LINE CASE
         * Read from an HDFS Directory a sequence of Sequence files and
         * save them as a binary file in the local directory
        */
      case "-5" => call_baseLine_ReadSparkSequenceFile_SparkSQL(args)
        /* BASE LINE CASE
         * Read from an HDFS Directory a sequence of Sequence files and
         * save them as a binary file in the local directory
        */
    }
    
  }
  
  def call_baseLine_ReadSparkSequenceFile_SparkSQL(args:Array[String]):Unit={
    val inputDir = args(1)
    val outputDir = args(2)
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val hdfs = FileSystem.get(NidanContext.getHDFSConf())
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to CALL BASELINE MULTIPLE TIMES")
    
    var sequenceNumber:Int = 1
    
    val fileList = hdfs.listStatus(new Path(inputDir))
    for (file <- fileList){
      log.warn(">>> File to process: " + file.getPath.getName)
      
      val fileString = file.getPath.toString
      val outputFile = s"$outputDir/output_$sequenceNumber.bin"
      
      val newArgs = Array[String](
          "useless", 
          fileString, 
          outputFile
      )
      
      log.warn(">>> Calling baseLine_ReadSparkSequenceFile_SaveLocalFile")
      log.warn(">>> Arguments: ")
      newArgs.foreach(x => log.warn(">>> # " + x))
      log.warn(">>> Running ")
      
      //baseLine_ReadSparkSequenceFile_SaveLocalFile(newArgs)
      baseLine_ReadSparkSequenceFile_SparkSQL(newArgs)
      sequenceNumber = sequenceNumber + 1
    }
    
  }
  
  def baseLine_ReadSparkSequenceFile_SparkSQL(args:Array[String]):Unit={
    val input = args(1)
    val output = args(2)
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val sql = NidanContext.sqlContext
    
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to HDFSSequenceFile")
    
    val sFile = sc.sequenceFile(input, classOf[LongWritable], classOf[BytesWritable])
      .map{case (k, v) =>
        val byteArray = v.getBytes
        
        (byteArray)
      }.zipWithIndex.map{case (v, k) => (k , v)}
    
    val df = sql.createDataFrame(sFile)
    
    val names = Seq("Id", "Bytes")
    val newDf = df.toDF(names: _*)
    
    log.warn(newDf.schema)
    newDf.show()
    newDf.write.format("parquet").mode(SaveMode.Append).parquet(output)
    
  }
  
  def call_baseLine_ReadSparkSequenceFile_SaveLocalFile(args:Array[String]):Unit={
    val inputDir = args(1)
    val outputDir = args(2)
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    val hdfs = FileSystem.get(NidanContext.getHDFSConf())
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to CALL BASELINE MULTIPLE TIMES")
    
    var sequenceNumber:Int = 1
    
    val fileList = hdfs.listStatus(new Path(inputDir))
    for (file <- fileList){
      log.warn(">>> File to process: " + file.getPath.getName)
      
      val fileString = file.getPath.toString
      val outputFile = s"$outputDir/output_$sequenceNumber.bin"
      
      val newArgs = Array[String](
          "useless", 
          fileString, 
          outputFile
      )
      
      log.warn(">>> Calling baseLine_ReadSparkSequenceFile_SaveLocalFile")
      log.warn(">>> Arguments: ")
      newArgs.foreach(x => log.warn(">>> # " + x))
      log.warn(">>> Running ")
      
      baseLine_ReadSparkSequenceFile_SaveLocalFile(newArgs)
//      baseLine_ReadSparkSequenceFile_SparkSQL(newArgs)
      sequenceNumber = sequenceNumber + 1
    }
    
  }
  
  def baseLine_ReadSparkSequenceFile_SaveLocalFile(args:Array[String]):Unit={
    val input = args(1)
    val output = args(2)
    
    val sc = NidanContext.sparkContext
    val log = NidanContext.log
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to HDFSSequenceFile")
    
    val bytesRead = sc.accumulator(0L, "FileSize Test")
    
    val sFile = sc.sequenceFile(input, classOf[LongWritable], classOf[BytesWritable])
      .map{case (k, v) =>
        val byteArray = v.getBytes
        bytesRead += byteArray.length
        
        (byteArray)
      }.collect
      
    log.warn(">>> File collected, proceding to save ")
    log.warn(s">>> File collected with ${sFile.size} elements")
    log.warn(s">>> Bytes read were ${bytesRead.value}")
    
    
    // Should I stop the SparkContext here?
    
    val outFile = new File(output)
    if(!outFile.exists()) outFile.createNewFile()
      
    val outStream = new FileOutputStream(outFile)
    val buffer = new BufferedOutputStream(outStream)
      
    var parti = 1
    for(partition <- sFile){
      buffer.write(partition)
      log.warn(s">>> Partition $parti saved")
      
      parti = parti + 1
    }
      
    buffer.flush
    buffer.close
  }
  
  // TODO: Change the name
  // This will call the base line, we will test multiple files
  def call_baseLine_ReadHDFSFile_SaveSparkSequenceFile(args:Array[String]) = {
    val inputDir = args(1)
    val outputDir = args(2)
    val partitions = args(3).toInt
    
    val log = NidanContext.log
    val hdfs = FileSystem.get(NidanContext.getHDFSConf())
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to CALL BASELINE MULTIPLE TIMES")
    
    var sequenceNumber:Int = 1
    val fileList = hdfs.listFiles(new Path(inputDir), false) 
    while (fileList.hasNext()){
      val fileString = fileList.next.getPath.toString
      val fileSize = getHDFSFileSize(fileString)
      val outputFile = s"$outputDir/output_${sequenceNumber}.seq"
    
      val newArgs = Array[String](
          "useless", 
          fileString, 
          outputFile, 
          partitions.toString, 
          fileSize.toString
      )
      
      log.warn(">>> Calling baseLine_ReadHDFSFile_SaveSparkSequenceFile")
      log.warn(">>> Arguments: ")
      newArgs.foreach(x => log.warn(">>> # " + x))
      log.warn(">>> Running ")
      
      baseLine_ReadHDFSFile_SaveSparkSequenceFile(newArgs)
      sequenceNumber = sequenceNumber + 1
    }
    
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
    log.setLevel(Level.WARN)
    log.warn("## Nidan, LocalFile to HDFSSequenceFile")
    
    val input = args(1)
    val output = args(2)
    val partitions = args(3).toInt
    val fileSize = args(4).toLong
    
    // Simple test
    val fileSizeTest = sc.accumulator(0L, "fileSizeTest_name")
    // Simple test
    
    val sizes = sc.parallelize(getSizesMap(fileSize, partitions), partitions)
      .map{ case (index, size, key) => (index, size, key, input)}
    
    
    // This was outside the method, but Is need the accumulator for
    // a simple test
    val fileToKeyArrayPair = (index:Long, size:Long, key:Long, fileDir:String) => {
      val conf = NidanContext.getHDFSConf
      println(">>> Configuration created")
      
      val fs = FileSystem.get(conf)
      val hdfsInputFile = fs.open(new Path(fileDir))
      println(">>> File System accessed")
      
      // TODO: Change this for a outputbufferstream or sim
      val buffer = new Array[Byte](size.toInt)
      println(">>> Buffer was created")
      
      if (index > 0) hdfsInputFile.skip(index-1)
      
      // Simple test
      val bytesRead = hdfsInputFile.read(buffer)
      fileSizeTest += bytesRead
      println(s">>> Comparison: R[$bytesRead] E[${size}]")
      // Simple test
      
      hdfsInputFile.close
      
      (key, buffer)
    }
    
//    val partitionRDD = sizes.map(it => fileToKeyArrayPair(it._1, it._2, it._3, it._4))
    
    val partitionRDD = sizes.map(it => fileToKeyArrayPair_Stream(it._1, it._2, it._3, it._4))
    val sizesPRDD = partitionRDD
        .flatMap(it => it._2)
        .zipWithIndex()
        .map{case (v, k) => (k, v)}
        .saveAsSequenceFile(output)
    
    log.warn(">>> According to the computations: ")
    log.warn(s">>> Original file size $fileSize ")
    log.warn(s">>> Uploaded file size ${fileSizeTest.value} ")
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
  
  
//  case class Split(
//    fileId:Int,
//    splitId:Int, 
//    fileName:String, 
//    bytes:Array[Byte]
//    ) extends java.io.Serializable
//  
//  // TODO: Find a more elegant solution
//  // This is not at all elegant, but it will take me somewhere
//  // Usee this as a pre-procesing stage of the image
//  def baseLine_LocalFile2KVPFile(args:Array[String]) = {
//    val input = args(1)
//    val output = args(2)
//    val partitions = args(3).toInt
//    
//    val file = new File(input)
//    val sizes = getSizesMap(file.length, partitions)
//    val fileStream = new FileInputStream(file)
//    val partitionsList = new ArrayBuffer[(Int,Array[Byte])](partitions)
//    
//    println("## Nidan ")
//    println(s">>> Reading: $input")
//    println(s">>> Writing: $output into # $partitions parts")
//    for(sizei <- sizes){
//      println(s">>> I:${sizei._1} S:${sizei._2} K:${sizei._3}")
//      println(s">>> I:${sizei._1.toInt} S:${sizei._2.toInt} K:${sizei._3.toInt}")
//    }
//    
//    // Let's see if we have enough memory
//    for(sizei <- sizes){
//      val buffer = new Array[Byte](sizei._2.toInt)
//      fileStream.read(buffer)
//      
//      partitionsList.append((sizei._3, buffer))
//      println(s">>> Added partition # ${sizei._3}")
//    }
//    fileStream.close
//    
//    // To crazy to work
//    println(">>> Initializing Spark")
//    val conf = new SparkConf()
//    val sc = new SparkContext(conf)
//    sc.parallelize(partitionsList.toList, partitions)
//      .saveAsSequenceFile(output)
//    
//    sc.stop
//    
//    println("## End")
//  }
//  
//  
//  // Distribute the datastream in the nodes and skip the bytes
//  def readFromHDFS_SaveHDFS_WithSplits_Skiping(args:Array[String]) ={
//    val input = args(1)
//    val output = args(2)
//    val partitions = args(3).toInt
//    val imageSize = args(4).toLong
//    
//    // Spark's objects
//    val sc = NidanContext.sparkContext
//    val log = NidanContext.log
//    log.setLevel(Level.WARN)
//    
//    log.warn("## NIDAN: Read a WSI file in each worker node as a stream of data, save it as a sequence file")
//    
//    val inputFiles = sc.binaryFiles(input, partitions)
//    val sizeMap = sc.parallelize(getSizesMap(imageSize, partitions), partitions).cache()
//    
//    // Let's do some rock and roll
//    for(file <- inputFiles.collect){
//      // DEBUG MESSAGE
//      log.warn("## DEBUG")
//      log.warn(s">>> About to access: ${file._1}")
//      log.warn(">>> The file will be splitted in")
//      for(element <- sizeMap.collect){
//        log.warn(s">>> Split[${element._3}] From ${element._1} to ${element._1 + element._2}")
//      }
//      log.warn("## END DEBUG")
//      
//      // Try to load the image in memmory
//      val bcImageBytes = sc.broadcast(file._2.toArray)
//      
//      // TODO: Map the stream and the chunks to the nodes
//      // TODO: Map the file and the chunks to the nodes
////      val partitionedImage = sizeMap
////          .map{ case (index, size, key) =>
////            // TODO: Read the specific chunk in the node
////              
////            val buffer = new Array[Byte](size.toInt)
////            val nodeStream = bcImageBytes
////            val image = nodeStream.first()._2.open
////            log2.warn(">>> Inside the Worker, Memory assigned")
////            if(index > 1) image.skip(index-1)
////            image.read(buffer)
////            log2.warn(">>> Inside the Worker, Partition read it")
////            
////            (key, buffer)
////          }
////      
//      // TODO: Concentrate everything and save it in a sequence file
////      partitionedImage.saveAsSequenceFile(output)
//    }
//    
//    println(">>> END")
//  }
//  
//  def readFromHDFS_SaveHDFS_WithSplits(args:Array[String]) = {
//    val input = args(1)
//    val output = args(2)
//    val splits = args(3).toInt
//    
//    // Get the cluster enviroment variables
//    val sc = NidanContext.sparkContext
//    val log = NidanContext.log
//    log.setLevel(Level.WARN)
//    
//    var fileId:Int = 0
//    val fileSplits = ArrayBuffer[RDD[Split]]()
//    val inputFiles = sc.binaryFiles(input, splits)
//    val outputFile = "output.seq"
//     
//    for(file <- inputFiles.toArray){
//      val stream = file._2.toArray
//      
//      val arrayLen = stream.length 
//      
//      val sizesMap = getSizesMap(arrayLen, splits)
//      val streamB = sc.broadcast(stream)
////      val arrayData = sc.parallelize(sizesMap, splits)
////        .map{case (index, size, key) => (key, streamB.value.slice(index, size))}
////        .saveAsSequenceFile(s"$output/${fileId}_$outputFile")
//        
//      fileId = fileId + 1
//      log.warn(s">>> Sequence file saved for file: ${file._1}")
//    }
//  }
//  
//  // index, size, key
//  def getSizesMap(arrayLen:Long, splits:Int) : Array[(Long,Long,Int)]={
//    val sizesMap = new Array[(Long,Long,Int)](splits)
//    val gap = arrayLen / splits
//    val extra = arrayLen % splits
//    
//    var i = 1
//    sizesMap.update(0, (0, gap + extra, 0))
//    for(i <- 1 to splits -1){
//      sizesMap.update(i, (i*gap, gap, i))
//    }
//    
//    return sizesMap
//  }
//  
//  val generateFileSplits = (fileId:Int, file:(String,PortableDataStream), 
//      rddStorage:StorageLevel, sc:SparkContext) => {
//    val fileSplits = new ArrayBuffer[RDD[Split]]()
//    val fileName = file._1
//    val fileStream = file._2.open
//    
//    var bytesRead = 0
//    var splitId = 0
//    do{
//      val split = new Array[Byte](HDFS_BLOCK_SIZE)
//      bytesRead = fileStream.read(split)
//      
//      val rdd = sc.parallelize(Seq(Split(fileId,splitId, fileName, split)), HDFS_NODES_SIZE)
//      rdd.persist(rddStorage)
//      
//      fileSplits.append(rdd)
//      splitId = splitId + 1
//    }while(bytesRead != -1)
//    
//    fileSplits.toArray
//  }
//  
//  // This experiment uses Spark
//  def readFromHDFS_SaveInHDFS(args:Array[String]){
//    val inputHDFSDir = args(1)
//    val outputHDFSDir = args(2)
//    
//    val sc = NidanContext.sparkContext 
//    
//    // WARNING
//    // We assume that the inputdir only has WSI files.
//    val fileListL = sc.binaryFiles(inputHDFSDir)
//    val fileList = sc.broadcast(fileListL)
//    
//    // Split into:
//    // (fileID, splitID, fileName, Bytes). Go through all the files generate
//    // a sequence of splits according to the block size, and save them in HDFS.
//    val fileSplits = new ArrayBuffer[RDD[Split]]()
//    var fileId = 0
//    
//    for (file <- fileList.value.toArray()){
////      val arr = generateFileSplits(fileId, file, StorageLevel.DISK_ONLY, sc)
////      arr.foreach { elm => fileSplits += elm }
//      
//      val fileName = file._1
//      val fileStream = file._2.open
//      
//      var bytesRead = 0
//      var splitId = 0
//      do{
//        val split = new Array[Byte](HDFS_BLOCK_SIZE)
//        bytesRead = fileStream.read(split)
//        
//        val rdd = sc.parallelize(Seq(Split(fileId,splitId, fileName, split)), HDFS_NODES_SIZE)
//        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
////        rdd.persist(StorageLevel.MEMORY_AND_DISK)
////        rdd.persist(StorageLevel.DISK_ONLY)
//        
//        fileSplits.append(rdd)
//        splitId = splitId + 1
//      }while(bytesRead != -1)
//      
//      fileId = fileId + 1
//    }
//    
//    // The RDD ready to distribute
//    val fileRDD = sc.parallelize(fileSplits.toSeq, HDFS_NODES_SIZE)
//      .zipWithIndex()
//      .map{ case (k, v) => (v, k)}
//      
//    fileRDD.saveAsObjectFile(outputHDFSDir)
//  } 
}