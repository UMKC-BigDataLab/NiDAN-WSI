package nidan.test

import java.awt.image.BufferedImage
import java.awt.image.DataBufferInt
import java.awt.image.WritableRaster
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.commons.imaging.ImageFormats
import org.openslide.OpenSlide

import javax.imageio.IIOImage
import javax.imageio.ImageIO
import javax.imageio.ImageWriteParam
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import javax.imageio.stream.FileImageOutputStream
import nidan.io.NidanContext
import nidan.regions.CoordinateGenerator
import nidan.spark.NidanRecord
import nidan.tiles.Index
import nidan.tiles.TileDimension
import nidan.tiles.TileMetadata
import nidan.tiles.TilePoint
import nidan.utils.NidanUtils
import org.apache.spark.sql.Row
import nidan.spark.NidanRecord
import org.apache.spark.sql.types.{StringType,LongType}
import org.apache.spark.sql.types.{IntegerType,BinaryType}
import org.apache.spark.sql.types.{StructType,StructField}
import org.apache.spark.api.java.StorageLevels


object SparkTileGenerator2 {

  def getSchema = {
    new StructType(Array(
    StructField("fileId", StringType, true)
    ,StructField("level", IntegerType, true)
    ,StructField("x", LongType, true)
    ,StructField("y", LongType, true)
    ,StructField("tileWidth", LongType, true)
    ,StructField("tileHeight", LongType, true)
    ,StructField("row", IntegerType, true)
    ,StructField("col", IntegerType, true)
    ,StructField("seqIndex", IntegerType, true)
    ,StructField("zIndex", IntegerType, true)
    ,StructField("cIndex", IntegerType, true)
    ,StructField("totalRows", IntegerType, true)
    ,StructField("totalCols", IntegerType, true)
    ,StructField("bytes", BinaryType, true)))
  }
  
  def getSchema2 = {
    Seq[String](
      "fileId","level","x","y","tileWidth","tileHeight",
      "row","col","seqIndex","zIndex","cIndex","totalRows",
      "totalCols","bytes"
    )
  }
//  def main(args: Array[String]): Unit = {
//    val fileName = args(0)
//    val localInput = args(1)
//    val localOutput = args(2)
//    val hdfsDB = args(3)
//    
//    val n = args(4).toInt //Number of partitionss
//    val level = args(5).toInt
//    val clusterNodes = args(6).toInt
//    
//    val logger = NidanContext.log
//    val sc = NidanContext.sparkContext
//    val sql = NidanContext.sqlContext
//    import sql.implicits._
//    
//    // Getting the nodes in a fancy way ;)
//    val host = sc.getConf.get("spark.driver.host")
//    val nodes = sc.getExecutorMemoryStatus.map(_._1).filter(!_.contains(host)).size
//    
//    logger.info(s">> ClusterNodes ${clusterNodes} and Nodes ${nodes} are equal")
//    
//    // Let's rock it
//    val squareMatrix = CoordinateGenerator.squareMatrixfromDimension(_, _)
//    val localFile = localInput + "/" + fileName
//    val dim = tileDimension(new File(localFile))
//    
//    /*** Operations
//     * 1. Get the tile coordinate
//     * 2. Generate a TileMetadata instance from the coordinates
//     * 3. Repartition the RDD with as many nodes in the cluster
//     * 4. Extract the tiles in groups to create less OpenSlide instances
//     */
//    val (rddTiles, timeGenerateTiles) = NidanUtils.timeIt{ 
//      sc.parallelize(squareMatrix(dim, n))
//      .map(coord2meta(localFile, localOutput, level, n, _))
//      .repartition(clusterNodes)
//      .mapPartitionsWithIndex(partitionGroups)
//    }
//    
//    val count = rddTiles.count
//    logger.info(s">> Total tiles before grouping: ${count}")
//    
////    // Action plan:
////    
////    // 1. Count successes
////    val (errors, timeCount) = NidanUtils.timeIt{ 
////      rddTiles.filter(_._1 == 1).count
////    }
////    
////    // 2. Change to Dataframe 
////    val dfTiles = sql.createDataFrame(
////      rddTiles.map(item => toORCRecord(item._2, item._3, fileName)), 
////      getSchema
////    )
////    
////    
////    // 3. Write to the database
////    val (dfWrite, timeWrite) = NidanUtils.timeIt{
////      dfTiles.write.mode("append").orc(hdfsDB)
////    }
////    
////    
////    logger.info(s">> Time to write local tiles    : ${timeCount} secs, errors ${errors}")
//////    logger.info(s">> Time to switch to Dataframe  : ${timeDF} secs")
////    logger.info(s">> Time to write to HDFS ORC DB : ${timeWrite} secs")
//    
//  }

  
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val localInput = args(1)
    val localOutput = args(2)
    val hdfsDB = args(3)
    
    val n = args(4).toInt //Number of partitionss
    val level = args(5).toInt
    val clusterNodes = args(6).toInt
    
    val logger = NidanContext.log
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    import sql.implicits._
    
    // Getting the nodes in a fancy way ;)
    val host = sc.getConf.get("spark.driver.host")
    val nodes = sc.getExecutorMemoryStatus.map(_._1).filter(!_.contains(host)).size
    
    logger.info(s">> ClusterNodes ${clusterNodes} and Nodes ${nodes} are equal")
    
    // Let's rock it
    val squareMatrix = CoordinateGenerator.squareMatrixfromDimension(_, _)
    val localFile = localInput + "/" + fileName
    val dim = tileDimension(new File(localFile))
    
    /*** Operations
     * 1. Get the tile coordinate
     * 2. Generate a TileMetadata instance from the coordinates
     * 3. Repartition the RDD with as many nodes in the cluster
     * 4. Extract the tiles in groups to create less OpenSlide instances
     */
    val (rddTiles1, timeGenerateTiles) = NidanUtils.timeIt{ 
      sc.parallelize(squareMatrix(dim, n))
      .repartition(clusterNodes)
      .map(coord2meta(localFile, localOutput, level, n, _))
      .distinct
      .persist(StorageLevels.MEMORY_AND_DISK)
    }
    
    val totalEls = rddTiles1.mapPartitions(iterator => {List(iterator.size).toIterator}, true).sum
    
    // Write the data in local
    val (rddWriteTiles, timeW) = NidanUtils.timeIt{ 
//      rddTiles1.mapPartitions(partitionGroups1, true)
      rddTiles1.foreachPartition(partitionGroups1)
    }
    
    
    
//    // Get the binary data from local
//    val (rddReadTile, timeR) = NidanUtils.timeIt{
//      rddTiles1.mapPartitions(iteratorGetBytes)
//    }
//    
//    
    
//    val originalTotal = rddTiles1.count
//    val writes = rddWriteTiles.map(el => el._2).sum
//    val errors = rddWriteTiles.map(el => el._1).sum
    
//    val reads
//    val total = data.count
//    val error = data.filter(_._1 == 1).count
//    val success = data.filter(_._1 == 0).count
//    logger.info(s">> Original Total: ${totalEls}")
//    logger.info(s">> Total writes: ${writes}")
//    logger.info(s">> Errors: ${errors}")
//    logger.info(s">> Success: ${success}")
//    
//    // 2. Change to Dataframe 
//    val dfTiles = sql.createDataFrame(
//      rddTiles.map(item => toORCRecord(item._2, item._3, fileName)), 
//      getSchema
//    )
//    
//    
//    // 3. Write to the database
//    val (dfWrite, timeWrite) = NidanUtils.timeIt{
//      dfTiles.write.mode("append").partitionBy("fileId", "level").orc(hdfsDB)
//    }
//    
//    
//    logger.info(s">> Time to write local tiles    : ${timeCount} secs, errors ${errors}")
////    logger.info(s">> Time to switch to Dataframe  : ${timeDF} secs")
//    logger.info(s">> Time to write to HDFS ORC DB : ${timeWrite} secs")
    
  }
  
  def iteratorGetBytes(it:Iterator[(String,String,TileMetadata)]) = {
    it.map(e => getBytes(e._1, e._2, e._3))
  }
  
  def getBytes(file:String, out:String, meta:TileMetadata) = {
    val file = out + new File(meta.toString).getName
    val bytes = Files.readAllBytes(Paths.get(file))
    (bytes, meta)
  }
  
  def toORCRecord(bytes:Array[Byte], meta:TileMetadata, file:String) = {
    Row(
        file,
        meta.level,
        meta.position.x,
        meta.position.y,
        meta.dimension.width,
        meta.dimension.height,
        meta.index.row,
        meta.index.col,
        meta.index.seq,
        meta.index.z,
        meta.index.c,
        meta.rowsCols._1,
        meta.rowsCols._2,
        bytes
        )
    
    
  }
  
  def tileDimension(svsFile:File):TileDimension = {
    val os = new OpenSlide(svsFile)
    val dim = new TileDimension(os.getLevel0Width, os.getLevel0Height)
    os.close
    
    dim
  }
  
  def coord2meta(file:String, outdir:String, level:Int, n:Int, coord:(Int,Int,Int, TilePoint,TileDimension)) = {
    val index = new Index(coord._2, coord._3, coord._1, coord._1, coord._1)
    val meta = new TileMetadata(file, file, level,coord._4, coord._5, index,(n,n))
    
    (file, outdir, meta)
  }
  
  def partitionGroups(k:Int, it:Iterator[(String, String, TileMetadata)]) = {
    val krdd = it.map(el => (k, el)).toIterator
    krdd
  }
  
  def partitionGroups1(it:Iterator[(String, String, TileMetadata)]):Iterator[(Int,Int)] = {
    val file = it.toSeq.head._1
    val localOutput = it.toSeq.head._2
    val os = new OpenSlide(new File(file))
    
    val data = it.toList
    val errors = data.map(el => (writeTileLocal(os, el._3, localOutput), data.size))
    
    errors.toIterator
  }
  
  // Take a tile object and write it down in the local storage
  def writeTileLocal(oslide:OpenSlide, meta:TileMetadata, outputDir:String) = {
    val outputFile = outputDir + new File(meta.toString).getName
    
    // Get the tile information
    val w = meta.dimension.width.toInt
    val h = meta.dimension.height.toInt
    val point = meta.position
    
    // Build up canvas to write the tile
    val canvas = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
    val data = canvas.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
    val graph = canvas.getGraphics
    
    // Extract and write the tile
    oslide.paintRegionARGB(data, point.x, point.y, meta.level, w, h)
    graph.drawImage(canvas, 0, 0, w, h, null)
    writeJPEG(canvas.getRaster, outputFile, ImageFormats.JPEG)
    graph.dispose
    
    // Check if file was written, 0 => no error, 1 => means error
    val img = new File(outputFile)
    val error = if (img.exists) 0 else 1
    
    (error)
//    val bytes = Files.readAllBytes(Paths.get(outputFile))
//    (error, bytes, meta)
  }
  
  def writeJPEG(raster:WritableRaster, output:String, format:ImageFormats){
    val jpegParams = new JPEGImageWriteParam(null)
    jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    jpegParams.setCompressionQuality(0.9f)
    
    val writer = ImageIO.getImageWritersByFormatName(format.toString).next()
    writer.setOutput(new FileImageOutputStream(new File(output)))
    writer.write(null, new IIOImage(raster, null, null), jpegParams);
  } 
}