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
import nidan.spark.NidanRecord
import org.apache.spark.sql.types._


object SparkTileGenerator2 {

  def getSchema = {
    new StructType()
    .add(StructField("fileId", StringType, true))
    .add(StructField("level", IntegerType, true))
    .add(StructField("x", LongType, true))
    .add(StructField("y", LongType, true))
    .add(StructField("tileWidth", LongType, true))
    .add(StructField("tileHeight", LongType, true))
    .add(StructField("row", IntegerType, true))
    .add(StructField("col", IntegerType, true))
    .add(StructField("seqIndex", IntegerType, true))
    .add(StructField("zIndex", IntegerType, true))
    .add(StructField("cIndex", IntegerType, true))
    .add(StructField("totalRows", IntegerType, true))
    .add(StructField("totalCols", IntegerType, true))
    .add(StructField("bytes", BinaryType, true))
  }
  
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
    val (rddTiles, timeGenerateTiles) = NidanUtils.timeIt{ 
      sc.parallelize(squareMatrix(dim, n))
      .map(coord2meta(localFile, localOutput, level, n, _))
      .repartition(clusterNodes)
      .mapPartitionsWithIndex(partitionGroups)
    }
    
    // Action plan:
    
    // 1. Count successes
    val (errors, timeCount) = NidanUtils.timeIt{ 
      rddTiles.filter(_._1 == 1).count
    }
    
    // 2. Change to Dataframe 
    val (dfTiles, timeDF) = NidanUtils.timeIt{
      sql.createDataFrame(
        rddTiles.map(item => Row(toORCRecord(item._2, item._3))), 
        getSchema
      )
    }
    
    // 3. Write to the database
    val (dfWrite, timeWrite) = NidanUtils.timeIt{
      dfTiles.write.mode("append").partitionBy("fileId").orc(hdfsDB)
    }
    
    
    logger.info(s">> Time to write local tiles    : ${timeCount} secs, errors ${errors}")
    logger.info(s">> Time to switch to Dataframe  : ${timeDF} secs")
    logger.info(s">> Time to write to HDFS ORC DB : ${timeWrite} secs")
    
  }
  
  def toORCRecord(bytes:Array[Byte], meta:TileMetadata) = {
    val record = new NidanRecord(
        meta.file,
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
    
    record
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
    val file = it.toSeq.head._1
    val localOutput = it.toSeq.head._2
    val os = new OpenSlide(new File(file))
    val errors = it.map(el => writeTileLocal(os, el._3, localOutput))
    
    errors
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
    
    val bytes = Files.readAllBytes(Paths.get(outputFile))
    
    (error, bytes, meta)
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