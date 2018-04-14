package nidan.test

import nidan.io.NidanContext
import org.openslide.OpenSlide
import java.io.File
import nidan.regions.CoordinateGenerator
import nidan.tiles.{TileDimension,TilePoint}
import nidan.tiles.TileMetadata
import java.awt.image.BufferedImage
import java.awt.image.DataBufferInt
import org.apache.commons.imaging.Imaging
import org.apache.commons.imaging.ImageFormats
import javax.imageio.ImageIO
import java.awt.image.WritableRaster
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import javax.imageio.ImageWriteParam
import javax.imageio.IIOImage
import javax.imageio.stream.FileImageOutputStream


object SparkTileGenerator2 {

  
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val localInput = args(1)
    val localOutput = args(2)
    
    val n = args(3).toInt //Number of partitionss
    val level = args(4).toInt
    val clusterNodes = args(5).toInt
    
    val logger = NidanContext.log
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    
    // Getting the nodes in a fancy way ;)
    val host = sc.getConf.get("spark.driver.host")
    val nodes = sc.getExecutorMemoryStatus.map(_._1).filter(!_.contains(host)).size
    if(nodes == clusterNodes) 
      logger.info(s">> ClusterNodes ${clusterNodes} and Nodes ${nodes} are equal")
    
    // Let's rock it
    val squareMatrix = CoordinateGenerator.squareMatrixfromDimension(_, _)
    val localFile = localInput + "/" + fileName
    val dim = tileDimension(new File(localFile))
    
    /*** Operations
     * 1. Get the tile coordinate
     * 2. Repartition the RDD with as many nodes in the cluster
     * 3. Extract the tiles in groups to create less OpenSlide instances
     * 4. Get the errors that occour
     * 5. Count the errors
     */
    val coords = sc.parallelize(squareMatrix(dim, n))
      .map(coord2meta(localFile, localOutput, level, n, _))
      .repartition(clusterNodes)
      .mapPartitionsWithIndex(partitionGroups)
      .filter(_ == 1)
      .count
      
    logger.info(s"Errors generated: $coords")
  }
  def tileDimension(svsFile:File):TileDimension = {
    val os = new OpenSlide(svsFile)
    val dim = new TileDimension(os.getLevel0Width, os.getLevel0Height)
    os.close
    
    dim
  }
  
  def coord2meta(file:String, outdir:String, level:Int, n:Int, coord:(Int,Int,Int, TilePoint,TileDimension)) = {
    val index = new nidan.tiles.Index(coord._2, coord._3, coord._1, coord._1, coord._1)
    val meta = new TileMetadata(file, file, level,coord._4, coord._5, index,(n,n))
    
    (file, outdir, meta)
  }
  
  def partitionGroups(k:Int, it:Iterator[(String, String, TileMetadata)]) = {
    val file = it.toSeq.head._1
    val localOutput = it.toSeq.head._2
    val os = new OpenSlide(new File(file))
    val errors = it.map(el => writeTileLocal(os, el._3, localOutput))
    os.close
    
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
    if (img.exists) 0 else 1
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