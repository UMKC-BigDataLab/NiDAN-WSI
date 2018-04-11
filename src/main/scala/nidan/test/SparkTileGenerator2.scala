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
  
  def time[R](block: => R): (R, Double) = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val secs = (t1 - t0)/1000000000.0
    (result, secs)
}
  
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val hdfsInput = args(1)
    val hdfsOutput = args(2)
    val localInput = args(3)
    
    // Getting the nodes in a fancy way ;)
    val sc = NidanContext.sparkContext
    val host = sc.getConf.get("spark.driver.host")
    val nodes = sc.getExecutorMemoryStatus.map(_._1).filter(!_.contains(host)).size
    
    val seq = sc.parallelize(Seq(1 to nodes))
    
    // Download the copy into the local dir
    seq.map{item =>
      val from = hdfsInput + "/" + fileName
      val to = localInput + "/" + fileName
      
      
    }
    
    // Generate the array of tiles from the local copy
    
    
    
    // Group the array of tiles using range partition, based on the index
    // and the cluster's size
    
    
    val input = args(0)
    val output = args(1)
    val n = args(2).toInt
    val level = args(3).toInt
    
    
    
    
    // Read the image dimensions
    val os = new OpenSlide(new File(input))
    val dim = new TileDimension(os.getLevel0Width, os.getLevel0Height)
    val coords = sc.parallelize(CoordinateGenerator.squareMatrixfromDimension(dim, n))
    
    // Get the partitions
    val metas = coords.map{item =>
      val index = new nidan.tiles.Index(item._2, item._3, item._1, item._1, item._1)
      val meta = new TileMetadata(input, input, level,item._4, item._5, index,(n,n))
      (input, meta)
    }
        
    // Extract the data
    val tiles = metas.map{item => 
      
      val startTime = System.nanoTime
      
      val oslide = new OpenSlide(new File(item._1))
      val point = item._2.position
      val outputFile = output + "/" + new File(item._2.toString).getName
      
      // Get the image canvas
      val w = item._2.dimension.width.toInt
      val h = item._2.dimension.height.toInt
      
      val canvas = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
      val graph = canvas.getGraphics
      val data = canvas.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
      
      // Get the data from os
      oslide.paintRegionARGB(data, point.x, point.y, level, w, h)
      oslide.close
      
      // Write the output in local file
      graph.drawImage(canvas, 0, 0, w, h, null)
      writeJPEG(canvas.getRaster, outputFile, ImageFormats.JPEG)
      
      val totalTime = (System.nanoTime - startTime) / 1000000000
      
      val img = new File(outputFile)
      //Imaging.writeImage(canvas, img, ImageFormats.JPEG, null)
      graph.dispose
      
      val error = if (img.exists) 1 else 0
      (error, totalTime)
    }
    
    // Test it
    //tiles.saveAsTextFile(output + "/testOut")
    
    
    // Count the errors
    val totalSecs = tiles.map(_._2).sum
    val error = tiles.filter(_._1 < 1).count
    println(">> Errors: " + error)
    println(">> Total seconds: " + totalSecs)
    sc.stop()
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