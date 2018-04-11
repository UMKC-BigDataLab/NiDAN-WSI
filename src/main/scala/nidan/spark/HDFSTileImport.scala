package nidan.spark

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream

import javax.imageio.ImageIO
import nidan.io.NidanContext
import org.apache.spark.rdd.RDD


case class NidanOrder(rowOrder:Int, zOrder:Int, hOrder:Int, nOrder:Int)
case class NidanTile(image:Array[Byte], width:Long, height:Long, order:NidanOrder)
case class NidanLevel(level:Int, partitions:Int, widht:Long, height:Long, tiles:Array[NidanTile])

case class Metadata(level:Int, partitions:Int, files:Array[String])

// This class assumes that the data has been written in hdfs
// using the nidan.preprocessing.LocalTileGenerator convention

// As a general rule, methods will work with RDDs, but they
// wont call scala context

// Keep it simple as a byte array

object HDFSTileImport {
  def getOrder(orders:String) = {
    val values = orders.split(",").map(x => x.toInt)
    
  }
}

