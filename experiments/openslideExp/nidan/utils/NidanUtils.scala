package nidan.utils

import java.nio.ByteBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import nidan.models.entities.Patient
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import java.awt.image.BufferedImage

object NidanUtils{
  
  
  
  def stitchImages(imgs:Array[(BufferedImage, Int, Int)], buffer:BufferedImage,
      LOG:String=>Unit):BufferedImage = {
    
    val canvas = buffer.createGraphics
    LOG(">> Canvas was generated")
    
    var x, y = 0
    var count = 0
    for(item <- imgs){
      val img = item._1
      val w = item._2
      val h = item._3
      
      canvas.drawImage(img, x, y, null)
      
      x = x + w
      if( x >= buffer.getWidth){
        x = 0
        y = y + h
      }
      
      count = count + 1
      LOG(s">> \t Stitching $count image(s)")
    }
    
    canvas.dispose
    return buffer
    
  }
  
  def getDataFrameFromPatient(newPatient:Patient, sql:SQLContext) : DataFrame ={
    val rows = List[Row](Row(newPatient.id,
        newPatient.strDateOfBirth, 
        newPatient.gender)
    ).asJava
    
    return sql.createDataFrame(rows, Patient.getSchema)
  }

  
  
  def appendParquetEntity(data:DataFrame, source:String) : Unit ={
    data
      .write
      .mode(SaveMode.Append)
      .parquet(source)
  }
  
  def getByteArray(intArray:Array[Int]) : Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(intArray.size * (Integer.SIZE/8))
    intArray.map(byteBuffer.putInt(_))
    
    byteBuffer.flip
    return byteBuffer.array
  }
  
   def getHeightBounds(row:Int, rows:Int, gaps:Array[Int]):(Int,Int) ={
      val lowerBound = row * gaps(1)
      var upperBound = (row + 1) * gaps(1) 
        
      if(row == (rows-1)){ 
        println(s"# Enter the comparason")
        println(s"# Gaps: ${gaps(2)} ${gaps(3)}")
        println(s"# Previous UB: $upperBound")
        
        upperBound = upperBound + gaps(3)
        println(s"# New UB: $upperBound")
      }
        
      return (lowerBound,upperBound)
    }
	  
	  def getWidthBounds(col:Int, cols:Int, gaps:Array[Int]):(Int,Int) ={
      val lowerBound = col * gaps(0)
      var upperBound = (col + 1) * gaps(0) 
        
      if(col == (cols-1)){
        println(s"# Enter the comparason")
        println(s"# Gaps: ${gaps(2)} ${gaps(3)}")
        println(s"# Previous UB: $upperBound")
        
        upperBound = upperBound + gaps(2)
        println(s"# New UB: $upperBound")
      }
        
      return (lowerBound,upperBound)
    }
	  
	  def getDimensionsArray(width:Int, height:Int, arraySize:Int):Array[Int] = {
	    return Array[Int](
        width/arraySize, 
        height/arraySize, 
        width%arraySize, 
        height%arraySize
      )
	  }
	  
	  def getDimensionsArray(width:Long, height:Long, arraySize:Long):Array[Long] = {
	    return Array[Long](
        width/arraySize, 
        height/arraySize, 
        width%arraySize, 
        height%arraySize
      )
	  }
	  
	  def getHeightBounds(row:Int, rows:Int, gaps:Array[Long]):(Long,Long) ={
      val lowerBound = row * gaps(1)
      var upperBound = (row + 1) * gaps(1) 
        
      if(row == (rows-1)){         
        upperBound = upperBound + gaps(3)
      }
        
      return (lowerBound,upperBound)
    }
	  
	  def getWidthBounds(col:Int, cols:Int, gaps:Array[Long]):(Long,Long) ={
      val lowerBound = col * gaps(0)
      var upperBound = (col + 1) * gaps(0) 
        
      if(col == (cols-1)){
        upperBound = upperBound + gaps(2)
      }
        
      return (lowerBound,upperBound)
    }
  
}