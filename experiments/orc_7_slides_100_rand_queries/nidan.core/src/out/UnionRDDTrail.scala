/**
  * Created by vinu on 12/16/15.
  */

import java.io._
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.RangePartitioner
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable._


/**
  * Created by vinu on 12/8/15.
  * Updated: March 2016 by daniel
*/
object UnionRDDTrial {

  def printHelp(){
    System.err.println("Usage: HDFSToLocal " +
      "{local input image dir} " +
      "{hdfs output filename} " +
      "{optional: use partition=Y/N}")    

    System.exit(1)
  }

  def printDebug(message : String){
    println(">> DEBUG \n " + message)
  }

  def getZCoord(fileName : String) : Int = {
      return fileName.substring(0, fileName.indexOf("_")).toInt
  }

  def main(args: Array[String]) {

    // args(0) = split image directory
    // args(1) = HDFS full path and file name to store
    // args(2) = user partition - Y/N
    // args(3) = number of partition
        
    if (args.length < 2)
      printHelp()      
    
    val startTime = System.nanoTime 

    // get list of files from the specified directory passed as args(0)
    val fileList = getListOfFiles(args(0))
    val hdfsFileName = args(1)
    var usePartition: String = if (args.length == 4) args(2) else "N"


    printDebug(">> args(0) = " + args(0) + "\n " +
      ">> args(1) = " + args(1) + "\n " +
      ">> fileList.size = " + fileList.size + "\n " +
      ">> args(2) = " + args(2) + "\n " 
    )

    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()

    printDebug(">> After spark conf" + "\n " +
      ">> begining of Initialize spark Context" + "\n "
    )

    try {

      for (file <- fileList) {
        // get zcoord from file name 0_img.jpg
        var zcoord: Int = getZCoord(file.getName())
        var imageChuck: Array[Byte] = read(file)
        imageList += (new Tuple2(zcoord, imageChuck))

        printDebug(
          ">> file.getName() - " + file.getName() + "\n" +
          ">> zcoord:  " + zcoord
        )
      }

      val data = sc.parallelize(imageList).persist(StorageLevel.MEMORY_AND_DISK)
      val input = data.map{case (x,y) => (x.get(),y.getBytes)}.cache()

      printDebug(">> initialize parallelize " + Calendar.getInstance().getTime() + "\n" +
        ">> done parallelize " + Calendar.getInstance().getTime() + "\n" +
        ">> done map() " + Calendar.getInstance().getTime() + "\n"
      )

      if (usePartition.equalsIgnoreCase("Y")) {

        val tunedPartitioner = new RangePartitioner(args(3).toInt, input)
        val sizeBeforeCache = sc.getPersistentRDDs.size

        val partitioner = input.partitionBy(tunedPartitioner).cache()
        val sizeAfterCache = sc.getPersistentRDDs.size

        partitioner.saveAsSequenceFile(hdfsFileName)

        printDebug(">> Number of partitions: " + args(3) + "\n" +
          // ">> Number of items in an RDD: " + input.count() + "\n" +
          ">> Using range partition to store " + "\n" +
          ">> Before caching " + sizeBeforeCache + "\n" +
          ">> After caching " + sizeAfterCache + "\n"
        )

      }
      else{
        val sortedInput = input.sortByKey(true) //.persist(StorageLevel.MEMORY_ONLY)
        val sizeBeforeCache = sc.getPersistentRDDs.size
        val cacheRDD = sortedInput.cache()
        val sizeAfterCache = sc.getPersistentRDDs.size

        cacheRDD.saveAsSequenceFile(hdfsFileName)
        printDebug(">> Before caching " + sizeBeforeCache + "\n" +
          ">> After caching " + sizeAfterCache + "\n"
        )
     }

     val elapsedTime = (System.nanoTime - startTime) / 1e6
     printDebug(">> Elapsed time: " + elapsedTime + " ms")

    }
    catch {
      case unknown: Throwable => {
        println("An exception occured")
        unknown.printStackTrace()
      }


      case e:Exception => {
        println("An exception occured")
        e.printStackTrace()
      }
    }

    // Stop sparks enviroment
    sc.stop();
  }


  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    println("d.exists = " + d.exists)
    println("d.isDirectory = " + d.isDirectory)
    if (d.exists && d.isDirectory) {
      //d.listFiles.filter(_.isFile).toList
      println("get list of .png")
      d.listFiles.filter(f => f.isFile && f.getName().endsWith(".png")).toList
    } else {
      println("empty list of .png")
      List[File]()
    }
  }

  def read(file: File) : Array[Byte] = {

    val result: Array[Byte] = new Array[Byte](file.length.toInt)

    try {
      var input: InputStream = null
      try {
        var totalBytesRead: Int = 0
        input = new BufferedInputStream(new FileInputStream(file))
        while (totalBytesRead < result.length) {
          val bytesRemaining: Int = result.length - totalBytesRead
          val bytesRead: Int = input.read(result, totalBytesRead, bytesRemaining)
          if (bytesRead > 0) {
            totalBytesRead = totalBytesRead + bytesRead
          }
        }
      } finally {
        input.close
      }
    }
    catch {
      case ex: FileNotFoundException => {
        println("FileNotFoundException exception")
      }
      case ex: IOException => {
        println("IOException exception")
      }
    }
    return result
  }
}


