package nidan.main

import java.io.File
import nidan.io.NidanContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.FileWriter

object ShortCuts{
//  val in = <-
}

object MainImportSVS {
  def main(args: Array[String]): Unit = {
    val inputDirHDFS = args(1)
    val outputHDFSDir = args(2)
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    val log = NidanContext.log
    val hdfs = FileSystem.get(NidanContext.getHDFSConf())
    val PARTITIONS = 4
    
    /* Input dir struct is:
     * Dir/
     * 	Patient1.svs
     * 	Patient2.svs ...
    */
    // First lets read the SVS files
    log.warn(">>> Dir to read: " + inputDirHDFS)
    val listFiles = hdfs.listStatus(new Path(inputDirHDFS)).map(fileStatus => fileStatus.getPath.toString)
    
    val result = sc.parallelize(listFiles, PARTITIONS).map{ case path =>
      val fs = FileSystem.get(NidanContext.getHDFSConf)
      val size = fs.getFileStatus(new Path(path)).getLen
      
      
      val newFile = new File("/users/dl544/testFile.txt")
      val writer = new FileWriter(newFile)
      writer.append(s"File $path and Size $size")
      writer.flush
      writer.close
      
      (path, size)
    }.collect
    
    for(item <- result){
      log.warn(s">>> ${item._1} => ${item._2}")
    }
    
  }
  
  def getFilesFromFromLocal(inputDir:String):Option[Array[File]] = {
    val list = new File(inputDir).listFiles
    if (list.length > 0) Some(list) else None
    
//    try{
//      val list = new File(inputDir).listFiles
//      println(">>> It runs")
//      Some(list)
//    }catch{
//      case e:Exception =>
//        println(">>> Error ")
//        None
//    }
  }
  
  
  
}