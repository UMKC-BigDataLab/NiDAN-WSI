package nidan.models

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object Configuration {
  val DataSourceFormat = "parquet"
  val DataSource = "/nidan/data/tables/"
  val DataSource_Patient = s"$DataSource/Patients.$DataSourceFormat"
  val DataSource_WSImage = s"$DataSource/WSImages.$DataSourceFormat"
  val DataSource_Format = s"$DataSource/Formats.$DataSourceFormat"
  
  def isTableCreated(sc:SparkContext, source:String) : Boolean = {
     // Check in HDFS
    val hConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hConf)
    
    return fs.exists(new Path(source))  
  }
  
}