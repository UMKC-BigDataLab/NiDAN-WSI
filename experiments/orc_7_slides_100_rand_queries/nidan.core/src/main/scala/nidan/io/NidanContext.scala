package nidan.io

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object NidanContext {
  
  def getHDFSConf():Configuration = {
    val coreFile = "/usr/local/hadoop/etc/hadoop/core-site.xml"
    val hdfsFile = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"
    val yarnFile = "/usr/local/hadoop/etc/hadoop/yarn-site.xml"
    
    val corePath = new Path(coreFile)
    val hdfsPath = new Path(hdfsFile)
    val yarnPath = new Path(yarnFile)
    
    val conf = new Configuration()
    conf.addResource(corePath)
    conf.addResource(hdfsPath)
    conf.addResource(yarnPath)
    
    return conf
  }
  
  def getHDFSConfSingleton():Configuration = {
    if(hadoopConf == null){
      hadoopConf = getHDFSConf()
    }
    
    return hadoopConf
  }
  
  private var hadoopConf:Configuration = null
  
  lazy val sparkConf = new SparkConf()
  lazy val hiveContext = new HiveContext(sparkContext)
  lazy val sparkContext = new SparkContext(sparkConf)
//  lazy val sqlContext = SQLContext.getOrCreate(HiveContext)
  lazy val sqlContext = SparkSession.builder.getOrCreate
  lazy val log = LogManager.getRootLogger
}