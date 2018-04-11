package nidan.main

import java.text.SimpleDateFormat

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

import nidan.models.entities.Patient
import nidan.models.Configuration
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import nidan.utils.NidanUtils

import scala.collection.JavaConverters._

object MainModels {
  lazy val dateFormatParser = new SimpleDateFormat("MM-dd-yyyy")
  val dataSource = Configuration.DataSource_Patient
  
  // TODO Add this objects into another object
  lazy val conf = new SparkConf()
  lazy val sc = new SparkContext(conf)
  lazy val sql = new SQLContext(sc)
//  lazy val hSQL = new HiveContext(sc)
  lazy val log = LogManager.getRootLogger
  
  def main(args: Array[String]): Unit = {
    log.setLevel(Level.WARN)
    log.warn(">>> Nidan Models")
    
    args(0) match{
      case "-f" => // Insert a patient
        testCreateNewPatientFirstTime(args)
      
      case "-i" => // Insert a patient
        testInsertNewPatient(args)
        
      case "-s" => // Select a patient
        testSelectAll(args)
      
      case "-iv2" => //Insert patient v2
        testInsertNewPatientV2(args)
        
      case "-d" => ??? // Delete a patient
      case _ => ??? // Nothing
    }
  }
  
  def testSelectAll(args:Array[String]): Unit={
    log.warn(">>> Load the database")
    
    val patients = Patient.getAllPatients(sql)
    
    if (patients.count() == 0)
      println(">>> Empty DataFrame")
    else
      patients.show()
    
////    if (!Files.exists(Paths.get(dataSource)))
////      println(">>> There is no database")
//
//    // Check in HDFS
//    val hConf = sc.hadoopConfiguration
//    val fs = org.apache.hadoop.fs.FileSystem.get(hConf)
//    val exits = fs.exists(new Path(dataSource))
//      
//    if(!exits){
//      println(">>> Table does not exits")
////      return
//    } else{
//      val db = sql.read.parquet(dataSource)
//      val totalRows = db.count()
//      db.show()
//    }
  }
  
  def testInsertNewPatientV2(args:Array[String]):Unit ={
    
    log.warn(">>> Load the database")
//    val db = sql.read.parquet(dataSource)
    
    log.warn(">>> Create the new patient")
    val newPatient = getPatientFromArgs(args)
    log.warn(">>> Create Dataframe")
    val rows = List[Row](Row(newPatient.id,
        newPatient.strDateOfBirth, 
        newPatient.gender)
    )
    val rowRdd = sc.parallelize(rows)
    
    
    val dataFrame = sql.createDataFrame(rowRdd, Patient.getSchema)
//    val dataFrame = hSQL.createDataFrame(rowRdd, Patient.getSchema)
    
    
    NidanUtils.appendParquetEntity(dataFrame, dataSource)
//    
//    dataFrame.write
//      .format(Configuration.DataSourceFormat)
//      .mode(SaveMode.Append)
//      .saveAsTable(Patient.tableName)
    
    log.warn(">>> Save data in the database")
//    persistParquetDataFrame(db.unionAll(dataFrame), SaveMode.Overwrite, dataSource)
  }
  
  
  def testInsertNewPatient(args:Array[String]):Unit ={
    
    log.warn(">>> Load the database")
    val db = Patient.getAllPatients(sql) 
//      sql.read.parquet(dataSource)
    
    log.warn(">>> Create the new patient")
    val newPatient = getPatientFromArgs(args)
    log.warn(">>> Create Dataframe")
    val rows = List[Row](Row(newPatient.id,
        newPatient.strDateOfBirth, 
        newPatient.gender)
    ).asJava
    
//    val rows = List[Row](Row(newPatient.id,
//        newPatient.strDateOfBirth, 
//        newPatient.gender)
//    )
//    val rowRdd = sc.parallelize(rows)
    
    val dataFrame = sql.createDataFrame(rows, Patient.getSchema)
    
    log.warn(">>> Save data in the database")
    
//    val finalDataFrame = 
//      if (db.columns.size == 0) dataFrame // This means EmptyDataFrame no need to execute actions
//      else db.unionAll(dataFrame)
//        
//    persistParquetDataFrame(finalDataFrame, SaveMode.Append, dataSource)
    persistParquetDataFrame(dataFrame, SaveMode.Append, dataSource)
  }
  
  def testCreateNewPatientFirstTime(args:Array[String]):Unit = {
    log.warn(">>> Creating a new patient for the fist time")
    val newPatient = getPatientFromArgs(args)
    log.warn(newPatient)
     
    log.warn(">>> Create Dataframe")
    val rows = List[Row](Row(newPatient.id,
        newPatient.strDateOfBirth, 
        newPatient.gender)
    )
    val rowRdd = sc.parallelize(rows)
    
    val dataFrame = sql.createDataFrame(rowRdd, Patient.getSchema)
    persistParquetDataFrame(dataFrame, SaveMode.Append, dataSource)
    
  }
  
  // TODO See how can we create a class for this
  def persistParquetDataFrame(data:DataFrame, mode:SaveMode, source:String): Unit ={
    data.write
      .format(Configuration.DataSourceFormat)
      .mode(mode)
      .parquet(source)
  }
  
  // TODO Add an option to validate the format
  def getPatientFromArgs(args:Array[String]):Patient = {
    // Patient Data
    val patientId = args(1).toLong
    val patientDateOfBirth = args(2)
    val patientGender = args(3)
    
    return Patient(patientId, 
        patientDateOfBirth, 
        patientGender
    )
  }
  /*
1. Insert a patient in the system
-- Requirements
PatientID,
DateOfBirth,
Gender,
ImageID,
DateOfCapture
Resolution
Magnification,
Format,
Size,
Image
   * */
  
}