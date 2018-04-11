package nidan.models.entities

import java.util.Date

import nidan.models.Configuration

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

// TODO Generate a mapping from the class to the attributes
// http://stackoverflow.com/questions/32535273/how-to-match-dataframe-column-names-to-scala-case-class-attributes

case class Patient(id: Long, strDateOfBirth: String, gender : String){
  
  override def toString():String = {
    val representation = s"Id:$id DOB:$strDateOfBirth Gender:$gender"
    return representation
  }
  
}
object Patient extends TNidanSQL{
  val tableName = "Patient"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("strDateOfBirth", StringType, true),
          StructField("gender", StringType, true)
        ))
  }
  
  def getAllPatients(sql:SQLContext): DataFrame = {
    val dataSourceFile = Configuration.DataSource_Patient
    
    // Not a good format, but ideal for scala's val
    val data = 
      if (Configuration.isTableCreated(sql.sparkContext, dataSourceFile))
        sql.read.parquet(dataSourceFile)
      else
        sql.emptyDataFrame
    
    return data
  }
  
  
  
  
  
  
}