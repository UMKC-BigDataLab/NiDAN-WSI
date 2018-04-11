package nidan.models.entities

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.LongType

case class Annotation(id:Long, 
    text:String,
    locationX:Int,
    locationY:Int,
    dateTime:DateTime
    )

object Annotation extends TNidanSQL{
  val tableName = "Annotation"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("text",StringType, true),
          StructField("locationX",IntegerType, true),
          StructField("locationY",IntegerType,true),
          StructField("dateTime",DateType,true)
        ))
  }
}