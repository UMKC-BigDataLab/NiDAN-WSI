package nidan.models.entities

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime
import org.apache.spark.sql.types.LongType

case class Discussion(id:Long, 
    title:String,
    body:String,
    locationX:Int,
    locationY:Int,
    dateTime:DateTime
    )

object Discussion extends TNidanSQL{
  val tableName = "Discussion"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("title",StringType, true),
          StructField("Body",StringType, true),
          StructField("locationX",IntegerType, true),
          StructField("locationY",IntegerType,true),
          StructField("dateTime",TimestampType,true)
        ))
  }
}