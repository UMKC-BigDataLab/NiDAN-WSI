package nidan.models.entities

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

case class Description(id:Long, 
    text:String,
    origin:String
    )

object Description extends TNidanSQL{
  val tableName = "Description"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("text",StringType, true),
          StructField("origin",StringType, true)
        ))
  }
}