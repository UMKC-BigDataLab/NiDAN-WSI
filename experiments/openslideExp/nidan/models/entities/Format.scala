package nidan.models.entities

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType

case class Format(id:Long, name:String, isMedical:Boolean)
object Format extends TNidanSQL{
  val tableName = "Format"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("name", StringType, true),
          StructField("isMedical", BooleanType, true)
        ))
  }
}