package nidan.models.entities

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

case class Laboratory(id:Long, name:String, 
    address:String, email:String, phone:String)
object Laboratory extends TNidanSQL{
  val tableName = "Laboratory"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("name", StringType, true),
          StructField("address", StringType, true),
          StructField("email", StringType, true),
          StructField("phone", StringType, true)
        ))
  }
}