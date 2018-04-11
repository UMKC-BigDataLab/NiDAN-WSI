package nidan.models.entities

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

case class Expert(id:Long, name:String, gender:String)
object Expert extends TNidanSQL{
  val tableName = "Expert"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("name", StringType, true),
          StructField("gender", StringType, true)
        ))
  }
}