package nidan.models.entities

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import java.util.Date
import org.apache.spark.sql.types.LongType

case class Scanner(id:Long, 
    company:String,
    model:String,
    activeSince:Date,
    imageMode:String,
    slideCapacity:Int,
    speed:Float
    )

object Scanner extends TNidanSQL{
  val tableName = "Scanner"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id", LongType, true),
          StructField("company",StringType, true),
          StructField("model",StringType, true),
          StructField("activeSince",DateType,true),
          StructField("imageMode",StringType,true),
          StructField("slideCapacity",IntegerType,true),
          StructField("speed",FloatType,true)
        ))
  }
}