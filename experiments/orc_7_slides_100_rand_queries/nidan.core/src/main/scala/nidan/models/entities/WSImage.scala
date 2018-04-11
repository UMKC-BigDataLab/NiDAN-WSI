package nidan.models.entities

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime
import org.apache.spark.sql.types.LongType


case class WSImage(id:Long,
    date:DateTime,
    pixelResolution:Int,
    zoomMagnification:Int,
    imageFormat:String,
    imageSize:Int,
    imageData:Array[Byte]
    )

object WSImage extends TNidanSQL{
  val tableName = "WSImage"
  def getSchema : StructType = {
    return StructType(Seq(
          StructField("id",LongType, true),
          StructField("date",StringType, true),
          StructField("pixelResolution",IntegerType, true),
          StructField("zoomMagnification",IntegerType,true),
          StructField("imageFormat",StringType,true),
          StructField("imageSize",IntegerType,true),
          StructField("imageData",BinaryType, true)
        ))
  }
}
