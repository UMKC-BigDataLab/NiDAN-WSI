package nidan.models.entities

import org.apache.spark.sql.types.{StructType,StructField,IntegerType,BinaryType}
import org.apache.spark.sql.types.LongType

case class R_PatientWSImage(patientId : Int, imageIndex: Int, data: Array[Byte])
object R_PatientWSImage extends TNidanSQL{
  val tableName = "R_PatientImage"
  def getSchema : StructType = {
    return StructType(Seq(
    		StructField("patientId", LongType, true),
    		StructField("imageIndex", IntegerType, true),
    		StructField("data", BinaryType, true)
    		))
  }
}
