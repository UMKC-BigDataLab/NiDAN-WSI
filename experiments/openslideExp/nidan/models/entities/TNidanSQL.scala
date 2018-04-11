package nidan.models.entities

import org.apache.spark.sql.types.StructType

trait TNidanSQL {
  val tableName : String
  def getSchema : StructType
}