package nidan.models

import org.apache.spark.sql.types._

object Schemas {
  def ImageSchema(): StructType = {
    val schema = new StructType(Array(
          StructField("imageId", StringType, nullable = true),
          StructField("imageLevel", IntegerType, nullable = true),
          StructField("imagePartitions", IntegerType, nullable = true),
          StructField("partitionIndex", IntegerType, nullable = true),
          StructField("partitionZIndex", IntegerType, nullable = true),
          StructField("partitionWidth", IntegerType, nullable = true),
          StructField("partitionHeight", IntegerType, nullable = true),
          StructField("imageWidht", LongType, nullable = true),
          StructField("imageHeight", LongType, nullable = true),
          StructField("imageBytes", BinaryType, nullable = true)
    ))
    
    return schema
  }
  
}