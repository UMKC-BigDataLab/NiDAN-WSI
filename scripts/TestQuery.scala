import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val data = spark.read.orc("/nidan/orc/KU_Unsorted.orc")
data.createOrReplaceTempView("t0")

val values =(df:DataFrame) => df.map(x => (x.getLong(0), x.getLong(1))).collect(0)

val f0 = values(spark.sql("SELECT slideWidth, slideHeight from t0 where imageLevel = 0 and rowOrder = 1")
val f1 = values(spark.sql("SELECT slideWidth, slideHeight from t0 where imageLevel = 1 and rowOrder = 1")
val f2 = values(spark.sql("SELECT slideWidth, slideHeight from t0 where imageLevel = 2 and rowOrder = 1")
val f3 = values(spark.sql("SELECT slideWidth, slideHeight from t0 where imageLevel = 3 and rowOrder = 1")

val rW = f0._1 / f3._1
val rH = f0._2 / f3._2

val (tX1, tX2) = (1000 * rW, 2000 * rH)
val (tY1, tY2) = (2000 * rW, 1000 * rH)
val query= s"""SELECT xPos, yPos 
              |FROM t0 
              |WHERE 
              |slideId = 'Slide1.svs' AND
              |imageLevel = 1 AND
              |xPos >= ($tX1) AND
              |xPos <= ($tX2) AND
              |yPos >= ($tY1) AND
              |yPos <= ($tY2)
              """.stripMargin.replaceAll("\n", " ")


val result = spark.sql(query)
result.count
