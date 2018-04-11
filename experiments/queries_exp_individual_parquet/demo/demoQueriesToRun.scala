//spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=2g


import java.io.File
import java.io.FileOutputStream

val queryMsg = "#QUERY "
val loadDBMsg = "#LOAD_DB "
val loadTable = "#LOAD_TABLE "
val loadSqlContext = "#LOAD_SQL_CONTEXT "
val dataSource = "/nidan/parquet/slide5.prqt"
val currentSlide = "5"

def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
  }

val queries = List(
("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <=1 and imageLevel = 0 and imageId = ${currentSlide}.svs",1),
("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <=2 and imageLevel = 0 and imageId = ${currentSlide}.svs",2),
("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <=4 and imageLevel = 0 and imageId = ${currentSlide}.svs",4),
("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <=8 and imageLevel = 0 and imageId = ${currentSlide}.svs",8)
)


val sqlContext = show_timing{new org.apache.spark.sql.SQLContext(sc)}
val pf = show_timing{sqlContext.read.parquet(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

for (query <- queries){ 
println(s">> Running query: ${query._1}")
show_timing{sqlContext.sql(query._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}


