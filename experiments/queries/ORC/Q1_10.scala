 
import java.io.File
import java.io.FileOutputStream
import org.apache.spark.sql._

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
    1
  }

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  
val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 175 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 73 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 148 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 49 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 141 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 131 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 124 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 93 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 8 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 67 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 58 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 116 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 211 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 234 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 155 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 47 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 233 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 221 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 122 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 17 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 237 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 154 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 223 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 163 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 137 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 230 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 142 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 191 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 226 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 68 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 201 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 19 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 216 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 28 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 151 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 161 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 127 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 38 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 241 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 70 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 114 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 208 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 43 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 6 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 144 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 159 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 90 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 20 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 139 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 104 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 253 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 119 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 189 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 225 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 174 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 18 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 185 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 40 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 222 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 53 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 157 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 195 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 248 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 128 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 97 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 140 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 146 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 199 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 82 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 121 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=2.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 251 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 162 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 64 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 85 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 164 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 168 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=9.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 123 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=7.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 100 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 39 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 252 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 203 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 184 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 117 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 250 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 178 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 57 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=3.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 15 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 77 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 197 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 115 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 113 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=5.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 118 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=10.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 50 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 133 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=6.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 249 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 59 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=1.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 235 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 136 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=4.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 2 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/KUDB.R10.orc/imageId=8.svs"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 76 ", 1))


show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

