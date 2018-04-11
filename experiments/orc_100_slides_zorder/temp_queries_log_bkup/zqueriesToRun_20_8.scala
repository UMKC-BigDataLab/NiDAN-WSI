import java.io.File
import java.io.FileOutputStream
import org.apache.spark.sql._

val queryMsg = "#QUERY "
val loadDBMsg = "#LOAD_DB "
val loadTable = "#LOAD_TABLE "
val loadsqlHive = "#LOAD_SQL_CONTEXT "

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

val dataSource = "/nidan/orc/individualORC/slide4"

show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=100 AND partitionZIndex<=107", 8))
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

 
val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=153", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=125 AND partitionZIndex<=132", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=68 AND partitionZIndex<=75", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=74 AND partitionZIndex<=81", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide17"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=38 AND partitionZIndex<=45", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=227 AND partitionZIndex<=234", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=169 AND partitionZIndex<=176", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=46", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=149 AND partitionZIndex<=156", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide7"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=122 AND partitionZIndex<=129", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=217 AND partitionZIndex<=224", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=103 AND partitionZIndex<=110", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=94 AND partitionZIndex<=101", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=138 AND partitionZIndex<=145", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide7"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=117 AND partitionZIndex<=124", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=234 AND partitionZIndex<=241", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=2 AND partitionZIndex<=9", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=12 AND partitionZIndex<=19", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=213 AND partitionZIndex<=220", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=134 AND partitionZIndex<=141", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=53 AND partitionZIndex<=60", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=153 AND partitionZIndex<=160", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=123 AND partitionZIndex<=130", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide16"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=207 AND partitionZIndex<=214", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=155", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=193 AND partitionZIndex<=200", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=124 AND partitionZIndex<=131", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=212 AND partitionZIndex<=219", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=154 AND partitionZIndex<=161", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=52 AND partitionZIndex<=59", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=158 AND partitionZIndex<=165", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=232 AND partitionZIndex<=239", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=111 AND partitionZIndex<=118", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=90 AND partitionZIndex<=97", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=44 AND partitionZIndex<=51", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=33 AND partitionZIndex<=40", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=236 AND partitionZIndex<=243", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=229 AND partitionZIndex<=236", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide16"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=195 AND partitionZIndex<=202", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=8 AND partitionZIndex<=15", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=109", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=152 AND partitionZIndex<=159", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide7"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=194 AND partitionZIndex<=201", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=123", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=88 AND partitionZIndex<=95", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide17"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=203 AND partitionZIndex<=210", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=200 AND partitionZIndex<=207", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=211 AND partitionZIndex<=218", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=85 AND partitionZIndex<=92", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=29 AND partitionZIndex<=36", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=181 AND partitionZIndex<=188", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=40 AND partitionZIndex<=47", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=174 AND partitionZIndex<=181", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=150", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=162 AND partitionZIndex<=169", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=199 AND partitionZIndex<=206", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=112 AND partitionZIndex<=119", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=202 AND partitionZIndex<=209", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=225 AND partitionZIndex<=232", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=128 AND partitionZIndex<=135", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=137 AND partitionZIndex<=144", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=42 AND partitionZIndex<=49", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=189", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=28", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=230 AND partitionZIndex<=237", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=131 AND partitionZIndex<=138", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=166 AND partitionZIndex<=173", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=35", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=15 AND partitionZIndex<=22", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=171 AND partitionZIndex<=178", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=48 AND partitionZIndex<=55", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=179 AND partitionZIndex<=186", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=159 AND partitionZIndex<=166", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=4 AND partitionZIndex<=11", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=45 AND partitionZIndex<=52", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=196 AND partitionZIndex<=203", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide17"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=43 AND partitionZIndex<=50", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=96 AND partitionZIndex<=103", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=183 AND partitionZIndex<=190", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=81 AND partitionZIndex<=88", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=229", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=5 AND partitionZIndex<=12", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=190 AND partitionZIndex<=197", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=99 AND partitionZIndex<=106", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide17"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=129 AND partitionZIndex<=136", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=156 AND partitionZIndex<=163", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=71 AND partitionZIndex<=78", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=72 AND partitionZIndex<=79", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=91 AND partitionZIndex<=98", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=10 AND partitionZIndex<=17", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=140 AND partitionZIndex<=147", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=16 AND partitionZIndex<=23", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=26 AND partitionZIndex<=33", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=25 AND partitionZIndex<=32", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=208 AND partitionZIndex<=215", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=18 AND partitionZIndex<=25", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=121 AND partitionZIndex<=128", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=67 AND partitionZIndex<=74", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=210 AND partitionZIndex<=217", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide16"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=47 AND partitionZIndex<=54", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

