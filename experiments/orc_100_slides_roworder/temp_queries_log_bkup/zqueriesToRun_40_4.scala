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

 
val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=173 AND partitionZIndex<=176", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=152 AND partitionZIndex<=155", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=66 AND partitionZIndex<=69", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=23 AND partitionZIndex<=26", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=186 AND partitionZIndex<=189", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide39"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=129 AND partitionZIndex<=132", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=30 AND partitionZIndex<=33", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=50 AND partitionZIndex<=53", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide26"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=31", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=206 AND partitionZIndex<=209", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=90 AND partitionZIndex<=93", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=105", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=64 AND partitionZIndex<=67", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide25"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=108 AND partitionZIndex<=111", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=202 AND partitionZIndex<=205", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=188 AND partitionZIndex<=191", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide34"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=185 AND partitionZIndex<=188", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=211 AND partitionZIndex<=214", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide40"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=7 AND partitionZIndex<=10", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=93 AND partitionZIndex<=96", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=240 AND partitionZIndex<=243", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=207", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=22 AND partitionZIndex<=25", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=124 AND partitionZIndex<=127", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=35 AND partitionZIndex<=38", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide36"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=149", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=101 AND partitionZIndex<=104", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=171 AND partitionZIndex<=174", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=130 AND partitionZIndex<=133", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide21"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=43 AND partitionZIndex<=46", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=243 AND partitionZIndex<=246", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide24"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=151", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=80 AND partitionZIndex<=83", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide23"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=27 AND partitionZIndex<=30", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=51 AND partitionZIndex<=54", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide27"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=133 AND partitionZIndex<=136", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=203 AND partitionZIndex<=206", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide32"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=185", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=88 AND partitionZIndex<=91", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide25"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=151 AND partitionZIndex<=154", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide25"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=114 AND partitionZIndex<=117", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=208 AND partitionZIndex<=211", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=127 AND partitionZIndex<=130", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide16"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=107 AND partitionZIndex<=110", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide29"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=57 AND partitionZIndex<=60", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=62 AND partitionZIndex<=65", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide36"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=235 AND partitionZIndex<=238", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=140 AND partitionZIndex<=143", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=83 AND partitionZIndex<=86", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=150 AND partitionZIndex<=153", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide37"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=179 AND partitionZIndex<=182", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide12"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=72 AND partitionZIndex<=75", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=97 AND partitionZIndex<=100", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=194 AND partitionZIndex<=197", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=5 AND partitionZIndex<=8", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=225 AND partitionZIndex<=228", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=38 AND partitionZIndex<=41", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=41 AND partitionZIndex<=44", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide6"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=142 AND partitionZIndex<=145", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=100 AND partitionZIndex<=103", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=155 AND partitionZIndex<=158", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide23"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=221 AND partitionZIndex<=224", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=154 AND partitionZIndex<=157", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide26"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=157 AND partitionZIndex<=160", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide22"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=184 AND partitionZIndex<=187", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=94 AND partitionZIndex<=97", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=132 AND partitionZIndex<=135", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=122 AND partitionZIndex<=125", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=11 AND partitionZIndex<=14", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide34"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=78 AND partitionZIndex<=81", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=225", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=24", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=146", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=144 AND partitionZIndex<=147", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=189 AND partitionZIndex<=192", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=218 AND partitionZIndex<=221", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=216 AND partitionZIndex<=219", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide23"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=67 AND partitionZIndex<=70", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide25"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=183 AND partitionZIndex<=186", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=118 AND partitionZIndex<=121", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide16"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=77 AND partitionZIndex<=80", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide29"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=161 AND partitionZIndex<=164", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=164 AND partitionZIndex<=167", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide32"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=201 AND partitionZIndex<=204", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide32"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=37 AND partitionZIndex<=40", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=131 AND partitionZIndex<=134", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide21"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=119", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide17"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=136 AND partitionZIndex<=139", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide27"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=137 AND partitionZIndex<=140", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide23"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=110 AND partitionZIndex<=113", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide26"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=160 AND partitionZIndex<=163", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=239 AND partitionZIndex<=242", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide29"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=125 AND partitionZIndex<=128", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide27"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=156 AND partitionZIndex<=159", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=233 AND partitionZIndex<=236", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=109 AND partitionZIndex<=112", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=9 AND partitionZIndex<=12", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=49 AND partitionZIndex<=52", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide32"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=46 AND partitionZIndex<=49", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE partitionZIndex>=191 AND partitionZIndex<=194", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

