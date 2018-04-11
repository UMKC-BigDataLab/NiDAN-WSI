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

 
val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 49  OR  partitionIndex = 50  OR  partitionIndex = 36  OR  partitionIndex = 37 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 75  OR  partitionIndex = 89  OR  partitionIndex = 90  OR  partitionIndex = 102 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 127  OR  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 154 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide61"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 15  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 42 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 228  OR  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 199 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide75"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 175  OR  partitionIndex = 176  OR  partitionIndex = 132  OR  partitionIndex = 133 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide68"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 159  OR  partitionIndex = 173  OR  partitionIndex = 174  OR  partitionIndex = 160 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide73"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 202  OR  partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 229 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide71"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 131  OR  partitionIndex = 145  OR  partitionIndex = 146  OR  partitionIndex = 158 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 52  OR  partitionIndex = 60  OR  partitionIndex = 61  OR  partitionIndex = 75 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide78"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 21  OR  partitionIndex = 22 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 189  OR  partitionIndex = 203  OR  partitionIndex = 204  OR  partitionIndex = 190 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide39"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 53  OR  partitionIndex = 54  OR  partitionIndex = 40  OR  partitionIndex = 41 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide40"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 225  OR  partitionIndex = 226 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide45"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 45  OR  partitionIndex = 46 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 73  OR  partitionIndex = 87  OR  partitionIndex = 88  OR  partitionIndex = 74 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 157  OR  partitionIndex = 171  OR  partitionIndex = 172  OR  partitionIndex = 180 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide56"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 9  OR  partitionIndex = 23  OR  partitionIndex = 24  OR  partitionIndex = 10 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide26"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 39  OR  partitionIndex = 53  OR  partitionIndex = 54  OR  partitionIndex = 40 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 226  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 227 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide57"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 143  OR  partitionIndex = 144  OR  partitionIndex = 130  OR  partitionIndex = 131 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 32  OR  partitionIndex = 33  OR  partitionIndex = 47  OR  partitionIndex = 48 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide68"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 27  OR  partitionIndex = 28 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide7"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 235  OR  partitionIndex = 236 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 51  OR  partitionIndex = 52 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide34"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 65  OR  partitionIndex = 79  OR  partitionIndex = 80  OR  partitionIndex = 66 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide29"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 107  OR  partitionIndex = 108  OR  partitionIndex = 64  OR  partitionIndex = 65 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide24"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 191  OR  partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 218 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 45  OR  partitionIndex = 46  OR  partitionIndex = 32  OR  partitionIndex = 33 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 55  OR  partitionIndex = 56  OR  partitionIndex = 12  OR  partitionIndex = 13 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide41"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 186  OR  partitionIndex = 187  OR  partitionIndex = 201  OR  partitionIndex = 202 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide51"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 112  OR  partitionIndex = 8  OR  partitionIndex = 9  OR  partitionIndex = 23 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide44"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 177  OR  partitionIndex = 178 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 20  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 21 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide75"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 81  OR  partitionIndex = 82  OR  partitionIndex = 94  OR  partitionIndex = 95 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide61"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 93  OR  partitionIndex = 107  OR  partitionIndex = 108  OR  partitionIndex = 64 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 133  OR  partitionIndex = 147  OR  partitionIndex = 148  OR  partitionIndex = 134 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide78"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 15  OR  partitionIndex = 16  OR  partitionIndex = 2  OR  partitionIndex = 3 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide56"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 115  OR  partitionIndex = 116 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 232  OR  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 143 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide19"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 105  OR  partitionIndex = 119  OR  partitionIndex = 120  partitionIndex = 120 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide78"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 51  OR  partitionIndex = 52  OR  partitionIndex = 60  OR  partitionIndex = 61 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 3  OR  partitionIndex = 17  OR  partitionIndex = 18  OR  partitionIndex = 30 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 49  OR  partitionIndex = 50 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide41"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 141  OR  partitionIndex = 142 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide67"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 216  OR  partitionIndex = 217  OR  partitionIndex = 231  OR  partitionIndex = 232 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 7  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 34 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide51"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 104  OR  partitionIndex = 105  OR  partitionIndex = 119  OR  partitionIndex = 120 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 177  OR  partitionIndex = 178  OR  partitionIndex = 164  OR  partitionIndex = 165 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide54"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 151  OR  partitionIndex = 165  OR  partitionIndex = 166  OR  partitionIndex = 152 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide61"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 234  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 235 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide40"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 111  OR  partitionIndex = 112 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 46  OR  partitionIndex = 32  OR  partitionIndex = 33  OR  partitionIndex = 47 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 25  OR  partitionIndex = 26 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide47"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 219  OR  partitionIndex = 233  OR  partitionIndex = 234  OR  partitionIndex = 220 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide73"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 179  OR  partitionIndex = 180  OR  partitionIndex = 188  OR  partitionIndex = 189 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide64"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 119  OR  partitionIndex = 120  OR  partitionIndex = 120  OR  partitionIndex = 121 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 103  OR  partitionIndex = 117  OR  partitionIndex = 118  OR  partitionIndex = 104 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide72"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 45  OR  partitionIndex = 59  OR  partitionIndex = 60  OR  partitionIndex = 68 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 114  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 115 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 89  OR  partitionIndex = 90 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide44"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 19  OR  partitionIndex = 20 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide11"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 163  OR  partitionIndex = 177  OR  partitionIndex = 178  OR  partitionIndex = 164 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 83  OR  partitionIndex = 84 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide77"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 199  OR  partitionIndex = 200  OR  partitionIndex = 186  OR  partitionIndex = 187 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide32"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 179  OR  partitionIndex = 180 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide43"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 165  OR  partitionIndex = 166 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide57"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 130  OR  partitionIndex = 131  OR  partitionIndex = 145  OR  partitionIndex = 146 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide52"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 113  OR  partitionIndex = 114 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 69  OR  partitionIndex = 83  OR  partitionIndex = 84  OR  partitionIndex = 70 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide61"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 110  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 111 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide47"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 53  OR  partitionIndex = 54 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 115  OR  partitionIndex = 116  OR  partitionIndex = 72  OR  partitionIndex = 73 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide36"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 24  OR  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 25 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide71"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 165  OR  partitionIndex = 166  OR  partitionIndex = 152  OR  partitionIndex = 153 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide58"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 146  OR  partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 173 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide53"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 217  OR  partitionIndex = 231  OR  partitionIndex = 232  OR  partitionIndex = 128 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide73"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 107  OR  partitionIndex = 108 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide50"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 201  OR  partitionIndex = 202  OR  partitionIndex = 214  OR  partitionIndex = 215 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 213  OR  partitionIndex = 227  OR  partitionIndex = 228  OR  partitionIndex = 184 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide5"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 90  OR  partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 117 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide66"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 33  OR  partitionIndex = 47  OR  partitionIndex = 48  OR  partitionIndex = 4 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide80"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 200  OR  partitionIndex = 186  OR  partitionIndex = 187  OR  partitionIndex = 201 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide7"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 25  OR  partitionIndex = 26  OR  partitionIndex = 38  OR  partitionIndex = 39 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 118  OR  partitionIndex = 104  OR  partitionIndex = 105  OR  partitionIndex = 119 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide57"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 203  OR  partitionIndex = 204 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide46"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 185  OR  partitionIndex = 199  OR  partitionIndex = 200  OR  partitionIndex = 186 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide25"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 34  OR  partitionIndex = 35 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide4"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 147  OR  partitionIndex = 148 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 19  OR  partitionIndex = 20  OR  partitionIndex = 6  OR  partitionIndex = 7 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide51"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 205  OR  partitionIndex = 206 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide8"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 35  OR  partitionIndex = 49  OR  partitionIndex = 50  OR  partitionIndex = 36 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide77"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 81  OR  partitionIndex = 82 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide43"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 197  OR  partitionIndex = 198  OR  partitionIndex = 210  OR  partitionIndex = 211 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 87  OR  partitionIndex = 88  OR  partitionIndex = 74  OR  partitionIndex = 75 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 227  OR  partitionIndex = 228  OR  partitionIndex = 184  OR  partitionIndex = 185 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 70  OR  partitionIndex = 71  OR  partitionIndex = 85  OR  partitionIndex = 86 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide47"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 43  OR  partitionIndex = 57  OR  partitionIndex = 58  OR  partitionIndex = 44 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide36"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 109  OR  partitionIndex = 110  OR  partitionIndex = 96  OR  partitionIndex = 97 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide67"
val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 60  OR  partitionIndex = 61  OR  partitionIndex = 75  OR  partitionIndex = 76 ", 4))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

