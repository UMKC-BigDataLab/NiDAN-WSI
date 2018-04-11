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

 
val dataSource = "/nidan/orc/individualORC/slide21"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 180  OR  partitionIndex = 181 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 78  OR  partitionIndex = 79  OR  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 108  OR  partitionIndex = 109  OR  partitionIndex = 124  OR  partitionIndex = 125 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide3"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 58  OR  partitionIndex = 59 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 235  OR  partitionIndex = 250  OR  partitionIndex = 251 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 23 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide100"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 131  OR  partitionIndex = 146  OR  partitionIndex = 147  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 162  OR  partitionIndex = 176  OR  partitionIndex = 177 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide57"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 196  OR  partitionIndex = 197  OR  partitionIndex = 212  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 241  OR  partitionIndex = 242  OR  partitionIndex = 243 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide41"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 173  OR  partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 200 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 7  OR  partitionIndex = 22  OR  partitionIndex = 23  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 38  OR  partitionIndex = 52  OR  partitionIndex = 53 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide55"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 178  OR  partitionIndex = 179 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide34"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 62  OR  partitionIndex = 63  OR  partitionIndex = 72  OR  partitionIndex = 73  OR  partitionIndex = 88  OR  partitionIndex = 89 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide66"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 159  OR  partitionIndex = 172  OR  partitionIndex = 173  OR  partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 190 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide2"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 133  OR  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 164 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide37"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 22  OR  partitionIndex = 23  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 52  OR  partitionIndex = 53 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide28"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 15  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 46  OR  partitionIndex = 60  OR  partitionIndex = 61 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide80"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 83  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 112  OR  partitionIndex = 113  OR  partitionIndex = 114 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide18"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 50  OR  partitionIndex = 51 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide51"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 24  OR  partitionIndex = 25  OR  partitionIndex = 26  OR  partitionIndex = 27  OR  partitionIndex = 40  OR  partitionIndex = 41 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide66"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 8  OR  partitionIndex = 9  OR  partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 116  OR  partitionIndex = 117  OR  partitionIndex = 118  OR  partitionIndex = 119 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide85"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 139  OR  partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 168  OR  partitionIndex = 169  OR  partitionIndex = 170  OR  partitionIndex = 184  OR  partitionIndex = 185 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 150  OR  partitionIndex = 151 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 3  OR  partitionIndex = 18  OR  partitionIndex = 19  OR  partitionIndex = 32  OR  partitionIndex = 33  OR  partitionIndex = 34  OR  partitionIndex = 48  OR  partitionIndex = 49 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide82"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 150  OR  partitionIndex = 179 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide54"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 196  OR  partitionIndex = 197  OR  partitionIndex = 198  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 227  OR  partitionIndex = 242  OR  partitionIndex = 243 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide13"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 200  OR  partitionIndex = 201  OR  partitionIndex = 202  OR  partitionIndex = 203  OR  partitionIndex = 216  OR  partitionIndex = 217  OR  partitionIndex = 218  OR  partitionIndex = 219 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide79"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 56  OR  partitionIndex = 57  OR  partitionIndex = 58  OR  partitionIndex = 59 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide40"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 151  OR  partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 180  OR  partitionIndex = 181  OR  partitionIndex = 182 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 140  OR  partitionIndex = 169  OR  partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 184  OR  partitionIndex = 185  OR  partitionIndex = 186  OR  partitionIndex = 187 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide94"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 108  OR  partitionIndex = 109  OR  partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 124  OR  partitionIndex = 125  OR  partitionIndex = 126  OR  partitionIndex = 127 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide58"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 79  OR  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 108  OR  partitionIndex = 109  OR  partitionIndex = 110  OR  partitionIndex = 124  OR  partitionIndex = 125 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide91"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 144  OR  partitionIndex = 145 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide22"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 228  OR  partitionIndex = 229  OR  partitionIndex = 230  OR  partitionIndex = 231  OR  partitionIndex = 244  OR  partitionIndex = 245  OR  partitionIndex = 246  OR  partitionIndex = 247 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 143  OR  partitionIndex = 156  OR  partitionIndex = 157  OR  partitionIndex = 158  OR  partitionIndex = 159 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide36"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 25  OR  partitionIndex = 26  OR  partitionIndex = 27  OR  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 56 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 70  OR  partitionIndex = 71  OR  partitionIndex = 84  OR  partitionIndex = 85  OR  partitionIndex = 86  OR  partitionIndex = 115 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide80"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 199  OR  partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 228  OR  partitionIndex = 229  OR  partitionIndex = 230  OR  partitionIndex = 244  OR  partitionIndex = 245 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide97"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 167  OR  partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 208  OR  partitionIndex = 209 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide31"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 51 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide38"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 178  OR  partitionIndex = 179 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 31 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide87"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 84  OR  partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 113  OR  partitionIndex = 114  OR  partitionIndex = 115 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide92"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 204  OR  partitionIndex = 233  OR  partitionIndex = 234  OR  partitionIndex = 235  OR  partitionIndex = 248  OR  partitionIndex = 249  OR  partitionIndex = 250  OR  partitionIndex = 251 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide94"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 76  OR  partitionIndex = 77  OR  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 106  OR  partitionIndex = 107  OR  partitionIndex = 122  OR  partitionIndex = 123 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide74"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 62  OR  partitionIndex = 63  OR  partitionIndex = 72  OR  partitionIndex = 73  OR  partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 88  OR  partitionIndex = 89 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide25"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 135  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 166  OR  partitionIndex = 180  OR  partitionIndex = 181 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 181  OR  partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 208 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide80"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 13  OR  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 44 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide23"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 8  OR  partitionIndex = 101  OR  partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 116  OR  partitionIndex = 117  OR  partitionIndex = 118  OR  partitionIndex = 119 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 176  OR  partitionIndex = 177  OR  partitionIndex = 178  OR  partitionIndex = 179 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide68"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 112  OR  partitionIndex = 113  OR  partitionIndex = 114  OR  partitionIndex = 115 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide35"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 180  OR  partitionIndex = 181  OR  partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide20"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 220  OR  partitionIndex = 234  OR  partitionIndex = 235  OR  partitionIndex = 249  OR  partitionIndex = 250  OR  partitionIndex = 251 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 41  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 56  OR  partitionIndex = 57  OR  partitionIndex = 58  OR  partitionIndex = 59 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide49"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 208  OR  partitionIndex = 209  OR  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 224  OR  partitionIndex = 225 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide7"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 4  OR  partitionIndex = 33  OR  partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 48  OR  partitionIndex = 49  OR  partitionIndex = 50  OR  partitionIndex = 51 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide96"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 201  OR  partitionIndex = 202  OR  partitionIndex = 203  OR  partitionIndex = 216  OR  partitionIndex = 217  OR  partitionIndex = 218  OR  partitionIndex = 219  OR  partitionIndex = 232 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 196  OR  partitionIndex = 197  OR  partitionIndex = 212  OR  partitionIndex = 213  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 242  OR  partitionIndex = 243 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide32"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 142  OR  partitionIndex = 143  OR  partitionIndex = 157  OR  partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 172  OR  partitionIndex = 173  OR  partitionIndex = 188 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide88"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 27  OR  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 56  OR  partitionIndex = 57  OR  partitionIndex = 58 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide33"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 82  OR  partitionIndex = 83  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 112  OR  partitionIndex = 113 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 80  OR  partitionIndex = 81  OR  partitionIndex = 82  OR  partitionIndex = 83  OR  partitionIndex = 96  OR  partitionIndex = 97 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide86"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 147  OR  partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 176  OR  partitionIndex = 177  OR  partitionIndex = 178 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide79"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 60  OR  partitionIndex = 61  OR  partitionIndex = 62  OR  partitionIndex = 63  OR  partitionIndex = 72  OR  partitionIndex = 73 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide78"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 9  OR  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 24  OR  partitionIndex = 25  OR  partitionIndex = 26  OR  partitionIndex = 27  OR  partitionIndex = 40 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 176  OR  partitionIndex = 177  OR  partitionIndex = 178  OR  partitionIndex = 179 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide80"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 70  OR  partitionIndex = 71  OR  partitionIndex = 84  OR  partitionIndex = 85  OR  partitionIndex = 86  OR  partitionIndex = 87 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 143  OR  partitionIndex = 156  OR  partitionIndex = 157  OR  partitionIndex = 158  OR  partitionIndex = 187 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide27"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 37  OR  partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 52  OR  partitionIndex = 53  OR  partitionIndex = 54  OR  partitionIndex = 55  OR  partitionIndex = 64 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide10"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 207  OR  partitionIndex = 222  OR  partitionIndex = 223  OR  partitionIndex = 236  OR  partitionIndex = 237  OR  partitionIndex = 238  OR  partitionIndex = 252  OR  partitionIndex = 253 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide42"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 207  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 222  OR  partitionIndex = 223 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide16"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 63  OR  partitionIndex = 72  OR  partitionIndex = 73  OR  partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 88  OR  partitionIndex = 89  OR  partitionIndex = 90 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide37"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 208  OR  partitionIndex = 209  OR  partitionIndex = 210 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide49"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 148  OR  partitionIndex = 149  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 164  OR  partitionIndex = 165 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 56  OR  partitionIndex = 57  OR  partitionIndex = 58  OR  partitionIndex = 59 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide1"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 224  OR  partitionIndex = 225  OR  partitionIndex = 240  OR  partitionIndex = 241 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide69"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 71  OR  partitionIndex = 86  OR  partitionIndex = 87  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 102  OR  partitionIndex = 116  OR  partitionIndex = 117 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 207  OR  partitionIndex = 220  OR  partitionIndex = 221  OR  partitionIndex = 222  OR  partitionIndex = 251 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide96"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 111  OR  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 130  OR  partitionIndex = 144  OR  partitionIndex = 145 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide71"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 60  OR  partitionIndex = 61  OR  partitionIndex = 62  OR  partitionIndex = 63 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide95"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 208  OR  partitionIndex = 209  OR  partitionIndex = 210  OR  partitionIndex = 211 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide54"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 152  OR  partitionIndex = 153  OR  partitionIndex = 154  OR  partitionIndex = 247 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide27"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 208  OR  partitionIndex = 209 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide37"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 188  OR  partitionIndex = 189  OR  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 200  OR  partitionIndex = 201 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide21"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 28  OR  partitionIndex = 29  OR  partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 58  OR  partitionIndex = 59 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide60"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 152  OR  partitionIndex = 153  OR  partitionIndex = 230  OR  partitionIndex = 231  OR  partitionIndex = 246  OR  partitionIndex = 247 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 60  OR  partitionIndex = 61 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide44"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 75  OR  partitionIndex = 90  OR  partitionIndex = 91  OR  partitionIndex = 104  OR  partitionIndex = 105  OR  partitionIndex = 106  OR  partitionIndex = 120  OR  partitionIndex = 121 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide9"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 76  OR  partitionIndex = 77  OR  partitionIndex = 78  OR  partitionIndex = 79  OR  partitionIndex = 92  OR  partitionIndex = 93  OR  partitionIndex = 122  OR  partitionIndex = 123 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide45"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 26  OR  partitionIndex = 27  OR  partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 56  OR  partitionIndex = 57 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide53"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 168  OR  partitionIndex = 169  OR  partitionIndex = 184  OR  partitionIndex = 185 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide30"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 86  OR  partitionIndex = 87  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 116  OR  partitionIndex = 117 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide99"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 156  OR  partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 185  OR  partitionIndex = 186  OR  partitionIndex = 187 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide95"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 132  OR  partitionIndex = 161  OR  partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 176  OR  partitionIndex = 177  OR  partitionIndex = 178  OR  partitionIndex = 179 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide82"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 53  OR  partitionIndex = 54  OR  partitionIndex = 55  OR  partitionIndex = 64  OR  partitionIndex = 65  OR  partitionIndex = 80 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide14"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 20  OR  partitionIndex = 21  OR  partitionIndex = 22  OR  partitionIndex = 23  OR  partitionIndex = 36 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide78"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 22  OR  partitionIndex = 23  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 52  OR  partitionIndex = 53 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide49"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 196  OR  partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 240  OR  partitionIndex = 241  OR  partitionIndex = 242  OR  partitionIndex = 243 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide15"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 208  OR  partitionIndex = 209  OR  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 224 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide65"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 224  OR  partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 240  OR  partitionIndex = 241 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

val dataSource = "/nidan/orc/individualORC/slide73"
val queries = List(("SELECT imageBytes FROM data WHERE  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 130  OR  partitionIndex = 131  OR  partitionIndex = 144  OR  partitionIndex = 145 ", 8))

// query
show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

