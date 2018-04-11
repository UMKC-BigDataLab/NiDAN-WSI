import java.io.File
import java.io.FileOutputStream
import org.apache.spark.sql._

val queryMsg = "#QUERY "
val loadDBMsg = "#LOAD_DB "
val loadTable = "#LOAD_TABLE "
val loadsqlHive = "#LOAD_SQL_CONTEXT "
val dataSource = "/nidan/orc/individualORC/slide1"

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
  
  val queries = List(("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 140 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 236 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 60 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 105 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 202 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 83 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 235 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 74 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 167 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 33 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 231 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 72 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 123 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 225 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 95 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 3 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 153 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 165 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 11 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 208 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 157 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 160 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 117 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 107 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 66 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 220 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 191 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 184 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 40 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 114 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 221 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 164 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 111 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 67 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 183 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 25 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 59 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 119 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 217 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 5 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 215 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 63 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 69 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 219 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 79 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 43 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 185 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 229 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 50 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 194 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 228 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 90 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 204 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 12 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 106 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 45 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 126 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 76 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 175 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 87 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 177 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 197 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 9 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 110 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 14 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 21 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 41 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 118 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 201 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 173 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 36 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 227 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 75 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 206 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 180 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 207 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 77 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 151 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 93 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 30 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 125 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 187 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 122 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 134 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 170 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 232 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 68 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 8 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 99 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 86 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 73 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 31 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 195 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 51 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 23 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 16 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 136 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 205 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 218 ", 1),
("SELECT imageBytes FROM data WHERE WHERE  partitionIndex = 176 ", 1)
)

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

show_timing{sqlContext.read.orc(dataSource).createOrReplaceTempView("data")}

show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query._1}")
show_timing{sqlContext.sql(query._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}

