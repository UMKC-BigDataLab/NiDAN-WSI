
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


val queries = Seq(
"SELECT imageBytes FROM data WHERE ( partitionIndex = 253) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 198) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 208) AND imageId='61.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 185) AND imageId='59.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 88) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 158) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 156) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 140) AND imageId='93.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 160) AND imageId='91.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 238) AND imageId='67.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 16) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 129) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138) AND imageId='69.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 100) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 115) AND imageId='24.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 31) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 186) AND imageId='39.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 159) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 94) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 200) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 73) AND imageId='47.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 103) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 221) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 8) AND imageId='59.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 169) AND imageId='43.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 161) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 7) AND imageId='51.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 206) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 141) AND imageId='91.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 143) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 216) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 213) AND imageId='97.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 148) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 21) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 52) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 25) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 164) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 165) AND imageId='65.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 65) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 2) AND imageId='51.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 134) AND imageId='47.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 112) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 132) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 4) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 232) AND imageId='37.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 102) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 76) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 105) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 189) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 19) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 176) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 162) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 182) AND imageId='46.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 175) AND imageId='53.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 126) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 241) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 178) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 18) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 195) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 60) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 229) AND imageId='77.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 149) AND imageId='95.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 82) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 183) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 86) AND imageId='84.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 71) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 201) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 124) AND imageId='89.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 14) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 207) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 54) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 44) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 30) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 231) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 38) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 50) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 39) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 41) AND imageId='41.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 0) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 135) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 225) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 226) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 174) AND imageId='89.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 204) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 199) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 121) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 3) AND imageId='73.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 209) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 230) AND imageId='75.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 93) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 66) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 248) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 147) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 155) AND imageId='22.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 97) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 133) AND imageId='66.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 62) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 58) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 61) AND imageId='28.svs' AND imageLevel=0"
)



show_timing{spark.read.orc("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc").createOrReplaceTempView("data")}

show_timing{spark.sql(queries(0))
.map(_.getAs[Array[Byte]](0))
.rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}
.collect.map(writeToLocal)
.filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query}")
show_timing{spark.sql(query).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}


sc.stop

