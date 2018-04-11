
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
"SELECT imageBytes FROM data WHERE ( partitionIndex = 69  OR  partitionIndex = 84) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 46  OR  partitionIndex = 61) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 98  OR  partitionIndex = 113) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 232  OR  partitionIndex = 233) AND imageId='50.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 225  OR  partitionIndex = 240) AND imageId='38.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 8  OR  partitionIndex = 119) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 40  OR  partitionIndex = 41) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 80  OR  partitionIndex = 81) AND imageId='24.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 100  OR  partitionIndex = 101) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 0  OR  partitionIndex = 1) AND imageId='61.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 63  OR  partitionIndex = 72) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 142  OR  partitionIndex = 143) AND imageId='55.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 75  OR  partitionIndex = 90) AND imageId='70.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 222  OR  partitionIndex = 223) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 71  OR  partitionIndex = 86) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 126  OR  partitionIndex = 127) AND imageId='25.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 168  OR  partitionIndex = 169) AND imageId='67.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 23  OR  partitionIndex = 36) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 41  OR  partitionIndex = 56) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 180  OR  partitionIndex = 181) AND imageId='41.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 197  OR  partitionIndex = 212) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 231  OR  partitionIndex = 246) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 212  OR  partitionIndex = 213) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 219  OR  partitionIndex = 232) AND imageId='48.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 96  OR  partitionIndex = 97) AND imageId='51.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 5  OR  partitionIndex = 20) AND imageId='73.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 72  OR  partitionIndex = 73) AND imageId='25.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 42  OR  partitionIndex = 57) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 136  OR  partitionIndex = 247) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 12  OR  partitionIndex = 59) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 78  OR  partitionIndex = 79) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 198  OR  partitionIndex = 213) AND imageId='72.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 82  OR  partitionIndex = 83) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 64  OR  partitionIndex = 65) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 174  OR  partitionIndex = 175) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 237  OR  partitionIndex = 252) AND imageId='96.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 131  OR  partitionIndex = 146) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 133  OR  partitionIndex = 148) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 166  OR  partitionIndex = 181) AND imageId='55.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 190  OR  partitionIndex = 191) AND imageId='94.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 70  OR  partitionIndex = 85) AND imageId='11.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 67  OR  partitionIndex = 82) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 42  OR  partitionIndex = 43) AND imageId='38.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 202  OR  partitionIndex = 217) AND imageId='83.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 19  OR  partitionIndex = 32) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 98  OR  partitionIndex = 99) AND imageId='25.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 141  OR  partitionIndex = 156) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 143  OR  partitionIndex = 158) AND imageId='97.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 227  OR  partitionIndex = 242) AND imageId='37.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 178  OR  partitionIndex = 179) AND imageId='17.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 4  OR  partitionIndex = 51) AND imageId='55.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 20  OR  partitionIndex = 21) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 226  OR  partitionIndex = 227) AND imageId='23.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 182  OR  partitionIndex = 183) AND imageId='97.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 77  OR  partitionIndex = 92) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 156  OR  partitionIndex = 157) AND imageId='97.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 62  OR  partitionIndex = 63) AND imageId='14.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 233  OR  partitionIndex = 248) AND imageId='22.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 38  OR  partitionIndex = 53) AND imageId='84.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 47  OR  partitionIndex = 62) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 234  OR  partitionIndex = 249) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 106  OR  partitionIndex = 121) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 238  OR  partitionIndex = 253) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 120  OR  partitionIndex = 121) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 83  OR  partitionIndex = 96) AND imageId='67.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 43  OR  partitionIndex = 58) AND imageId='33.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 162  OR  partitionIndex = 177) AND imageId='30.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 184  OR  partitionIndex = 185) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 68  OR  partitionIndex = 69) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 97  OR  partitionIndex = 112) AND imageId='83.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 185) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 139  OR  partitionIndex = 154) AND imageId='96.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 134  OR  partitionIndex = 149) AND imageId='11.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 191  OR  partitionIndex = 200) AND imageId='82.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 202  OR  partitionIndex = 203) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 109  OR  partitionIndex = 124) AND imageId='11.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 4  OR  partitionIndex = 5) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 205  OR  partitionIndex = 220) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 9  OR  partitionIndex = 24) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 171) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 2  OR  partitionIndex = 17) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 242  OR  partitionIndex = 243) AND imageId='70.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 228  OR  partitionIndex = 229) AND imageId='81.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 52  OR  partitionIndex = 53) AND imageId='48.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 103  OR  partitionIndex = 118) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 31  OR  partitionIndex = 44) AND imageId='30.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 26  OR  partitionIndex = 27) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138  OR  partitionIndex = 139) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 130  OR  partitionIndex = 145) AND imageId='61.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 147  OR  partitionIndex = 160) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 173  OR  partitionIndex = 188) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 223  OR  partitionIndex = 236) AND imageId='74.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 235  OR  partitionIndex = 250) AND imageId='71.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 154  OR  partitionIndex = 155) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 45  OR  partitionIndex = 60) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 6  OR  partitionIndex = 7) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 230  OR  partitionIndex = 245) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 129  OR  partitionIndex = 144) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 142  OR  partitionIndex = 157) AND imageId='77.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 172  OR  partitionIndex = 173) AND imageId='72.svs' AND imageLevel=0"
)



show_timing{spark.read.parquet("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet").createOrReplaceTempView("data")}

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

