
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
"SELECT imageBytes FROM data WHERE ( partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 112  OR  partitionIndex = 113) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 202  OR  partitionIndex = 203  OR  partitionIndex = 217  OR  partitionIndex = 218) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 234  OR  partitionIndex = 235  OR  partitionIndex = 250  OR  partitionIndex = 251) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 1  OR  partitionIndex = 2  OR  partitionIndex = 16  OR  partitionIndex = 17) AND imageId='14.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 209  OR  partitionIndex = 210) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 142  OR  partitionIndex = 143  OR  partitionIndex = 157  OR  partitionIndex = 158) AND imageId='96.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 28  OR  partitionIndex = 59) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 130  OR  partitionIndex = 131  OR  partitionIndex = 145  OR  partitionIndex = 146) AND imageId='98.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 198  OR  partitionIndex = 199  OR  partitionIndex = 213  OR  partitionIndex = 214) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 28  OR  partitionIndex = 29) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 8  OR  partitionIndex = 103  OR  partitionIndex = 118  OR  partitionIndex = 119) AND imageId='72.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 228  OR  partitionIndex = 229) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 116  OR  partitionIndex = 117) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 12  OR  partitionIndex = 43  OR  partitionIndex = 58  OR  partitionIndex = 59) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 142  OR  partitionIndex = 143  OR  partitionIndex = 158  OR  partitionIndex = 159) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 8  OR  partitionIndex = 9  OR  partitionIndex = 118  OR  partitionIndex = 119) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 234  OR  partitionIndex = 235  OR  partitionIndex = 249  OR  partitionIndex = 250) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 156  OR  partitionIndex = 157) AND imageId='94.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 137  OR  partitionIndex = 138  OR  partitionIndex = 152  OR  partitionIndex = 153) AND imageId='74.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 218  OR  partitionIndex = 219  OR  partitionIndex = 232  OR  partitionIndex = 233) AND imageId='57.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 207  OR  partitionIndex = 222  OR  partitionIndex = 223  OR  partitionIndex = 236) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 205  OR  partitionIndex = 206  OR  partitionIndex = 220  OR  partitionIndex = 221) AND imageId='43.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 160  OR  partitionIndex = 161  OR  partitionIndex = 176  OR  partitionIndex = 177) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 75  OR  partitionIndex = 90  OR  partitionIndex = 91  OR  partitionIndex = 104) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 47  OR  partitionIndex = 62  OR  partitionIndex = 63  OR  partitionIndex = 72) AND imageId='53.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 71  OR  partitionIndex = 86  OR  partitionIndex = 87  OR  partitionIndex = 100) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 172  OR  partitionIndex = 173  OR  partitionIndex = 188  OR  partitionIndex = 189) AND imageId='22.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 61  OR  partitionIndex = 62) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 126  OR  partitionIndex = 127) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 167  OR  partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192) AND imageId='74.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 204  OR  partitionIndex = 235  OR  partitionIndex = 250  OR  partitionIndex = 251) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 9  OR  partitionIndex = 10  OR  partitionIndex = 24  OR  partitionIndex = 25) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 58  OR  partitionIndex = 59) AND imageId='17.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 98  OR  partitionIndex = 99  OR  partitionIndex = 112  OR  partitionIndex = 113) AND imageId='92.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 198  OR  partitionIndex = 199  OR  partitionIndex = 214  OR  partitionIndex = 215) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 23  OR  partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 52) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 233  OR  partitionIndex = 234  OR  partitionIndex = 248  OR  partitionIndex = 249) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 101  OR  partitionIndex = 102  OR  partitionIndex = 116  OR  partitionIndex = 117) AND imageId='98.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 150  OR  partitionIndex = 151) AND imageId='72.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 143  OR  partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 172) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 199  OR  partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 228) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 140  OR  partitionIndex = 171  OR  partitionIndex = 186  OR  partitionIndex = 187) AND imageId='77.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 88  OR  partitionIndex = 89) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 60  OR  partitionIndex = 61) AND imageId='25.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 174  OR  partitionIndex = 175  OR  partitionIndex = 188  OR  partitionIndex = 189) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 220  OR  partitionIndex = 251) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 106  OR  partitionIndex = 107  OR  partitionIndex = 121  OR  partitionIndex = 122) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 149  OR  partitionIndex = 150) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 76  OR  partitionIndex = 77  OR  partitionIndex = 122  OR  partitionIndex = 123) AND imageId='38.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 82  OR  partitionIndex = 83  OR  partitionIndex = 96  OR  partitionIndex = 97) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 215  OR  partitionIndex = 228  OR  partitionIndex = 229  OR  partitionIndex = 244) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 15  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 44) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 153  OR  partitionIndex = 154) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 236  OR  partitionIndex = 237  OR  partitionIndex = 252  OR  partitionIndex = 253) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 30  OR  partitionIndex = 31) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 36  OR  partitionIndex = 37  OR  partitionIndex = 52  OR  partitionIndex = 53) AND imageId='56.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 154  OR  partitionIndex = 155) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 161  OR  partitionIndex = 162  OR  partitionIndex = 176  OR  partitionIndex = 177) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 49  OR  partitionIndex = 50) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 53  OR  partitionIndex = 54) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193) AND imageId='82.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 45  OR  partitionIndex = 46  OR  partitionIndex = 60  OR  partitionIndex = 61) AND imageId='28.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 21  OR  partitionIndex = 22) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 57  OR  partitionIndex = 58) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 83  OR  partitionIndex = 96  OR  partitionIndex = 97  OR  partitionIndex = 112) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 195  OR  partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 224) AND imageId='48.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 69  OR  partitionIndex = 70  OR  partitionIndex = 84  OR  partitionIndex = 85) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 177  OR  partitionIndex = 178) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 226  OR  partitionIndex = 227  OR  partitionIndex = 242  OR  partitionIndex = 243) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 95  OR  partitionIndex = 108  OR  partitionIndex = 109  OR  partitionIndex = 124) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 184  OR  partitionIndex = 185) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 56  OR  partitionIndex = 57) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 77  OR  partitionIndex = 78  OR  partitionIndex = 92  OR  partitionIndex = 93) AND imageId='39.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 168  OR  partitionIndex = 169  OR  partitionIndex = 184  OR  partitionIndex = 185) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 208  OR  partitionIndex = 209) AND imageId='61.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 128  OR  partitionIndex = 129  OR  partitionIndex = 144  OR  partitionIndex = 145) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 50  OR  partitionIndex = 51) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 46  OR  partitionIndex = 47  OR  partitionIndex = 60  OR  partitionIndex = 61) AND imageId='41.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 152  OR  partitionIndex = 153) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 31  OR  partitionIndex = 44  OR  partitionIndex = 45  OR  partitionIndex = 60) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 81  OR  partitionIndex = 82) AND imageId='83.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 132  OR  partitionIndex = 133  OR  partitionIndex = 178  OR  partitionIndex = 179) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 164  OR  partitionIndex = 165) AND imageId='30.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 168  OR  partitionIndex = 169) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 169  OR  partitionIndex = 170  OR  partitionIndex = 184  OR  partitionIndex = 185) AND imageId='39.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 70  OR  partitionIndex = 71  OR  partitionIndex = 84  OR  partitionIndex = 85) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 80  OR  partitionIndex = 81) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 73  OR  partitionIndex = 74  OR  partitionIndex = 88  OR  partitionIndex = 89) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 230  OR  partitionIndex = 231  OR  partitionIndex = 244  OR  partitionIndex = 245) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 210  OR  partitionIndex = 211  OR  partitionIndex = 224  OR  partitionIndex = 225) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 11  OR  partitionIndex = 26  OR  partitionIndex = 27  OR  partitionIndex = 40) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 52  OR  partitionIndex = 53) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 132  OR  partitionIndex = 163  OR  partitionIndex = 178  OR  partitionIndex = 179) AND imageId='46.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 130  OR  partitionIndex = 131  OR  partitionIndex = 144  OR  partitionIndex = 145) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 206  OR  partitionIndex = 207  OR  partitionIndex = 221  OR  partitionIndex = 222) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 230  OR  partitionIndex = 231  OR  partitionIndex = 246  OR  partitionIndex = 247) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 185  OR  partitionIndex = 186) AND imageId='43.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 82  OR  partitionIndex = 83) AND imageId='95.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 193  OR  partitionIndex = 194  OR  partitionIndex = 208  OR  partitionIndex = 209) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 5  OR  partitionIndex = 6  OR  partitionIndex = 20  OR  partitionIndex = 21) AND imageId='90.svs' AND imageLevel=0"
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

