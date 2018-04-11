
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
"SELECT imageBytes FROM data WHERE ( partitionIndex = 135  OR  partitionIndex = 150  OR  partitionIndex = 151  OR  partitionIndex = 164) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 26  OR  partitionIndex = 27) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 180  OR  partitionIndex = 181) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 126  OR  partitionIndex = 127) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 211  OR  partitionIndex = 224  OR  partitionIndex = 225  OR  partitionIndex = 240) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 25  OR  partitionIndex = 26) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 223  OR  partitionIndex = 236  OR  partitionIndex = 237  OR  partitionIndex = 252) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 154  OR  partitionIndex = 155) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 219  OR  partitionIndex = 232  OR  partitionIndex = 233  OR  partitionIndex = 248) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 175  OR  partitionIndex = 190  OR  partitionIndex = 191  OR  partitionIndex = 200) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 58  OR  partitionIndex = 59) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 73  OR  partitionIndex = 74  OR  partitionIndex = 88  OR  partitionIndex = 89) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 76  OR  partitionIndex = 77  OR  partitionIndex = 92  OR  partitionIndex = 123) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 134  OR  partitionIndex = 135  OR  partitionIndex = 148  OR  partitionIndex = 149) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 19  OR  partitionIndex = 32  OR  partitionIndex = 33  OR  partitionIndex = 48) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 90  OR  partitionIndex = 91  OR  partitionIndex = 104  OR  partitionIndex = 105) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 220  OR  partitionIndex = 251) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 12  OR  partitionIndex = 13  OR  partitionIndex = 28  OR  partitionIndex = 59) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 225  OR  partitionIndex = 226  OR  partitionIndex = 240  OR  partitionIndex = 241) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 117  OR  partitionIndex = 118) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 68  OR  partitionIndex = 99  OR  partitionIndex = 114  OR  partitionIndex = 115) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 152  OR  partitionIndex = 153) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 196  OR  partitionIndex = 227  OR  partitionIndex = 242  OR  partitionIndex = 243) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 168  OR  partitionIndex = 169) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 7  OR  partitionIndex = 22  OR  partitionIndex = 23  OR  partitionIndex = 36) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 116  OR  partitionIndex = 117) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 158  OR  partitionIndex = 159  OR  partitionIndex = 172  OR  partitionIndex = 173) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193  OR  partitionIndex = 208) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 42  OR  partitionIndex = 43  OR  partitionIndex = 57  OR  partitionIndex = 58) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 2  OR  partitionIndex = 3  OR  partitionIndex = 16  OR  partitionIndex = 17) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 139  OR  partitionIndex = 154  OR  partitionIndex = 155  OR  partitionIndex = 168) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 198  OR  partitionIndex = 199  OR  partitionIndex = 213  OR  partitionIndex = 214) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 204  OR  partitionIndex = 205  OR  partitionIndex = 250  OR  partitionIndex = 251) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 206  OR  partitionIndex = 207  OR  partitionIndex = 221  OR  partitionIndex = 222) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 78  OR  partitionIndex = 79  OR  partitionIndex = 94  OR  partitionIndex = 95) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 0  OR  partitionIndex = 1  OR  partitionIndex = 16  OR  partitionIndex = 17) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 40  OR  partitionIndex = 41  OR  partitionIndex = 56  OR  partitionIndex = 57) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 22  OR  partitionIndex = 23  OR  partitionIndex = 36  OR  partitionIndex = 37) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 74  OR  partitionIndex = 75  OR  partitionIndex = 89  OR  partitionIndex = 90) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 230  OR  partitionIndex = 231  OR  partitionIndex = 244  OR  partitionIndex = 245) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 2  OR  partitionIndex = 3  OR  partitionIndex = 17  OR  partitionIndex = 18) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 206  OR  partitionIndex = 207  OR  partitionIndex = 220  OR  partitionIndex = 221) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 228  OR  partitionIndex = 229  OR  partitionIndex = 244  OR  partitionIndex = 245) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 184  OR  partitionIndex = 185) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 4  OR  partitionIndex = 35  OR  partitionIndex = 50  OR  partitionIndex = 51) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 39  OR  partitionIndex = 54  OR  partitionIndex = 55  OR  partitionIndex = 64) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 6  OR  partitionIndex = 7  OR  partitionIndex = 22  OR  partitionIndex = 23) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 34  OR  partitionIndex = 35  OR  partitionIndex = 48  OR  partitionIndex = 49) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 137  OR  partitionIndex = 138  OR  partitionIndex = 152  OR  partitionIndex = 153) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 196  OR  partitionIndex = 197  OR  partitionIndex = 212  OR  partitionIndex = 243) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 110  OR  partitionIndex = 111  OR  partitionIndex = 125  OR  partitionIndex = 126) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 65  OR  partitionIndex = 66  OR  partitionIndex = 80  OR  partitionIndex = 81) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 129  OR  partitionIndex = 130  OR  partitionIndex = 144  OR  partitionIndex = 145) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 186  OR  partitionIndex = 187) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 182  OR  partitionIndex = 183  OR  partitionIndex = 192  OR  partitionIndex = 193) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 79  OR  partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 108) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 94  OR  partitionIndex = 95  OR  partitionIndex = 108  OR  partitionIndex = 109) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 164  OR  partitionIndex = 165  OR  partitionIndex = 180  OR  partitionIndex = 181) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 162  OR  partitionIndex = 163  OR  partitionIndex = 177  OR  partitionIndex = 178) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 87  OR  partitionIndex = 100  OR  partitionIndex = 101  OR  partitionIndex = 116) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 1  OR  partitionIndex = 2  OR  partitionIndex = 16  OR  partitionIndex = 17) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 10  OR  partitionIndex = 11  OR  partitionIndex = 24  OR  partitionIndex = 25) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 146  OR  partitionIndex = 147  OR  partitionIndex = 160  OR  partitionIndex = 161) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 136  OR  partitionIndex = 137  OR  partitionIndex = 246  OR  partitionIndex = 247) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 153  OR  partitionIndex = 154) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 28  OR  partitionIndex = 29) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 159  OR  partitionIndex = 172  OR  partitionIndex = 173  OR  partitionIndex = 188) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 55  OR  partitionIndex = 64  OR  partitionIndex = 65  OR  partitionIndex = 80) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 68  OR  partitionIndex = 69  OR  partitionIndex = 114  OR  partitionIndex = 115) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 20  OR  partitionIndex = 51) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 12  OR  partitionIndex = 43  OR  partitionIndex = 58  OR  partitionIndex = 59) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 238  OR  partitionIndex = 239  OR  partitionIndex = 252  OR  partitionIndex = 253) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 141  OR  partitionIndex = 142  OR  partitionIndex = 156  OR  partitionIndex = 157) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 106  OR  partitionIndex = 107  OR  partitionIndex = 120  OR  partitionIndex = 121) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 210  OR  partitionIndex = 211) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 82  OR  partitionIndex = 83) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 138  OR  partitionIndex = 139  OR  partitionIndex = 152  OR  partitionIndex = 153) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 170  OR  partitionIndex = 171  OR  partitionIndex = 185  OR  partitionIndex = 186) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 173  OR  partitionIndex = 174  OR  partitionIndex = 188  OR  partitionIndex = 189) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 4  OR  partitionIndex = 5  OR  partitionIndex = 50  OR  partitionIndex = 51) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 78  OR  partitionIndex = 79  OR  partitionIndex = 93  OR  partitionIndex = 94) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 15  OR  partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 44) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 238  OR  partitionIndex = 239  OR  partitionIndex = 253  OR  partitionIndex = 254) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 140  OR  partitionIndex = 141  OR  partitionIndex = 186  OR  partitionIndex = 187) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 111  OR  partitionIndex = 126  OR  partitionIndex = 127  OR  partitionIndex = 128) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 166  OR  partitionIndex = 167  OR  partitionIndex = 181  OR  partitionIndex = 182) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 95  OR  partitionIndex = 108  OR  partitionIndex = 109  OR  partitionIndex = 124) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 228  OR  partitionIndex = 229) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 30  OR  partitionIndex = 31  OR  partitionIndex = 44  OR  partitionIndex = 45) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 14  OR  partitionIndex = 15  OR  partitionIndex = 30  OR  partitionIndex = 31) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 237  OR  partitionIndex = 238  OR  partitionIndex = 252  OR  partitionIndex = 253) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 203  OR  partitionIndex = 218  OR  partitionIndex = 219  OR  partitionIndex = 232) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 38  OR  partitionIndex = 39  OR  partitionIndex = 54  OR  partitionIndex = 55) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 132  OR  partitionIndex = 163  OR  partitionIndex = 178  OR  partitionIndex = 179) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 102  OR  partitionIndex = 103  OR  partitionIndex = 118  OR  partitionIndex = 119) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 54  OR  partitionIndex = 55  OR  partitionIndex = 64  OR  partitionIndex = 65) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 194  OR  partitionIndex = 195  OR  partitionIndex = 208  OR  partitionIndex = 209) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 172  OR  partitionIndex = 173  OR  partitionIndex = 188  OR  partitionIndex = 189) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 199  OR  partitionIndex = 214  OR  partitionIndex = 215  OR  partitionIndex = 228) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE ( partitionIndex = 66  OR  partitionIndex = 67  OR  partitionIndex = 81  OR  partitionIndex = 82) AND imageId='8.svs' AND imageLevel=0"
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

