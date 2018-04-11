
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 103 OR partitionIndex = 118")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 92 OR partitionIndex = 93")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 34 OR partitionIndex = 35")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 230 OR partitionIndex = 231")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 6 OR partitionIndex = 21")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 198 OR partitionIndex = 199")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 142 OR partitionIndex = 157")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 44 OR partitionIndex = 45")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 31 OR partitionIndex = 44")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 107 OR partitionIndex = 122")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 14 OR partitionIndex = 29")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 39 OR partitionIndex = 54")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 72 OR partitionIndex = 73")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 134 OR partitionIndex = 135")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 111 OR partitionIndex = 126")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 12 OR partitionIndex = 59")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 11 OR partitionIndex = 26")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 139 OR partitionIndex = 154")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 146 OR partitionIndex = 147")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 232 OR partitionIndex = 233")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 27 OR partitionIndex = 40")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 219 OR partitionIndex = 232")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 140 OR partitionIndex = 187")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 202 OR partitionIndex = 203")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 222 OR partitionIndex = 223")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 162 OR partitionIndex = 177")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 238 OR partitionIndex = 253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 167 OR partitionIndex = 182")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 252 OR partitionIndex = 253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 63 OR partitionIndex = 72")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 175 OR partitionIndex = 190")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 133 OR partitionIndex = 148")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 100 OR partitionIndex = 101")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 225 OR partitionIndex = 240")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 75 OR partitionIndex = 90")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 158 OR partitionIndex = 159")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 68 OR partitionIndex = 115")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 152 OR partitionIndex = 153")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 4 OR partitionIndex = 51")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 5 OR partitionIndex = 20")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 196 OR partitionIndex = 197")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 151 OR partitionIndex = 164")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 193 OR partitionIndex = 208")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 200 OR partitionIndex = 201")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 94 OR partitionIndex = 95")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 74 OR partitionIndex = 75")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 235 OR partitionIndex = 250")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 23 OR partitionIndex = 36")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 162 OR partitionIndex = 163")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 180 OR partitionIndex = 181")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 134 OR partitionIndex = 149")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 206 OR partitionIndex = 221")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 163 OR partitionIndex = 178")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 126 OR partitionIndex = 127")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 188 OR partitionIndex = 189")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 74 OR partitionIndex = 89")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 166 OR partitionIndex = 167")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 77 OR partitionIndex = 92")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 154 OR partitionIndex = 155")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 78 OR partitionIndex = 93")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 226 OR partitionIndex = 241")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 6 OR partitionIndex = 7")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 37 OR partitionIndex = 52")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 8 OR partitionIndex = 119")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 229 OR partitionIndex = 244")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 102 OR partitionIndex = 117")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 87 OR partitionIndex = 100")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 30 OR partitionIndex = 31")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 110 OR partitionIndex = 111")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 47 OR partitionIndex = 62")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 131 OR partitionIndex = 146")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 55 OR partitionIndex = 64")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 1 OR partitionIndex = 16")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 104 OR partitionIndex = 105")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 82 OR partitionIndex = 83")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 43 OR partitionIndex = 58")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 46 OR partitionIndex = 47")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 170 OR partitionIndex = 185")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 156 OR partitionIndex = 157")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 194 OR partitionIndex = 209")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 102 OR partitionIndex = 103")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 234 OR partitionIndex = 235")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 36 OR partitionIndex = 37")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 240 OR partitionIndex = 241")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 132 OR partitionIndex = 179")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 130 OR partitionIndex = 131")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 68 OR partitionIndex = 69")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 230 OR partitionIndex = 245")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 237 OR partitionIndex = 252")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 18 OR partitionIndex = 19")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 206 OR partitionIndex = 207")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 38 OR partitionIndex = 53")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 138 OR partitionIndex = 139")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 78 OR partitionIndex = 79")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 26 OR partitionIndex = 27")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 3 OR partitionIndex = 18")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 99 OR partitionIndex = 114")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 201 OR partitionIndex = 216")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionIndex = 144 OR partitionIndex = 145")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 33 OR partitionIndex = 48")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

