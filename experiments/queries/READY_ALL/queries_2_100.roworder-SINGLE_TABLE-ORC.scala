
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionIndex = 69 OR partitionIndex = 84")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionIndex = 46 OR partitionIndex = 61")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionIndex = 98 OR partitionIndex = 113")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=50.svs","SELECT imageBytes FROM data WHERE partitionIndex = 232 OR partitionIndex = 233")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=38.svs","SELECT imageBytes FROM data WHERE partitionIndex = 225 OR partitionIndex = 240")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionIndex = 8 OR partitionIndex = 119")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionIndex = 40 OR partitionIndex = 41")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=24.svs","SELECT imageBytes FROM data WHERE partitionIndex = 80 OR partitionIndex = 81")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionIndex = 100 OR partitionIndex = 101")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionIndex = 0 OR partitionIndex = 1")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionIndex = 63 OR partitionIndex = 72")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionIndex = 142 OR partitionIndex = 143")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=70.svs","SELECT imageBytes FROM data WHERE partitionIndex = 75 OR partitionIndex = 90")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionIndex = 222 OR partitionIndex = 223")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 71 OR partitionIndex = 86")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionIndex = 126 OR partitionIndex = 127")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=67.svs","SELECT imageBytes FROM data WHERE partitionIndex = 168 OR partitionIndex = 169")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionIndex = 23 OR partitionIndex = 36")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionIndex = 41 OR partitionIndex = 56")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=41.svs","SELECT imageBytes FROM data WHERE partitionIndex = 180 OR partitionIndex = 181")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionIndex = 197 OR partitionIndex = 212")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionIndex = 231 OR partitionIndex = 246")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionIndex = 212 OR partitionIndex = 213")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=48.svs","SELECT imageBytes FROM data WHERE partitionIndex = 219 OR partitionIndex = 232")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionIndex = 96 OR partitionIndex = 97")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=73.svs","SELECT imageBytes FROM data WHERE partitionIndex = 5 OR partitionIndex = 20")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionIndex = 72 OR partitionIndex = 73")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionIndex = 42 OR partitionIndex = 57")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionIndex = 136 OR partitionIndex = 247")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionIndex = 12 OR partitionIndex = 59")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionIndex = 78 OR partitionIndex = 79")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=72.svs","SELECT imageBytes FROM data WHERE partitionIndex = 198 OR partitionIndex = 213")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 82 OR partitionIndex = 83")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 64 OR partitionIndex = 65")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionIndex = 174 OR partitionIndex = 175")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionIndex = 237 OR partitionIndex = 252")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionIndex = 131 OR partitionIndex = 146")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionIndex = 133 OR partitionIndex = 148")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionIndex = 166 OR partitionIndex = 181")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=94.svs","SELECT imageBytes FROM data WHERE partitionIndex = 190 OR partitionIndex = 191")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=11.svs","SELECT imageBytes FROM data WHERE partitionIndex = 70 OR partitionIndex = 85")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionIndex = 67 OR partitionIndex = 82")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=38.svs","SELECT imageBytes FROM data WHERE partitionIndex = 42 OR partitionIndex = 43")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=83.svs","SELECT imageBytes FROM data WHERE partitionIndex = 202 OR partitionIndex = 217")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionIndex = 19 OR partitionIndex = 32")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionIndex = 98 OR partitionIndex = 99")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionIndex = 141 OR partitionIndex = 156")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionIndex = 143 OR partitionIndex = 158")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionIndex = 227 OR partitionIndex = 242")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=17.svs","SELECT imageBytes FROM data WHERE partitionIndex = 178 OR partitionIndex = 179")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionIndex = 4 OR partitionIndex = 51")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionIndex = 20 OR partitionIndex = 21")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=23.svs","SELECT imageBytes FROM data WHERE partitionIndex = 226 OR partitionIndex = 227")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionIndex = 182 OR partitionIndex = 183")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionIndex = 77 OR partitionIndex = 92")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionIndex = 156 OR partitionIndex = 157")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=14.svs","SELECT imageBytes FROM data WHERE partitionIndex = 62 OR partitionIndex = 63")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=22.svs","SELECT imageBytes FROM data WHERE partitionIndex = 233 OR partitionIndex = 248")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=84.svs","SELECT imageBytes FROM data WHERE partitionIndex = 38 OR partitionIndex = 53")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionIndex = 47 OR partitionIndex = 62")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 234 OR partitionIndex = 249")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 106 OR partitionIndex = 121")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionIndex = 238 OR partitionIndex = 253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 120 OR partitionIndex = 121")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=67.svs","SELECT imageBytes FROM data WHERE partitionIndex = 83 OR partitionIndex = 96")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=33.svs","SELECT imageBytes FROM data WHERE partitionIndex = 43 OR partitionIndex = 58")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=30.svs","SELECT imageBytes FROM data WHERE partitionIndex = 162 OR partitionIndex = 177")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionIndex = 184 OR partitionIndex = 185")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionIndex = 68 OR partitionIndex = 69")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=83.svs","SELECT imageBytes FROM data WHERE partitionIndex = 97 OR partitionIndex = 112")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionIndex = 170 OR partitionIndex = 185")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionIndex = 139 OR partitionIndex = 154")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=11.svs","SELECT imageBytes FROM data WHERE partitionIndex = 134 OR partitionIndex = 149")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=82.svs","SELECT imageBytes FROM data WHERE partitionIndex = 191 OR partitionIndex = 200")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionIndex = 202 OR partitionIndex = 203")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=11.svs","SELECT imageBytes FROM data WHERE partitionIndex = 109 OR partitionIndex = 124")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionIndex = 4 OR partitionIndex = 5")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionIndex = 205 OR partitionIndex = 220")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 9 OR partitionIndex = 24")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 170 OR partitionIndex = 171")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionIndex = 2 OR partitionIndex = 17")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=70.svs","SELECT imageBytes FROM data WHERE partitionIndex = 242 OR partitionIndex = 243")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=81.svs","SELECT imageBytes FROM data WHERE partitionIndex = 228 OR partitionIndex = 229")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=48.svs","SELECT imageBytes FROM data WHERE partitionIndex = 52 OR partitionIndex = 53")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 103 OR partitionIndex = 118")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=30.svs","SELECT imageBytes FROM data WHERE partitionIndex = 31 OR partitionIndex = 44")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionIndex = 26 OR partitionIndex = 27")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionIndex = 138 OR partitionIndex = 139")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionIndex = 130 OR partitionIndex = 145")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionIndex = 147 OR partitionIndex = 160")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionIndex = 173 OR partitionIndex = 188")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=74.svs","SELECT imageBytes FROM data WHERE partitionIndex = 223 OR partitionIndex = 236")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=71.svs","SELECT imageBytes FROM data WHERE partitionIndex = 235 OR partitionIndex = 250")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 154 OR partitionIndex = 155")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionIndex = 45 OR partitionIndex = 60")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionIndex = 6 OR partitionIndex = 7")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionIndex = 230 OR partitionIndex = 245")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionIndex = 129 OR partitionIndex = 144")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=77.svs","SELECT imageBytes FROM data WHERE partitionIndex = 142 OR partitionIndex = 157")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=72.svs","SELECT imageBytes FROM data WHERE partitionIndex = 172 OR partitionIndex = 173")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

