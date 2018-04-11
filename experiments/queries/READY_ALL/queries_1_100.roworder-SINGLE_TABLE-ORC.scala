
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionIndex = 253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionIndex = 198")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionIndex = 208")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=59.svs","SELECT imageBytes FROM data WHERE partitionIndex = 185")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionIndex = 88")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionIndex = 158")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionIndex = 156")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=93.svs","SELECT imageBytes FROM data WHERE partitionIndex = 140")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=91.svs","SELECT imageBytes FROM data WHERE partitionIndex = 160")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=67.svs","SELECT imageBytes FROM data WHERE partitionIndex = 238")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionIndex = 16")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 129")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=69.svs","SELECT imageBytes FROM data WHERE partitionIndex = 138")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionIndex = 100")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=24.svs","SELECT imageBytes FROM data WHERE partitionIndex = 115")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionIndex = 31")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=39.svs","SELECT imageBytes FROM data WHERE partitionIndex = 186")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionIndex = 159")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionIndex = 94")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionIndex = 200")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=47.svs","SELECT imageBytes FROM data WHERE partitionIndex = 73")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionIndex = 103")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionIndex = 221")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=59.svs","SELECT imageBytes FROM data WHERE partitionIndex = 8")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=43.svs","SELECT imageBytes FROM data WHERE partitionIndex = 169")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionIndex = 161")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionIndex = 7")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionIndex = 206")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=91.svs","SELECT imageBytes FROM data WHERE partitionIndex = 141")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionIndex = 143")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=36.svs","SELECT imageBytes FROM data WHERE partitionIndex = 216")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionIndex = 213")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionIndex = 148")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionIndex = 21")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionIndex = 52")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionIndex = 25")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionIndex = 164")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=65.svs","SELECT imageBytes FROM data WHERE partitionIndex = 165")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionIndex = 65")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionIndex = 2")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=47.svs","SELECT imageBytes FROM data WHERE partitionIndex = 134")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionIndex = 112")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionIndex = 132")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionIndex = 4")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionIndex = 232")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionIndex = 102")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=88.svs","SELECT imageBytes FROM data WHERE partitionIndex = 76")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionIndex = 105")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionIndex = 189")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionIndex = 19")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionIndex = 176")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionIndex = 162")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=46.svs","SELECT imageBytes FROM data WHERE partitionIndex = 182")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=53.svs","SELECT imageBytes FROM data WHERE partitionIndex = 175")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionIndex = 126")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionIndex = 241")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionIndex = 178")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionIndex = 18")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionIndex = 195")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 60")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=77.svs","SELECT imageBytes FROM data WHERE partitionIndex = 229")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=95.svs","SELECT imageBytes FROM data WHERE partitionIndex = 149")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionIndex = 82")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionIndex = 183")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=84.svs","SELECT imageBytes FROM data WHERE partitionIndex = 86")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionIndex = 71")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionIndex = 201")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=89.svs","SELECT imageBytes FROM data WHERE partitionIndex = 124")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionIndex = 14")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionIndex = 207")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionIndex = 54")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionIndex = 44")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionIndex = 30")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionIndex = 231")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionIndex = 38")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionIndex = 50")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionIndex = 39")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=41.svs","SELECT imageBytes FROM data WHERE partitionIndex = 41")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionIndex = 0")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionIndex = 135")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionIndex = 225")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionIndex = 226")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=89.svs","SELECT imageBytes FROM data WHERE partitionIndex = 174")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionIndex = 204")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionIndex = 199")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionIndex = 121")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=73.svs","SELECT imageBytes FROM data WHERE partitionIndex = 3")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionIndex = 209")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=75.svs","SELECT imageBytes FROM data WHERE partitionIndex = 230")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionIndex = 93")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionIndex = 170")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionIndex = 66")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionIndex = 248")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionIndex = 147")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=22.svs","SELECT imageBytes FROM data WHERE partitionIndex = 155")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionIndex = 97")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=66.svs","SELECT imageBytes FROM data WHERE partitionIndex = 133")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=18.svs","SELECT imageBytes FROM data WHERE partitionIndex = 62")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionIndex = 58")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=28.svs","SELECT imageBytes FROM data WHERE partitionIndex = 61")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

