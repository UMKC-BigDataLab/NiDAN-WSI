
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
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=149 AND partitionZIndex<=156")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=117 AND partitionZIndex<=124")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=79 AND partitionZIndex<=86")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=238 AND partitionZIndex<=245")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=17 AND partitionZIndex<=24")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=134 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=57.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=172 AND partitionZIndex<=179")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=41.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=218 AND partitionZIndex<=225")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=22 AND partitionZIndex<=29")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=150")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=93 AND partitionZIndex<=100")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=66.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=216 AND partitionZIndex<=223")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=153")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=23 AND partitionZIndex<=30")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=28.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=86 AND partitionZIndex<=93")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=40 AND partitionZIndex<=47")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=18.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=15 AND partitionZIndex<=22")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=67 AND partitionZIndex<=74")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=66.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=59 AND partitionZIndex<=66")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=198 AND partitionZIndex<=205")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=69.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=145 AND partitionZIndex<=152")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=69.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=6 AND partitionZIndex<=13")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=82.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=144 AND partitionZIndex<=151")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=174 AND partitionZIndex<=181")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=225 AND partitionZIndex<=232")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=73 AND partitionZIndex<=80")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=152 AND partitionZIndex<=159")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=202 AND partitionZIndex<=209")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=94.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=121 AND partitionZIndex<=128")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=118 AND partitionZIndex<=125")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=91.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=125 AND partitionZIndex<=132")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=22.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=185 AND partitionZIndex<=192")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=65.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=209 AND partitionZIndex<=216")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=36.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=68 AND partitionZIndex<=75")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=48 AND partitionZIndex<=55")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=189")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=158 AND partitionZIndex<=165")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=16 AND partitionZIndex<=23")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=38.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=141 AND partitionZIndex<=148")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=33.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=81 AND partitionZIndex<=88")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=44 AND partitionZIndex<=51")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=92.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=234 AND partitionZIndex<=241")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=94.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=109 AND partitionZIndex<=116")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=74.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=95 AND partitionZIndex<=102")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=150 AND partitionZIndex<=157")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=156 AND partitionZIndex<=163")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=82 AND partitionZIndex<=89")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=23.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=58 AND partitionZIndex<=65")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=137 AND partitionZIndex<=144")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=68.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=43 AND partitionZIndex<=50")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=155 AND partitionZIndex<=162")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=236 AND partitionZIndex<=243")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=74 AND partitionZIndex<=81")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=49.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=163 AND partitionZIndex<=170")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=10 AND partitionZIndex<=17")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=226 AND partitionZIndex<=233")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=173 AND partitionZIndex<=180")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=212 AND partitionZIndex<=219")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=88.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=72 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=33.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=37 AND partitionZIndex<=44")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=35 AND partitionZIndex<=42")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=136 AND partitionZIndex<=143")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=91 AND partitionZIndex<=98")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=66 AND partitionZIndex<=73")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=139 AND partitionZIndex<=146")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=49 AND partitionZIndex<=56")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=69.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=208 AND partitionZIndex<=215")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=26 AND partitionZIndex<=33")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=246 AND partitionZIndex<=253")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=241 AND partitionZIndex<=248")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=96 AND partitionZIndex<=103")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=160 AND partitionZIndex<=167")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=49.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=147 AND partitionZIndex<=154")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=65.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=75 AND partitionZIndex<=82")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=165 AND partitionZIndex<=172")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=69.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=54 AND partitionZIndex<=61")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=14.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=240 AND partitionZIndex<=247")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=126 AND partitionZIndex<=133")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=71.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=89 AND partitionZIndex<=96")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=95.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=161 AND partitionZIndex<=168")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=192 AND partitionZIndex<=199")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=157 AND partitionZIndex<=164")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=219 AND partitionZIndex<=226")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=77 AND partitionZIndex<=84")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=60.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=189 AND partitionZIndex<=196")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=14.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=85 AND partitionZIndex<=92")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=109")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=111 AND partitionZIndex<=118")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=69 AND partitionZIndex<=76")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=53.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=197 AND partitionZIndex<=204")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=30.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=55 AND partitionZIndex<=62")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=211")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=95.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=138 AND partitionZIndex<=145")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=82.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=35")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=14.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=18 AND partitionZIndex<=25")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=28")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=49.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=177")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=162 AND partitionZIndex<=169")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=65.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=167 AND partitionZIndex<=174")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=73.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=127 AND partitionZIndex<=134")
)



for (query <- queries){
show_timing{spark.read.parquet(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

