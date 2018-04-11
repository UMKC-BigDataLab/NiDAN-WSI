
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
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=166 AND partitionZIndex<=173")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=9 AND partitionZIndex<=16")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=128 AND partitionZIndex<=135")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=247 AND partitionZIndex<=254")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=55 AND partitionZIndex<=62")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=95 AND partitionZIndex<=102")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=142 AND partitionZIndex<=149")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=159 AND partitionZIndex<=166")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=1 AND partitionZIndex<=8")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=209 AND partitionZIndex<=216")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=151 AND partitionZIndex<=158")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=114 AND partitionZIndex<=121")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=123")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=164 AND partitionZIndex<=171")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=200 AND partitionZIndex<=207")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=60 AND partitionZIndex<=67")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=181 AND partitionZIndex<=188")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=179 AND partitionZIndex<=186")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=49 AND partitionZIndex<=56")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=46")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=228 AND partitionZIndex<=235")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=92 AND partitionZIndex<=99")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=155")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=32 AND partitionZIndex<=39")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=10 AND partitionZIndex<=17")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=37 AND partitionZIndex<=44")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=35")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=61 AND partitionZIndex<=68")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=89 AND partitionZIndex<=96")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=38 AND partitionZIndex<=45")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=188 AND partitionZIndex<=195")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=113 AND partitionZIndex<=120")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=174 AND partitionZIndex<=181")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=20 AND partitionZIndex<=27")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=69 AND partitionZIndex<=76")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=119 AND partitionZIndex<=126")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=177")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=205 AND partitionZIndex<=212")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=111 AND partitionZIndex<=118")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=15 AND partitionZIndex<=22")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=81 AND partitionZIndex<=88")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=180 AND partitionZIndex<=187")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=147 AND partitionZIndex<=154")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=186 AND partitionZIndex<=193")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=70 AND partitionZIndex<=77")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=231 AND partitionZIndex<=238")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=175 AND partitionZIndex<=182")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=48 AND partitionZIndex<=55")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=34 AND partitionZIndex<=41")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=77 AND partitionZIndex<=84")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=167 AND partitionZIndex<=174")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=144 AND partitionZIndex<=151")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=229")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=246 AND partitionZIndex<=253")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=194 AND partitionZIndex<=201")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=28")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=47 AND partitionZIndex<=54")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=100 AND partitionZIndex<=107")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=176 AND partitionZIndex<=183")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=63 AND partitionZIndex<=70")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=122 AND partitionZIndex<=129")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=201 AND partitionZIndex<=208")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=238 AND partitionZIndex<=245")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=211")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=54 AND partitionZIndex<=61")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=13 AND partitionZIndex<=20")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=67 AND partitionZIndex<=74")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=241 AND partitionZIndex<=248")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=46 AND partitionZIndex<=53")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=74 AND partitionZIndex<=81")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=109")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=93 AND partitionZIndex<=100")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=150 AND partitionZIndex<=157")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=137 AND partitionZIndex<=144")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=163 AND partitionZIndex<=170")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=133 AND partitionZIndex<=140")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=123 AND partitionZIndex<=130")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=5 AND partitionZIndex<=12")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=134 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=76 AND partitionZIndex<=83")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=56 AND partitionZIndex<=63")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=99 AND partitionZIndex<=106")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=14 AND partitionZIndex<=21")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=30 AND partitionZIndex<=37")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=224 AND partitionZIndex<=231")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=172 AND partitionZIndex<=179")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=42 AND partitionZIndex<=49")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=196 AND partitionZIndex<=203")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=160 AND partitionZIndex<=167")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=44 AND partitionZIndex<=51")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=87 AND partitionZIndex<=94")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=26 AND partitionZIndex<=33")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=51 AND partitionZIndex<=58")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=236 AND partitionZIndex<=243")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=36 AND partitionZIndex<=43")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=211 AND partitionZIndex<=218")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=227 AND partitionZIndex<=234")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=242 AND partitionZIndex<=249")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=18 AND partitionZIndex<=25")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=62 AND partitionZIndex<=69")
)



for (query <- queries){
show_timing{spark.read.parquet(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

