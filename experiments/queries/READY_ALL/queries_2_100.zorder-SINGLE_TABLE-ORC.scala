
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=50 AND partitionZIndex<=51")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=92 AND partitionZIndex<=93")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=44 AND partitionZIndex<=45")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=50.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=233 AND partitionZIndex<=234")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=38.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=171")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=64 AND partitionZIndex<=65")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=73 AND partitionZIndex<=74")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=24.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=35 AND partitionZIndex<=36")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=57 AND partitionZIndex<=58")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=1 AND partitionZIndex<=2")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=96 AND partitionZIndex<=97")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=213 AND partitionZIndex<=214")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=70.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=103")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=247 AND partitionZIndex<=248")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=54 AND partitionZIndex<=55")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=127 AND partitionZIndex<=128")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=67.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=201 AND partitionZIndex<=202")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=24 AND partitionZIndex<=25")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=74 AND partitionZIndex<=75")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=41.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=155 AND partitionZIndex<=156")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=178 AND partitionZIndex<=179")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=190 AND partitionZIndex<=191")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=179 AND partitionZIndex<=180")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=48.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=232 AND partitionZIndex<=233")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=41 AND partitionZIndex<=42")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=73.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=18 AND partitionZIndex<=19")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=97 AND partitionZIndex<=98")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=76 AND partitionZIndex<=77")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=192 AND partitionZIndex<=193")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=80 AND partitionZIndex<=81")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=117 AND partitionZIndex<=118")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=72.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=180 AND partitionZIndex<=181")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=40")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=33 AND partitionZIndex<=34")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=221 AND partitionZIndex<=222")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=250 AND partitionZIndex<=251")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=134 AND partitionZIndex<=135")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=147")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=156 AND partitionZIndex<=157")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=94.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=223 AND partitionZIndex<=224")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=11.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=52 AND partitionZIndex<=53")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=38 AND partitionZIndex<=39")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=38.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=77 AND partitionZIndex<=78")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=83.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=228 AND partitionZIndex<=229")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=8 AND partitionZIndex<=9")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=45 AND partitionZIndex<=46")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=210 AND partitionZIndex<=211")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=214 AND partitionZIndex<=215")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=174 AND partitionZIndex<=175")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=17.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=144")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=55.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=16 AND partitionZIndex<=17")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=19 AND partitionZIndex<=20")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=23.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=173 AND partitionZIndex<=174")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=159 AND partitionZIndex<=160")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=114 AND partitionZIndex<=115")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=211 AND partitionZIndex<=212")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=14.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=95 AND partitionZIndex<=96")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=22.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=234 AND partitionZIndex<=235")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=84.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=29")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=94 AND partitionZIndex<=95")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=236 AND partitionZIndex<=237")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=108 AND partitionZIndex<=109")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=252 AND partitionZIndex<=253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=107 AND partitionZIndex<=108")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=67.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=40 AND partitionZIndex<=41")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=33.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=78 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=30.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=140 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=203 AND partitionZIndex<=204")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=49 AND partitionZIndex<=50")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=83.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=42 AND partitionZIndex<=43")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=205")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=198 AND partitionZIndex<=199")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=11.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=149")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=82.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=224 AND partitionZIndex<=225")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=229 AND partitionZIndex<=230")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=11.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=122 AND partitionZIndex<=123")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=17 AND partitionZIndex<=18")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=242 AND partitionZIndex<=243")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=66 AND partitionZIndex<=67")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=205 AND partitionZIndex<=206")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=4 AND partitionZIndex<=5")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=70.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=175 AND partitionZIndex<=176")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=81.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=185 AND partitionZIndex<=186")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=48.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=27 AND partitionZIndex<=28")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=62 AND partitionZIndex<=63")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=30.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=88 AND partitionZIndex<=89")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=71 AND partitionZIndex<=72")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=197 AND partitionZIndex<=198")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=132 AND partitionZIndex<=133")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=136 AND partitionZIndex<=137")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=218 AND partitionZIndex<=219")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=74.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=248 AND partitionZIndex<=249")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=71.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=238 AND partitionZIndex<=239")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=199 AND partitionZIndex<=200")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=90 AND partitionZIndex<=91")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=22")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=188 AND partitionZIndex<=189")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=130 AND partitionZIndex<=131")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=77.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=212 AND partitionZIndex<=213")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=72.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=217 AND partitionZIndex<=218")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

