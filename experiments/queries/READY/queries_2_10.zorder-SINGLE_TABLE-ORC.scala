
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=62 AND partitionZIndex<=63")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=115 AND partitionZIndex<=116")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=13 AND partitionZIndex<=14")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=189 AND partitionZIndex<=190")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=20 AND partitionZIndex<=21")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=181 AND partitionZIndex<=182")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=212 AND partitionZIndex<=213")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=89 AND partitionZIndex<=90")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=88 AND partitionZIndex<=89")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=110 AND partitionZIndex<=111")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=84 AND partitionZIndex<=85")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=30 AND partitionZIndex<=31")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=97 AND partitionZIndex<=98")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=149 AND partitionZIndex<=150")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=126 AND partitionZIndex<=127")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=80 AND partitionZIndex<=81")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=70 AND partitionZIndex<=71")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=198 AND partitionZIndex<=199")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=135 AND partitionZIndex<=136")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=233 AND partitionZIndex<=234")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=72 AND partitionZIndex<=73")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=232 AND partitionZIndex<=233")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=208 AND partitionZIndex<=209")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=229 AND partitionZIndex<=230")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=247 AND partitionZIndex<=248")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=140 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=252 AND partitionZIndex<=253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=158 AND partitionZIndex<=159")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=251 AND partitionZIndex<=252")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=96 AND partitionZIndex<=97")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=223")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=147")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=57 AND partitionZIndex<=58")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=171")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=103")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=215 AND partitionZIndex<=216")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=48 AND partitionZIndex<=49")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=195 AND partitionZIndex<=196")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=16 AND partitionZIndex<=17")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=18 AND partitionZIndex<=19")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=177 AND partitionZIndex<=178")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=152 AND partitionZIndex<=153")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=162 AND partitionZIndex<=163")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=225 AND partitionZIndex<=226")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=119 AND partitionZIndex<=120")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=101 AND partitionZIndex<=102")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=238 AND partitionZIndex<=239")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=24 AND partitionZIndex<=25")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=141 AND partitionZIndex<=142")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=155 AND partitionZIndex<=156")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=149")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=244 AND partitionZIndex<=245")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=142 AND partitionZIndex<=143")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=127 AND partitionZIndex<=128")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=219 AND partitionZIndex<=220")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=100 AND partitionZIndex<=101")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=157 AND partitionZIndex<=158")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=114 AND partitionZIndex<=115")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=199 AND partitionZIndex<=200")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=117")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=172 AND partitionZIndex<=173")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=22")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=26 AND partitionZIndex<=27")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=64 AND partitionZIndex<=65")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=186 AND partitionZIndex<=187")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=60 AND partitionZIndex<=61")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=56 AND partitionZIndex<=57")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=87 AND partitionZIndex<=88")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=125 AND partitionZIndex<=126")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=94 AND partitionZIndex<=95")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=134 AND partitionZIndex<=135")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=32 AND partitionZIndex<=33")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=2 AND partitionZIndex<=3")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=105 AND partitionZIndex<=106")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=40")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=78 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=93 AND partitionZIndex<=94")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=205")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=211 AND partitionZIndex<=212")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=164 AND partitionZIndex<=165")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=61 AND partitionZIndex<=62")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=237 AND partitionZIndex<=238")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=25 AND partitionZIndex<=26")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=171 AND partitionZIndex<=172")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=144 AND partitionZIndex<=145")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=133 AND partitionZIndex<=134")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=49 AND partitionZIndex<=50")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=188 AND partitionZIndex<=189")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=250 AND partitionZIndex<=251")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=7 AND partitionZIndex<=8")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=245 AND partitionZIndex<=246")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=29")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=197 AND partitionZIndex<=198")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=117 AND partitionZIndex<=118")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=71 AND partitionZIndex<=72")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=6 AND partitionZIndex<=7")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=46 AND partitionZIndex<=47")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=226 AND partitionZIndex<=227")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=131 AND partitionZIndex<=132")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=10 AND partitionZIndex<=11")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

