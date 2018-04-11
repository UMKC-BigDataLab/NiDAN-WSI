
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=252 AND partitionZIndex<=252")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=181 AND partitionZIndex<=181")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=163 AND partitionZIndex<=163")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=59.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=204")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=99 AND partitionZIndex<=99")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=215 AND partitionZIndex<=215")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=211 AND partitionZIndex<=211")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=93.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=209 AND partitionZIndex<=209")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=91.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=137 AND partitionZIndex<=137")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=67.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=253 AND partitionZIndex<=253")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=3 AND partitionZIndex<=3")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=130 AND partitionZIndex<=130")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=69.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=197 AND partitionZIndex<=197")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=57 AND partitionZIndex<=57")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=24.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=48 AND partitionZIndex<=48")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=88 AND partitionZIndex<=88")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=39.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=207 AND partitionZIndex<=207")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=216 AND partitionZIndex<=216")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=119 AND partitionZIndex<=119")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=225 AND partitionZIndex<=225")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=47.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=98 AND partitionZIndex<=98")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=62 AND partitionZIndex<=62")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=244 AND partitionZIndex<=244")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=59.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=65 AND partitionZIndex<=65")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=43.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=202 AND partitionZIndex<=202")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=138 AND partitionZIndex<=138")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=22 AND partitionZIndex<=22")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=245 AND partitionZIndex<=245")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=91.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=210 AND partitionZIndex<=210")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=214 AND partitionZIndex<=214")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=36.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=227 AND partitionZIndex<=227")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=97.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=180 AND partitionZIndex<=180")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=147 AND partitionZIndex<=147")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=20 AND partitionZIndex<=20")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=27 AND partitionZIndex<=27")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=68 AND partitionZIndex<=68")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=153 AND partitionZIndex<=153")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=65.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=154 AND partitionZIndex<=154")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=34 AND partitionZIndex<=34")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=51.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=5 AND partitionZIndex<=5")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=47.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=149 AND partitionZIndex<=149")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=43 AND partitionZIndex<=43")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=145 AND partitionZIndex<=145")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=17 AND partitionZIndex<=17")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=37.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=233 AND partitionZIndex<=233")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=61 AND partitionZIndex<=61")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=88.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=113 AND partitionZIndex<=113")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=106 AND partitionZIndex<=106")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=99.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=220 AND partitionZIndex<=220")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=8 AND partitionZIndex<=8")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=139 AND partitionZIndex<=139")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=141 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=46.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=159 AND partitionZIndex<=159")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=53.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=222")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=127 AND partitionZIndex<=127")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=172 AND partitionZIndex<=172")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=27.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=143")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=7 AND partitionZIndex<=7")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=166 AND partitionZIndex<=166")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=91 AND partitionZIndex<=91")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=77.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=186 AND partitionZIndex<=186")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=95.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=148")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=39")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=160 AND partitionZIndex<=160")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=84.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=55 AND partitionZIndex<=55")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=42.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=54 AND partitionZIndex<=54")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=16.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=226 AND partitionZIndex<=226")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=89.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=123 AND partitionZIndex<=123")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=85 AND partitionZIndex<=85")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=246 AND partitionZIndex<=246")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=31 AND partitionZIndex<=31")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=89 AND partitionZIndex<=89")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=87 AND partitionZIndex<=87")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=190 AND partitionZIndex<=190")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=29 AND partitionZIndex<=29")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=15 AND partitionZIndex<=15")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=30 AND partitionZIndex<=30")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=41.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=74 AND partitionZIndex<=74")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=1 AND partitionZIndex<=1")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=150 AND partitionZIndex<=150")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=170")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=173 AND partitionZIndex<=173")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=89.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=221 AND partitionZIndex<=221")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=241 AND partitionZIndex<=241")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=13.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=182")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=108 AND partitionZIndex<=108")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=73.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=6 AND partitionZIndex<=6")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=164 AND partitionZIndex<=164")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=75.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=189 AND partitionZIndex<=189")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=116")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=205 AND partitionZIndex<=205")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=37 AND partitionZIndex<=37")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=58.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=235 AND partitionZIndex<=235")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=45.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=136 AND partitionZIndex<=136")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=22.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=200 AND partitionZIndex<=200")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=42 AND partitionZIndex<=42")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=66.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=146")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=18.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=95 AND partitionZIndex<=95")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=79 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=28.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=92 AND partitionZIndex<=92")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

