
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
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=41 AND partitionZIndex<=44")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=228 AND partitionZIndex<=231")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=237 AND partitionZIndex<=240")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=14.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=2 AND partitionZIndex<=5")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=164 AND partitionZIndex<=167")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=96.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=212 AND partitionZIndex<=215")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=80 AND partitionZIndex<=83")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=98.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=132 AND partitionZIndex<=135")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=180 AND partitionZIndex<=183")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=83 AND partitionZIndex<=86")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=72.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=62 AND partitionZIndex<=65")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=183 AND partitionZIndex<=186")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=57 AND partitionZIndex<=60")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=78 AND partitionZIndex<=81")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=213 AND partitionZIndex<=216")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=20.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=63 AND partitionZIndex<=66")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=236 AND partitionZIndex<=239")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=94.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=210 AND partitionZIndex<=213")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=74.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=194 AND partitionZIndex<=197")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=57.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=231 AND partitionZIndex<=234")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=246 AND partitionZIndex<=249")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=43.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=242 AND partitionZIndex<=245")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=49.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=137 AND partitionZIndex<=140")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=85.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=102 AND partitionZIndex<=105")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=53.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=94 AND partitionZIndex<=97")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=29.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=54 AND partitionZIndex<=57")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=22.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=217 AND partitionZIndex<=220")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=15.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=92 AND partitionZIndex<=95")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=88.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=125 AND partitionZIndex<=128")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=74.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=158 AND partitionZIndex<=161")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=238 AND partitionZIndex<=241")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=66 AND partitionZIndex<=69")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=17.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=79 AND partitionZIndex<=82")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=92.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=43 AND partitionZIndex<=46")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=36.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=181 AND partitionZIndex<=184")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=24 AND partitionZIndex<=27")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=49.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=234 AND partitionZIndex<=237")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=98.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=58 AND partitionZIndex<=61")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=72.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=149 AND partitionZIndex<=152")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=214 AND partitionZIndex<=217")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=80.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=185")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=77.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=206 AND partitionZIndex<=209")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=99 AND partitionZIndex<=102")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=25.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=89 AND partitionZIndex<=92")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=35.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=219 AND partitionZIndex<=222")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=18.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=240 AND partitionZIndex<=243")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=87.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=108 AND partitionZIndex<=111")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=36.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=148 AND partitionZIndex<=151")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=38.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=111 AND partitionZIndex<=114")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=42")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=76.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=184 AND partitionZIndex<=187")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=86 AND partitionZIndex<=89")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=196 AND partitionZIndex<=199")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=54.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=249 AND partitionZIndex<=252")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=85 AND partitionZIndex<=88")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=56.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=25 AND partitionZIndex<=28")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=197 AND partitionZIndex<=200")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=31.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=138 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=44.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=12 AND partitionZIndex<=15")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=31")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=82.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=159 AND partitionZIndex<=162")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=28.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=90 AND partitionZIndex<=93")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=20 AND partitionZIndex<=23")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=76 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=40 AND partitionZIndex<=43")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=48.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=166 AND partitionZIndex<=169")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=50 AND partitionZIndex<=53")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=18.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=140 AND partitionZIndex<=143")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=19.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=173 AND partitionZIndex<=176")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=78.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=120 AND partitionZIndex<=123")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=79.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=203 AND partitionZIndex<=206")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=75 AND partitionZIndex<=78")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=39.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=114 AND partitionZIndex<=117")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=100.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=201 AND partitionZIndex<=204")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=61.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=163 AND partitionZIndex<=166")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=129 AND partitionZIndex<=132")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=64.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=13 AND partitionZIndex<=16")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=41.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=91 AND partitionZIndex<=94")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=36.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=193 AND partitionZIndex<=196")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=52.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=88 AND partitionZIndex<=91")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=83.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=36 AND partitionZIndex<=39")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=88.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=146")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=30.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=151 AND partitionZIndex<=154")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=199 AND partitionZIndex<=202")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=39.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=202 AND partitionZIndex<=205")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=34.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=51 AND partitionZIndex<=54")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=35 AND partitionZIndex<=38")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=98 AND partitionZIndex<=101")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=40.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=187 AND partitionZIndex<=190")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=86.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=167 AND partitionZIndex<=170")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=21.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=70 AND partitionZIndex<=73")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=27 AND partitionZIndex<=30")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=46.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=142 AND partitionZIndex<=145")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=12.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=131 AND partitionZIndex<=134")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=244 AND partitionZIndex<=247")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=32.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=189 AND partitionZIndex<=192")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=43.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=207")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=95.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=37 AND partitionZIndex<=40")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=162 AND partitionZIndex<=165")
,
("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc/imageId=90.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=18 AND partitionZIndex<=21")
)



for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

