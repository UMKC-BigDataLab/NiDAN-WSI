
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
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=222")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=98 AND partitionZIndex<=98")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=147 AND partitionZIndex<=147")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=12 AND partitionZIndex<=12")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=210 AND partitionZIndex<=210")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=134 AND partitionZIndex<=134")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=123 AND partitionZIndex<=123")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=116")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=65 AND partitionZIndex<=65")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=38 AND partitionZIndex<=38")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=79 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=59 AND partitionZIndex<=59")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=168 AND partitionZIndex<=168")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=237 AND partitionZIndex<=237")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=200 AND partitionZIndex<=200")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=94 AND partitionZIndex<=94")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=234 AND partitionZIndex<=234")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=244 AND partitionZIndex<=244")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=111 AND partitionZIndex<=111")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=4 AND partitionZIndex<=4")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=250 AND partitionZIndex<=250")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=199 AND partitionZIndex<=199")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=248 AND partitionZIndex<=248")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=142 AND partitionZIndex<=142")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=194 AND partitionZIndex<=194")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=189 AND partitionZIndex<=189")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=213 AND partitionZIndex<=213")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=224 AND partitionZIndex<=224")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=173 AND partitionZIndex<=173")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=49 AND partitionZIndex<=49")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=226 AND partitionZIndex<=226")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=8 AND partitionZIndex<=8")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=227 AND partitionZIndex<=227")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=83 AND partitionZIndex<=83")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=152 AND partitionZIndex<=152")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=138 AND partitionZIndex<=138")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=128 AND partitionZIndex<=128")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=29 AND partitionZIndex<=29")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=172 AND partitionZIndex<=172")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=53 AND partitionZIndex<=53")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=47 AND partitionZIndex<=47")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=163 AND partitionZIndex<=163")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=78 AND partitionZIndex<=78")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=21")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=131 AND partitionZIndex<=131")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=216 AND partitionZIndex<=216")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=103 AND partitionZIndex<=103")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=19 AND partitionZIndex<=19")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=198 AND partitionZIndex<=198")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=105 AND partitionZIndex<=105")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=252 AND partitionZIndex<=252")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=64 AND partitionZIndex<=64")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=220 AND partitionZIndex<=220")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=170")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=221 AND partitionZIndex<=221")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=7 AND partitionZIndex<=7")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=204")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=73 AND partitionZIndex<=73")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=247 AND partitionZIndex<=247")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=28 AND partitionZIndex<=28")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=212 AND partitionZIndex<=212")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=166 AND partitionZIndex<=166")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=235 AND partitionZIndex<=235")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=129 AND partitionZIndex<=129")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=42 AND partitionZIndex<=42")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=209 AND partitionZIndex<=209")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=135 AND partitionZIndex<=135")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=182")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=39 AND partitionZIndex<=39")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=108 AND partitionZIndex<=108")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=240 AND partitionZIndex<=240")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=141 AND partitionZIndex<=141")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=33 AND partitionZIndex<=33")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=52 AND partitionZIndex<=52")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=153 AND partitionZIndex<=153")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=201 AND partitionZIndex<=201")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=112 AND partitionZIndex<=112")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=57 AND partitionZIndex<=57")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=30 AND partitionZIndex<=30")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=251 AND partitionZIndex<=251")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=230 AND partitionZIndex<=230")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=203 AND partitionZIndex<=203")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=60 AND partitionZIndex<=60")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=239 AND partitionZIndex<=239")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=143 AND partitionZIndex<=143")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=76 AND partitionZIndex<=76")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=86 AND partitionZIndex<=86")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=114 AND partitionZIndex<=114")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=178 AND partitionZIndex<=178")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=48 AND partitionZIndex<=48")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=44 AND partitionZIndex<=44")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=63 AND partitionZIndex<=63")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=15 AND partitionZIndex<=15")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=146 AND partitionZIndex<=146")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=236 AND partitionZIndex<=236")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=80 AND partitionZIndex<=80")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=238 AND partitionZIndex<=238")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=193 AND partitionZIndex<=193")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=5 AND partitionZIndex<=5")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=113 AND partitionZIndex<=113")
)



for (query <- queries){
show_timing{spark.read.parquet(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

