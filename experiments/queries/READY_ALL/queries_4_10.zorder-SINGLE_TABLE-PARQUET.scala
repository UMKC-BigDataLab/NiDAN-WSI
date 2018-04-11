
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
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=150 AND partitionZIndex<=153")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=69 AND partitionZIndex<=72")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=155 AND partitionZIndex<=158")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=125 AND partitionZIndex<=128")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=168 AND partitionZIndex<=171")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=68 AND partitionZIndex<=71")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=248 AND partitionZIndex<=251")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=197 AND partitionZIndex<=200")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=232 AND partitionZIndex<=235")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=222 AND partitionZIndex<=225")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=77 AND partitionZIndex<=80")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=98 AND partitionZIndex<=101")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=112 AND partitionZIndex<=115")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=147 AND partitionZIndex<=150")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=8 AND partitionZIndex<=11")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=103 AND partitionZIndex<=106")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=240 AND partitionZIndex<=243")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=80 AND partitionZIndex<=83")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=170 AND partitionZIndex<=173")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=60 AND partitionZIndex<=63")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=46 AND partitionZIndex<=49")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=193 AND partitionZIndex<=196")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=174 AND partitionZIndex<=177")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=199 AND partitionZIndex<=202")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=22 AND partitionZIndex<=25")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=59 AND partitionZIndex<=62")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=215 AND partitionZIndex<=218")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=160 AND partitionZIndex<=163")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=76 AND partitionZIndex<=79")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=3 AND partitionZIndex<=6")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=198 AND partitionZIndex<=201")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=180 AND partitionZIndex<=183")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=239 AND partitionZIndex<=242")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=244 AND partitionZIndex<=247")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=117 AND partitionZIndex<=120")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=1 AND partitionZIndex<=4")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=73 AND partitionZIndex<=76")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=23 AND partitionZIndex<=26")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=100 AND partitionZIndex<=103")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=187 AND partitionZIndex<=190")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=4 AND partitionZIndex<=7")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=243 AND partitionZIndex<=246")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=185 AND partitionZIndex<=188")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=203 AND partitionZIndex<=206")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=14 AND partitionZIndex<=17")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=30 AND partitionZIndex<=33")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=21 AND partitionZIndex<=24")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=11 AND partitionZIndex<=14")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=194 AND partitionZIndex<=197")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=176 AND partitionZIndex<=179")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=124 AND partitionZIndex<=127")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=34 AND partitionZIndex<=37")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=130 AND partitionZIndex<=133")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=205 AND partitionZIndex<=208")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=159 AND partitionZIndex<=162")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=118 AND partitionZIndex<=121")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=119 AND partitionZIndex<=122")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=153 AND partitionZIndex<=156")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=140 AND partitionZIndex<=143")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=56 AND partitionZIndex<=59")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=2 AND partitionZIndex<=5")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=67 AND partitionZIndex<=70")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=135 AND partitionZIndex<=138")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=191 AND partitionZIndex<=194")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=196 AND partitionZIndex<=199")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=83 AND partitionZIndex<=86")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=9.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=216 AND partitionZIndex<=219")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=32 AND partitionZIndex<=35")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=47 AND partitionZIndex<=50")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=16 AND partitionZIndex<=19")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=78 AND partitionZIndex<=81")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=251 AND partitionZIndex<=254")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=210 AND partitionZIndex<=213")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=107 AND partitionZIndex<=110")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=165 AND partitionZIndex<=168")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=37 AND partitionZIndex<=40")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=195 AND partitionZIndex<=198")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=204 AND partitionZIndex<=207")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=218 AND partitionZIndex<=221")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=3.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=15 AND partitionZIndex<=18")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=116 AND partitionZIndex<=119")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=86 AND partitionZIndex<=89")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=252 AND partitionZIndex<=255")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=207 AND partitionZIndex<=210")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=126 AND partitionZIndex<=129")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=156 AND partitionZIndex<=159")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=5.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=120 AND partitionZIndex<=123")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=183 AND partitionZIndex<=186")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=87 AND partitionZIndex<=90")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=85 AND partitionZIndex<=88")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=250 AND partitionZIndex<=253")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=230 AND partitionZIndex<=233")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=4.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=29 AND partitionZIndex<=32")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=1.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=142 AND partitionZIndex<=145")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=61 AND partitionZIndex<=64")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=2.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=31 AND partitionZIndex<=34")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=7.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=163 AND partitionZIndex<=166")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=6.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=217 AND partitionZIndex<=220")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=10.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=182 AND partitionZIndex<=185")
,
("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet/imageId=8.svs","SELECT imageBytes FROM data WHERE partitionZIndex>=36 AND partitionZIndex<=39")
)



for (query <- queries){
show_timing{spark.read.parquet(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	

sc.stop

