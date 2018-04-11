
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
"SELECT imageBytes FROM data WHERE (partitionZIndex>=62 AND partitionZIndex<=63) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=115 AND partitionZIndex<=116) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=13 AND partitionZIndex<=14) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=189 AND partitionZIndex<=190) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=20 AND partitionZIndex<=21) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=181 AND partitionZIndex<=182) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=212 AND partitionZIndex<=213) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=89 AND partitionZIndex<=90) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=88 AND partitionZIndex<=89) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=110 AND partitionZIndex<=111) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=84 AND partitionZIndex<=85) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=30 AND partitionZIndex<=31) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=97 AND partitionZIndex<=98) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=149 AND partitionZIndex<=150) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=126 AND partitionZIndex<=127) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=80 AND partitionZIndex<=81) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=70 AND partitionZIndex<=71) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=198 AND partitionZIndex<=199) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=135 AND partitionZIndex<=136) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=233 AND partitionZIndex<=234) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=72 AND partitionZIndex<=73) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=232 AND partitionZIndex<=233) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=208 AND partitionZIndex<=209) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=229 AND partitionZIndex<=230) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=247 AND partitionZIndex<=248) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=140 AND partitionZIndex<=141) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=252 AND partitionZIndex<=253) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=158 AND partitionZIndex<=159) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=251 AND partitionZIndex<=252) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=96 AND partitionZIndex<=97) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=222 AND partitionZIndex<=223) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=146 AND partitionZIndex<=147) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=57 AND partitionZIndex<=58) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=170 AND partitionZIndex<=171) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=102 AND partitionZIndex<=103) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=215 AND partitionZIndex<=216) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=48 AND partitionZIndex<=49) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=195 AND partitionZIndex<=196) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=16 AND partitionZIndex<=17) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=18 AND partitionZIndex<=19) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=177 AND partitionZIndex<=178) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=152 AND partitionZIndex<=153) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=162 AND partitionZIndex<=163) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=225 AND partitionZIndex<=226) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=119 AND partitionZIndex<=120) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=101 AND partitionZIndex<=102) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=238 AND partitionZIndex<=239) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=24 AND partitionZIndex<=25) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=141 AND partitionZIndex<=142) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=155 AND partitionZIndex<=156) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=148 AND partitionZIndex<=149) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=244 AND partitionZIndex<=245) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=142 AND partitionZIndex<=143) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=127 AND partitionZIndex<=128) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=219 AND partitionZIndex<=220) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=100 AND partitionZIndex<=101) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=157 AND partitionZIndex<=158) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=114 AND partitionZIndex<=115) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=199 AND partitionZIndex<=200) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=116 AND partitionZIndex<=117) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=172 AND partitionZIndex<=173) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=21 AND partitionZIndex<=22) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=26 AND partitionZIndex<=27) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=64 AND partitionZIndex<=65) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=186 AND partitionZIndex<=187) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=60 AND partitionZIndex<=61) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=56 AND partitionZIndex<=57) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=87 AND partitionZIndex<=88) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=125 AND partitionZIndex<=126) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=94 AND partitionZIndex<=95) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=134 AND partitionZIndex<=135) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=32 AND partitionZIndex<=33) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=2 AND partitionZIndex<=3) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=105 AND partitionZIndex<=106) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=39 AND partitionZIndex<=40) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=78 AND partitionZIndex<=79) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=93 AND partitionZIndex<=94) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=204 AND partitionZIndex<=205) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=211 AND partitionZIndex<=212) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=164 AND partitionZIndex<=165) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=61 AND partitionZIndex<=62) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=237 AND partitionZIndex<=238) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=25 AND partitionZIndex<=26) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=171 AND partitionZIndex<=172) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=144 AND partitionZIndex<=145) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=133 AND partitionZIndex<=134) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=49 AND partitionZIndex<=50) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=188 AND partitionZIndex<=189) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=250 AND partitionZIndex<=251) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=7 AND partitionZIndex<=8) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=245 AND partitionZIndex<=246) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=28 AND partitionZIndex<=29) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=197 AND partitionZIndex<=198) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=117 AND partitionZIndex<=118) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=71 AND partitionZIndex<=72) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=6 AND partitionZIndex<=7) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=46 AND partitionZIndex<=47) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=226 AND partitionZIndex<=227) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=131 AND partitionZIndex<=132) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=10 AND partitionZIndex<=11) AND imageId='7.svs' AND imageLevel=0"
)



show_timing{spark.read.parquet("hdfs://ctl:9000//nidan/parquet/KUDB10.ROWORDER.parquet").createOrReplaceTempView("data")}

show_timing{spark.sql(queries(0))
.map(_.getAs[Array[Byte]](0))
.rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}
.collect.map(writeToLocal)
.filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query}")
show_timing{spark.sql(query).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}


sc.stop

