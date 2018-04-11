
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
"SELECT imageBytes FROM data WHERE (partitionZIndex>=149 AND partitionZIndex<=156) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=117 AND partitionZIndex<=124) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=79 AND partitionZIndex<=86) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=238 AND partitionZIndex<=245) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=17 AND partitionZIndex<=24) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=134 AND partitionZIndex<=141) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=172 AND partitionZIndex<=179) AND imageId='57.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=218 AND partitionZIndex<=225) AND imageId='41.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=22 AND partitionZIndex<=29) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=143 AND partitionZIndex<=150) AND imageId='55.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=93 AND partitionZIndex<=100) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=216 AND partitionZIndex<=223) AND imageId='66.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=146 AND partitionZIndex<=153) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=23 AND partitionZIndex<=30) AND imageId='37.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=86 AND partitionZIndex<=93) AND imageId='28.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=40 AND partitionZIndex<=47) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=15 AND partitionZIndex<=22) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=67 AND partitionZIndex<=74) AND imageId='51.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=59 AND partitionZIndex<=66) AND imageId='66.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=198 AND partitionZIndex<=205) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=145 AND partitionZIndex<=152) AND imageId='69.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=6 AND partitionZIndex<=13) AND imageId='69.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=144 AND partitionZIndex<=151) AND imageId='82.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=174 AND partitionZIndex<=181) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=225 AND partitionZIndex<=232) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=73 AND partitionZIndex<=80) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=152 AND partitionZIndex<=159) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=202 AND partitionZIndex<=209) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=121 AND partitionZIndex<=128) AND imageId='94.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=118 AND partitionZIndex<=125) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=125 AND partitionZIndex<=132) AND imageId='91.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=185 AND partitionZIndex<=192) AND imageId='22.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=209 AND partitionZIndex<=216) AND imageId='65.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=68 AND partitionZIndex<=75) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=48 AND partitionZIndex<=55) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=182 AND partitionZIndex<=189) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=158 AND partitionZIndex<=165) AND imageId='97.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=16 AND partitionZIndex<=23) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=141 AND partitionZIndex<=148) AND imageId='38.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=81 AND partitionZIndex<=88) AND imageId='33.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=44 AND partitionZIndex<=51) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=234 AND partitionZIndex<=241) AND imageId='92.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=109 AND partitionZIndex<=116) AND imageId='94.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=95 AND partitionZIndex<=102) AND imageId='74.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=150 AND partitionZIndex<=157) AND imageId='25.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=156 AND partitionZIndex<=163) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=82 AND partitionZIndex<=89) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=58 AND partitionZIndex<=65) AND imageId='23.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=137 AND partitionZIndex<=144) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=43 AND partitionZIndex<=50) AND imageId='68.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=155 AND partitionZIndex<=162) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=236 AND partitionZIndex<=243) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=74 AND partitionZIndex<=81) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=163 AND partitionZIndex<=170) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=10 AND partitionZIndex<=17) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=226 AND partitionZIndex<=233) AND imageId='96.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=173 AND partitionZIndex<=180) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=212 AND partitionZIndex<=219) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=72 AND partitionZIndex<=79) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=37 AND partitionZIndex<=44) AND imageId='33.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=35 AND partitionZIndex<=42) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=136 AND partitionZIndex<=143) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=91 AND partitionZIndex<=98) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=66 AND partitionZIndex<=73) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=139 AND partitionZIndex<=146) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=49 AND partitionZIndex<=56) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=208 AND partitionZIndex<=215) AND imageId='69.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=26 AND partitionZIndex<=33) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=246 AND partitionZIndex<=253) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=241 AND partitionZIndex<=248) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=96 AND partitionZIndex<=103) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=160 AND partitionZIndex<=167) AND imageId='37.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=147 AND partitionZIndex<=154) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=75 AND partitionZIndex<=82) AND imageId='65.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=165 AND partitionZIndex<=172) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=54 AND partitionZIndex<=61) AND imageId='69.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=240 AND partitionZIndex<=247) AND imageId='14.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=126 AND partitionZIndex<=133) AND imageId='96.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=89 AND partitionZIndex<=96) AND imageId='71.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=161 AND partitionZIndex<=168) AND imageId='95.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=192 AND partitionZIndex<=199) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=157 AND partitionZIndex<=164) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=219 AND partitionZIndex<=226) AND imageId='37.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=77 AND partitionZIndex<=84) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=189 AND partitionZIndex<=196) AND imageId='60.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=85 AND partitionZIndex<=92) AND imageId='14.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=102 AND partitionZIndex<=109) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=111 AND partitionZIndex<=118) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=69 AND partitionZIndex<=76) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=197 AND partitionZIndex<=204) AND imageId='53.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=55 AND partitionZIndex<=62) AND imageId='30.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=204 AND partitionZIndex<=211) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=138 AND partitionZIndex<=145) AND imageId='95.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=28 AND partitionZIndex<=35) AND imageId='82.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=18 AND partitionZIndex<=25) AND imageId='14.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=21 AND partitionZIndex<=28) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=170 AND partitionZIndex<=177) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=162 AND partitionZIndex<=169) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=167 AND partitionZIndex<=174) AND imageId='65.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=127 AND partitionZIndex<=134) AND imageId='73.svs' AND imageLevel=0"
)



show_timing{spark.read.orc("hdfs://ctl:9000//nidan/orc/KUDB10.ROWORDER.orc").createOrReplaceTempView("data")}

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

