
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
"SELECT imageBytes FROM data WHERE (partitionZIndex>=222 AND partitionZIndex<=222) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=98 AND partitionZIndex<=98) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=147 AND partitionZIndex<=147) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=12 AND partitionZIndex<=12) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=210 AND partitionZIndex<=210) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=134 AND partitionZIndex<=134) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=123 AND partitionZIndex<=123) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=116 AND partitionZIndex<=116) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=65 AND partitionZIndex<=65) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=38 AND partitionZIndex<=38) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=79 AND partitionZIndex<=79) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=59 AND partitionZIndex<=59) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=168 AND partitionZIndex<=168) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=237 AND partitionZIndex<=237) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=200 AND partitionZIndex<=200) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=94 AND partitionZIndex<=94) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=234 AND partitionZIndex<=234) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=244 AND partitionZIndex<=244) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=111 AND partitionZIndex<=111) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=4 AND partitionZIndex<=4) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=250 AND partitionZIndex<=250) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=199 AND partitionZIndex<=199) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=248 AND partitionZIndex<=248) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=142 AND partitionZIndex<=142) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=194 AND partitionZIndex<=194) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=189 AND partitionZIndex<=189) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=213 AND partitionZIndex<=213) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=224 AND partitionZIndex<=224) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=173 AND partitionZIndex<=173) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=49 AND partitionZIndex<=49) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=226 AND partitionZIndex<=226) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=8 AND partitionZIndex<=8) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=227 AND partitionZIndex<=227) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=83 AND partitionZIndex<=83) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=152 AND partitionZIndex<=152) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=138 AND partitionZIndex<=138) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=128 AND partitionZIndex<=128) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=29 AND partitionZIndex<=29) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=172 AND partitionZIndex<=172) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=53 AND partitionZIndex<=53) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=47 AND partitionZIndex<=47) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=163 AND partitionZIndex<=163) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=78 AND partitionZIndex<=78) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=21 AND partitionZIndex<=21) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=131 AND partitionZIndex<=131) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=216 AND partitionZIndex<=216) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=103 AND partitionZIndex<=103) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=19 AND partitionZIndex<=19) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=198 AND partitionZIndex<=198) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=105 AND partitionZIndex<=105) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=252 AND partitionZIndex<=252) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=64 AND partitionZIndex<=64) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=220 AND partitionZIndex<=220) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=170 AND partitionZIndex<=170) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=221 AND partitionZIndex<=221) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=7 AND partitionZIndex<=7) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=204 AND partitionZIndex<=204) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=73 AND partitionZIndex<=73) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=247 AND partitionZIndex<=247) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=28 AND partitionZIndex<=28) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=212 AND partitionZIndex<=212) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=166 AND partitionZIndex<=166) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=235 AND partitionZIndex<=235) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=129 AND partitionZIndex<=129) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=42 AND partitionZIndex<=42) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=209 AND partitionZIndex<=209) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=135 AND partitionZIndex<=135) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=182 AND partitionZIndex<=182) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=39 AND partitionZIndex<=39) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=108 AND partitionZIndex<=108) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=240 AND partitionZIndex<=240) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=141 AND partitionZIndex<=141) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=33 AND partitionZIndex<=33) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=52 AND partitionZIndex<=52) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=153 AND partitionZIndex<=153) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=201 AND partitionZIndex<=201) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=112 AND partitionZIndex<=112) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=57 AND partitionZIndex<=57) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=30 AND partitionZIndex<=30) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=251 AND partitionZIndex<=251) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=230 AND partitionZIndex<=230) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=203 AND partitionZIndex<=203) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=60 AND partitionZIndex<=60) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=239 AND partitionZIndex<=239) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=143 AND partitionZIndex<=143) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=76 AND partitionZIndex<=76) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=86 AND partitionZIndex<=86) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=114 AND partitionZIndex<=114) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=178 AND partitionZIndex<=178) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=48 AND partitionZIndex<=48) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=44 AND partitionZIndex<=44) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=63 AND partitionZIndex<=63) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=15 AND partitionZIndex<=15) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=146 AND partitionZIndex<=146) AND imageId='8.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=236 AND partitionZIndex<=236) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=80 AND partitionZIndex<=80) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=238 AND partitionZIndex<=238) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=193 AND partitionZIndex<=193) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=5 AND partitionZIndex<=5) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=113 AND partitionZIndex<=113) AND imageId='8.svs' AND imageLevel=0"
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

