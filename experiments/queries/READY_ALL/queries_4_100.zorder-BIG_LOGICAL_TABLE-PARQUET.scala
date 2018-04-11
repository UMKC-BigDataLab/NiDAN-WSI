
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
"SELECT imageBytes FROM data WHERE (partitionZIndex>=41 AND partitionZIndex<=44) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=228 AND partitionZIndex<=231) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=237 AND partitionZIndex<=240) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=2 AND partitionZIndex<=5) AND imageId='14.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=164 AND partitionZIndex<=167) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=212 AND partitionZIndex<=215) AND imageId='96.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=80 AND partitionZIndex<=83) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=132 AND partitionZIndex<=135) AND imageId='98.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=180 AND partitionZIndex<=183) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=83 AND partitionZIndex<=86) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=62 AND partitionZIndex<=65) AND imageId='72.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=183 AND partitionZIndex<=186) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=57 AND partitionZIndex<=60) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=78 AND partitionZIndex<=81) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=213 AND partitionZIndex<=216) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=63 AND partitionZIndex<=66) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=236 AND partitionZIndex<=239) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=210 AND partitionZIndex<=213) AND imageId='94.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=194 AND partitionZIndex<=197) AND imageId='74.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=231 AND partitionZIndex<=234) AND imageId='57.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=246 AND partitionZIndex<=249) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=242 AND partitionZIndex<=245) AND imageId='43.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=137 AND partitionZIndex<=140) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=102 AND partitionZIndex<=105) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=94 AND partitionZIndex<=97) AND imageId='53.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=54 AND partitionZIndex<=57) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=217 AND partitionZIndex<=220) AND imageId='22.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=92 AND partitionZIndex<=95) AND imageId='15.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=125 AND partitionZIndex<=128) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=158 AND partitionZIndex<=161) AND imageId='74.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=238 AND partitionZIndex<=241) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=66 AND partitionZIndex<=69) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=79 AND partitionZIndex<=82) AND imageId='17.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=43 AND partitionZIndex<=46) AND imageId='92.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=181 AND partitionZIndex<=184) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=24 AND partitionZIndex<=27) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=234 AND partitionZIndex<=237) AND imageId='49.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=58 AND partitionZIndex<=61) AND imageId='98.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=149 AND partitionZIndex<=152) AND imageId='72.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=214 AND partitionZIndex<=217) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=182 AND partitionZIndex<=185) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=206 AND partitionZIndex<=209) AND imageId='77.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=99 AND partitionZIndex<=102) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=89 AND partitionZIndex<=92) AND imageId='25.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=219 AND partitionZIndex<=222) AND imageId='35.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=240 AND partitionZIndex<=243) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=108 AND partitionZIndex<=111) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=148 AND partitionZIndex<=151) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=111 AND partitionZIndex<=114) AND imageId='38.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=39 AND partitionZIndex<=42) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=184 AND partitionZIndex<=187) AND imageId='76.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=86 AND partitionZIndex<=89) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=196 AND partitionZIndex<=199) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=249 AND partitionZIndex<=252) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=85 AND partitionZIndex<=88) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=25 AND partitionZIndex<=28) AND imageId='56.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=197 AND partitionZIndex<=200) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=138 AND partitionZIndex<=141) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=12 AND partitionZIndex<=15) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=28 AND partitionZIndex<=31) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=159 AND partitionZIndex<=162) AND imageId='82.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=90 AND partitionZIndex<=93) AND imageId='28.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=20 AND partitionZIndex<=23) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=76 AND partitionZIndex<=79) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=40 AND partitionZIndex<=43) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=166 AND partitionZIndex<=169) AND imageId='48.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=50 AND partitionZIndex<=53) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=140 AND partitionZIndex<=143) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=173 AND partitionZIndex<=176) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=120 AND partitionZIndex<=123) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=203 AND partitionZIndex<=206) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=75 AND partitionZIndex<=78) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=114 AND partitionZIndex<=117) AND imageId='39.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=201 AND partitionZIndex<=204) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=163 AND partitionZIndex<=166) AND imageId='61.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=129 AND partitionZIndex<=132) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=13 AND partitionZIndex<=16) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=91 AND partitionZIndex<=94) AND imageId='41.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=193 AND partitionZIndex<=196) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=88 AND partitionZIndex<=91) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=36 AND partitionZIndex<=39) AND imageId='83.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=143 AND partitionZIndex<=146) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=151 AND partitionZIndex<=154) AND imageId='30.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=199 AND partitionZIndex<=202) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=202 AND partitionZIndex<=205) AND imageId='39.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=51 AND partitionZIndex<=54) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=35 AND partitionZIndex<=38) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=98 AND partitionZIndex<=101) AND imageId='4.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=187 AND partitionZIndex<=190) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=167 AND partitionZIndex<=170) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=70 AND partitionZIndex<=73) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=27 AND partitionZIndex<=30) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=142 AND partitionZIndex<=145) AND imageId='46.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=131 AND partitionZIndex<=134) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=244 AND partitionZIndex<=247) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=189 AND partitionZIndex<=192) AND imageId='32.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=204 AND partitionZIndex<=207) AND imageId='43.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=37 AND partitionZIndex<=40) AND imageId='95.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=162 AND partitionZIndex<=165) AND imageId='7.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=18 AND partitionZIndex<=21) AND imageId='90.svs' AND imageLevel=0"
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

