
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
"SELECT imageBytes FROM data WHERE (partitionZIndex>=252 AND partitionZIndex<=252) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=181 AND partitionZIndex<=181) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=163 AND partitionZIndex<=163) AND imageId='61.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=204 AND partitionZIndex<=204) AND imageId='59.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=99 AND partitionZIndex<=99) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=215 AND partitionZIndex<=215) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=211 AND partitionZIndex<=211) AND imageId='10.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=209 AND partitionZIndex<=209) AND imageId='93.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=137 AND partitionZIndex<=137) AND imageId='91.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=253 AND partitionZIndex<=253) AND imageId='67.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=3 AND partitionZIndex<=3) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=130 AND partitionZIndex<=130) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=197 AND partitionZIndex<=197) AND imageId='69.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=57 AND partitionZIndex<=57) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=48 AND partitionZIndex<=48) AND imageId='24.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=88 AND partitionZIndex<=88) AND imageId='44.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=207 AND partitionZIndex<=207) AND imageId='39.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=216 AND partitionZIndex<=216) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=119 AND partitionZIndex<=119) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=225 AND partitionZIndex<=225) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=98 AND partitionZIndex<=98) AND imageId='47.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=62 AND partitionZIndex<=62) AND imageId='6.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=244 AND partitionZIndex<=244) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=65 AND partitionZIndex<=65) AND imageId='59.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=202 AND partitionZIndex<=202) AND imageId='43.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=138 AND partitionZIndex<=138) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=22 AND partitionZIndex<=22) AND imageId='51.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=245 AND partitionZIndex<=245) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=210 AND partitionZIndex<=210) AND imageId='91.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=214 AND partitionZIndex<=214) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=227 AND partitionZIndex<=227) AND imageId='36.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=180 AND partitionZIndex<=180) AND imageId='97.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=147 AND partitionZIndex<=147) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=20 AND partitionZIndex<=20) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=27 AND partitionZIndex<=27) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=68 AND partitionZIndex<=68) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=153 AND partitionZIndex<=153) AND imageId='78.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=154 AND partitionZIndex<=154) AND imageId='65.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=34 AND partitionZIndex<=34) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=5 AND partitionZIndex<=5) AND imageId='51.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=149 AND partitionZIndex<=149) AND imageId='47.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=43 AND partitionZIndex<=43) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=145 AND partitionZIndex<=145) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=17 AND partitionZIndex<=17) AND imageId='12.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=233 AND partitionZIndex<=233) AND imageId='37.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=61 AND partitionZIndex<=61) AND imageId='80.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=113 AND partitionZIndex<=113) AND imageId='88.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=106 AND partitionZIndex<=106) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=220 AND partitionZIndex<=220) AND imageId='99.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=8 AND partitionZIndex<=8) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=139 AND partitionZIndex<=139) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=141 AND partitionZIndex<=141) AND imageId='3.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=159 AND partitionZIndex<=159) AND imageId='46.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=222 AND partitionZIndex<=222) AND imageId='53.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=127 AND partitionZIndex<=127) AND imageId='21.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=172 AND partitionZIndex<=172) AND imageId='29.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=143 AND partitionZIndex<=143) AND imageId='27.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=7 AND partitionZIndex<=7) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=166 AND partitionZIndex<=166) AND imageId='64.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=91 AND partitionZIndex<=91) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=186 AND partitionZIndex<=186) AND imageId='77.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=148 AND partitionZIndex<=148) AND imageId='95.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=39 AND partitionZIndex<=39) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=160 AND partitionZIndex<=160) AND imageId='100.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=55 AND partitionZIndex<=55) AND imageId='84.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=54 AND partitionZIndex<=54) AND imageId='42.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=226 AND partitionZIndex<=226) AND imageId='16.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=123 AND partitionZIndex<=123) AND imageId='89.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=85 AND partitionZIndex<=85) AND imageId='5.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=246 AND partitionZIndex<=246) AND imageId='52.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=31 AND partitionZIndex<=31) AND imageId='85.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=89 AND partitionZIndex<=89) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=87 AND partitionZIndex<=87) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=190 AND partitionZIndex<=190) AND imageId='34.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=29 AND partitionZIndex<=29) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=15 AND partitionZIndex<=15) AND imageId='90.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=30 AND partitionZIndex<=30) AND imageId='9.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=74 AND partitionZIndex<=74) AND imageId='41.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=1 AND partitionZIndex<=1) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=150 AND partitionZIndex<=150) AND imageId='31.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=170 AND partitionZIndex<=170) AND imageId='87.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=173 AND partitionZIndex<=173) AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=221 AND partitionZIndex<=221) AND imageId='89.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=241 AND partitionZIndex<=241) AND imageId='86.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=182 AND partitionZIndex<=182) AND imageId='13.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=108 AND partitionZIndex<=108) AND imageId='54.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=6 AND partitionZIndex<=6) AND imageId='73.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=164 AND partitionZIndex<=164) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=189 AND partitionZIndex<=189) AND imageId='75.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=116 AND partitionZIndex<=116) AND imageId='20.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=205 AND partitionZIndex<=205) AND imageId='40.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=37 AND partitionZIndex<=37) AND imageId='2.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=235 AND partitionZIndex<=235) AND imageId='58.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=136 AND partitionZIndex<=136) AND imageId='45.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=200 AND partitionZIndex<=200) AND imageId='22.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=42 AND partitionZIndex<=42) AND imageId='19.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=146 AND partitionZIndex<=146) AND imageId='66.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=95 AND partitionZIndex<=95) AND imageId='18.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=79 AND partitionZIndex<=79) AND imageId='79.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE (partitionZIndex>=92 AND partitionZIndex<=92) AND imageId='28.svs' AND imageLevel=0"
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

