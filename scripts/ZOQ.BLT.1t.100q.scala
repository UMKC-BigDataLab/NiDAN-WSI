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
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=222 AND partitionZIndex<=222",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=98 AND partitionZIndex<=98",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=147 AND partitionZIndex<=147",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=12 AND partitionZIndex<=12",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=210 AND partitionZIndex<=210",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=134 AND partitionZIndex<=134",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=123 AND partitionZIndex<=123",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=116 AND partitionZIndex<=116",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=65 AND partitionZIndex<=65",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=38 AND partitionZIndex<=38",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=79 AND partitionZIndex<=79",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=59 AND partitionZIndex<=59",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=168 AND partitionZIndex<=168",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=237 AND partitionZIndex<=237",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=200 AND partitionZIndex<=200",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=94 AND partitionZIndex<=94",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=234 AND partitionZIndex<=234",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=244 AND partitionZIndex<=244",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=111 AND partitionZIndex<=111",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=4 AND partitionZIndex<=4",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=250 AND partitionZIndex<=250",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=199 AND partitionZIndex<=199",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=248 AND partitionZIndex<=248",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=142 AND partitionZIndex<=142",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=194 AND partitionZIndex<=194",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=189 AND partitionZIndex<=189",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=213 AND partitionZIndex<=213",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=224 AND partitionZIndex<=224",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=173 AND partitionZIndex<=173",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=49 AND partitionZIndex<=49",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=226 AND partitionZIndex<=226",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=8 AND partitionZIndex<=8",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=227 AND partitionZIndex<=227",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=83 AND partitionZIndex<=83",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=152 AND partitionZIndex<=152",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=138 AND partitionZIndex<=138",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=128 AND partitionZIndex<=128",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=29 AND partitionZIndex<=29",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=172 AND partitionZIndex<=172",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=53 AND partitionZIndex<=53",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=47 AND partitionZIndex<=47",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=163 AND partitionZIndex<=163",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=78 AND partitionZIndex<=78",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=21 AND partitionZIndex<=21",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=131 AND partitionZIndex<=131",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=216 AND partitionZIndex<=216",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=103 AND partitionZIndex<=103",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=19 AND partitionZIndex<=19",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=198 AND partitionZIndex<=198",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=105 AND partitionZIndex<=105",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=252 AND partitionZIndex<=252",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=64 AND partitionZIndex<=64",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=220 AND partitionZIndex<=220",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=170 AND partitionZIndex<=170",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=221 AND partitionZIndex<=221",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=7 AND partitionZIndex<=7",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=204 AND partitionZIndex<=204",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=73 AND partitionZIndex<=73",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=247 AND partitionZIndex<=247",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=28 AND partitionZIndex<=28",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=212 AND partitionZIndex<=212",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=166 AND partitionZIndex<=166",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=235 AND partitionZIndex<=235",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=129 AND partitionZIndex<=129",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=42 AND partitionZIndex<=42",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=209 AND partitionZIndex<=209",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=135 AND partitionZIndex<=135",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=182 AND partitionZIndex<=182",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=39 AND partitionZIndex<=39",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=108 AND partitionZIndex<=108",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionZIndex>=240 AND partitionZIndex<=240",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=141 AND partitionZIndex<=141",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=33 AND partitionZIndex<=33",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=52 AND partitionZIndex<=52",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=153 AND partitionZIndex<=153",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=201 AND partitionZIndex<=201",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionZIndex>=112 AND partitionZIndex<=112",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionZIndex>=57 AND partitionZIndex<=57",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=30 AND partitionZIndex<=30",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=251 AND partitionZIndex<=251",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=230 AND partitionZIndex<=230",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=203 AND partitionZIndex<=203",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=60 AND partitionZIndex<=60",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=239 AND partitionZIndex<=239",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=143 AND partitionZIndex<=143",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=76 AND partitionZIndex<=76",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionZIndex>=86 AND partitionZIndex<=86",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=114 AND partitionZIndex<=114",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=178 AND partitionZIndex<=178",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=48 AND partitionZIndex<=48",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=44 AND partitionZIndex<=44",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionZIndex>=63 AND partitionZIndex<=63",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionZIndex>=15 AND partitionZIndex<=15",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=146 AND partitionZIndex<=146",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionZIndex>=236 AND partitionZIndex<=236",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=80 AND partitionZIndex<=80",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionZIndex>=238 AND partitionZIndex<=238",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=193 AND partitionZIndex<=193",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionZIndex>=5 AND partitionZIndex<=5",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionZIndex>=113 AND partitionZIndex<=113"
)

show_timing{spark.read.option("mergeSchema", "true").orc("hdfs://ctl:9000/nidan/orc/KUDB.Z10.orc").createOrReplaceTempView("data")}

show_timing{spark.sql(queries(0)).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query}")
show_timing{spark.sql(query).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
