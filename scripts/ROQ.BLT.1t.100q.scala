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
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 175 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 73 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 148 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 49 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 141 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 131 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 124 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 93 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 8 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 67 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 58 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 116 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 211 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 234 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 155 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 47 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 233 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 221 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 122 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 17 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 237 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 154 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 223 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 163 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 137 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 230 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 142 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 191 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 226 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 68 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 201 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 19 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 216 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 28 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 151 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 161 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 127 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 38 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 241 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 70 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 114 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 208 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 43 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 6 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 144 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 159 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 90 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 20 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 139 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 104 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 253 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 119 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 189 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 225 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 174 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 18 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 185 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 40 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 222 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 53 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 157 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 195 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 248 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 128 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 97 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 140 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 146 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 199 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 82 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 121 ",
"SELECT imageBytes FROM data WHERE imageId='2.svs' AND imageLevel=0 AND partitionIndex = 251 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 162 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 64 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 85 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 164 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 168 ",
"SELECT imageBytes FROM data WHERE imageId='9.svs' AND imageLevel=0 AND partitionIndex = 123 ",
"SELECT imageBytes FROM data WHERE imageId='7.svs' AND imageLevel=0 AND partitionIndex = 100 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 39 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 252 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 203 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 184 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 117 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 250 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 178 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 57 ",
"SELECT imageBytes FROM data WHERE imageId='3.svs' AND imageLevel=0 AND partitionIndex = 15 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 77 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 197 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 115 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 113 ",
"SELECT imageBytes FROM data WHERE imageId='5.svs' AND imageLevel=0 AND partitionIndex = 118 ",
"SELECT imageBytes FROM data WHERE imageId='10.svs' AND imageLevel=0 AND partitionIndex = 50 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 133 ",
"SELECT imageBytes FROM data WHERE imageId='6.svs' AND imageLevel=0 AND partitionIndex = 249 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 59 ",
"SELECT imageBytes FROM data WHERE imageId='1.svs' AND imageLevel=0 AND partitionIndex = 235 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 136 ",
"SELECT imageBytes FROM data WHERE imageId='4.svs' AND imageLevel=0 AND partitionIndex = 2 ",
"SELECT imageBytes FROM data WHERE imageId='8.svs' AND imageLevel=0 AND partitionIndex = 76 "
)

show_timing{spark.read.orc("hdfs://ctl:9000/nidan/orc/r10.orc").createOrReplaceTempView("data")}

show_timing{spark.sql(queries(0)).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query}")
show_timing{spark.sql(query).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}