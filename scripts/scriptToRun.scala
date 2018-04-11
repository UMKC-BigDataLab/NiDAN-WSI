
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
"SELECT imageBytes FROM data WHERE  partitionIndex = 63 AND imageId='1.svs'"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 143 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 107 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 94 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 6 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 157 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 71 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 174 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 15 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 210 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 226 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 27 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 79 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 30 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 103 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 90 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 135 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 178 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 86 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 190 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 236 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 185 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 155 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 170 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 106 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 62 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 171 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 128 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 160 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 146 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 92 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 31 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 99 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 93 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 50 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 205 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 186 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 239 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 33 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 238 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 75 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 59 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 140 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 243 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 191 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 36 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 149 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 195 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 142 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 209 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 200 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 202 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 72 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 123 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 132 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 83 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 52 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 120 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 0 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 82 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 168 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 218 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 147 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 53 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 252 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 111 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 40 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 251 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 45 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 219 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 247 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 152 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 110 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 18 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 1 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 2 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 13 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 51 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 246 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 112 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 98 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 137 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 43 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 113 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 35 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 68 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 44 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 9 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 22 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 229 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 196 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 39 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 11 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 182 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 241 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 167 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 176 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 240 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 131 AND imageId='1.svs' AND imageLevel=0"
,
"SELECT imageBytes FROM data WHERE  partitionIndex = 122 AND imageId='1.svs' AND imageLevel=0"
)



	show_timing{spark.read.orc("hdfs://ctl:9000//nidan/orc/table.orc").createOrReplaceTempView("data")}

	show_timing{
		spark.sql(queries(0))
		.map(_.getAs[Array[Byte]](0))
		.rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}
		.collect.map(writeToLocal)
		.filter(_ => false).size}

	for (query <- queries){
		println(s">> Running query: ${query}")
		show_timing{
			spark.sql(query)
			.map(_.getAs[Array[Byte]](0))
			.rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}
			.collect.map(writeToLocal)
			.filter(_ => false).size
		}
	}
	
