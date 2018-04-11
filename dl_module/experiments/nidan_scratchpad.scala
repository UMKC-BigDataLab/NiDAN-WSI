//quick reference commands
hdfs://ctl.nidan.nosql-json-pg0.utah.cloudlab.us:9000/nidan/parquet/Slides-uncompressed-256-ZINDEX.prqt
hdfs dfs -rm -r /nidan/parquet/Slides-uncompressed-256-ZINDEX.prqt
http://128.110.152.45:4040 //spark UI after starting spark-shell
cat 1.scala | ./1.sh
/proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/slide1
python ./nidan.core/run-script.py -scala nidan.main.MainOpenSlide ./nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar "1 /users/dl544/openslideExp/dataset/ /users/dl544/openslideExp/dataset/ 268435456 Logger"
sbt assembly
cat Log_30_1.log | grep 'Time elapsed' | tail -4 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'
./makescript.sh -assembly
//268435456
------------------------------------------------------------------------------------------------------------------------------

to try
 compression
 write to hdfs
 with and without collect
 does caching
 comparison - use openslide to extract tile
 increase executor-memory
 add image name attribute
 try storing in hdfs
 create a large spark sql table
 try to post many queries at once


 --------------------------------------------------------------------------------------------------------------


time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --class nidan.main.MainSparkSQL --master yarn hdfs:///nidan/jars/nidan.core-assembly-0.1.jar -11  ~/slide2  /nidan/parquet/Slide_1_standalone-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP


time spark-submit  --name Nidan  --master local[*]  --driver-memory 36G  --executor-memory 4G  --executor-cores 2  --num-executors 2  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1024  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar  -12 /nidan/parquet/Slide_1-256-ZINDEX.prqt data "SELECT imageBytes from data where partitionIndex > 0 and partitionIndex < 2 and imageLevel = 0" /dev/shm/parquet_output JPEG

time spark-submit  --name Nidan --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1024  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar  -12 /nidan/parquet/Slide_1-256-ZINDEX.prqt data "SELECT imageBytes from data where partitionIndex = 1 and imageLevel = 0" /nidan/parquet/p_0_2 JPEG


env JAVA_OPTS="-Xmx2048M -Xms1024M -Xss1024M"

time spark-submit  --name Nidan  --master local[*]  --driver-memory 46G  --executor-memory 4G  --executor-cores 2  --num-executors 2  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar -11  ~/slide4  /nidan/parquet/Slide_4-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP




 time spark-submit  --name Nidan --master spark://ctl:7077 --driver-memory 36G  --executor-memory 4G  --executor-cores 2  --num-executors 10  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1024  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar  -12 /nidan/parquet/Slide_1-256-ZINDEX.prqt data "SELECT imageBytes from data where partitionIndex = 1 and imageLevel = 0" /nidan/tiles/parquet_output_69 JPEG false

# add parquet compression codec snappy
 time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=snappy --class nidan.main.MainSparkSQL target/scala-2.11/nidan.core-assembly-0.1.jar  -11  ~/slide2  /nidan/parquet/Slide_2_compress_snappy-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP

 # add parquet compression codec gzip
 time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=gzip --class nidan.main.MainSparkSQL target/scala-2.11/nidan.core-assembly-0.1.jar  -11  ~/slide2  /nidan/parquet/Slide_2_compress_gzip-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP

 # add parquet compression codec lzo
 time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=lzo --class nidan.main.MainSparkSQL target/scala-2.11/nidan.core-assembly-0.1.jar  -11  ~/slide2  /nidan/parquet/Slide_2_compress_lzo-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP

# query the tile from gzip compressed parquet
time spark-submit  --name Nidan --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1024  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar  -12 /nidan/parquet/Slide_2_compress_gzip-256-ZINDEX.prqt data "SELECT imageBytes from data where partitionIndex = 1 and imageLevel = 0" /nidan/parquet/p_c_0_1 JPEG

#query a tile from default parquet file and store it in HDFS
 time spark-submit  --name Nidan --master spark://ctl:7077 --driver-memory 36G  --executor-memory 4G  --executor-cores 2  --num-executors 10  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1024  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar  -12 /nidan/parquet/Slide_1-256-ZINDEX.prqt data "SELECT imageBytes from data where partitionIndex = 1 and imageLevel = 0" /dev/shm/p_0_3 JPEG true

 #query a tile from compressed gzip parquet and store it in HDFS
  time spark-submit  --name Nidan --master spark://ctl:7077 --driver-memory 36G  --executor-memory 4G  --executor-cores 2  --num-executors 10  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1024  --conf  spark.sql.parquet.compression.codec=gzip  --class nidan.main.MainSparkSQL  target/scala-2.11/nidan.core-assembly-0.1.jar  -12 /nidan/parquet/Slide_2_compress_gzip-256-ZINDEX.prqt data "SELECT imageBytes from data where partitionIndex = 100 and imageLevel = 0" /nidan/tiles/parquet_output_gzip_0_1 JPEG false

# add parquet compression codec uncompressed
 time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL target/scala-2.11/nidan.core-assembly-0.1.jar  -11  ~/slide2  /nidan/parquet/Slide1-uncompressed-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP


# gpu machine ip address
134.193.128.90  

# spark-shell commands
spark-shell --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}
val pf = show_timing{sqlContext.read.parquet("/nidan/parquet/Slide1-uncompressed-256-ZINDEX.prqt")}
show_timing{pf.createOrReplaceTempView("data")}


val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <=2 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o5.JPEG")}}

val count = show_timing{result.count}
val dataTiles = show_timing{result.collect}

import java.io.File
import java.io.FileOutputStream

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
  }

 show_timing{dataTiles.map(writeToLocal).filter(_ => false).size}


# import org.apache.hadoop.fs.FileSystem
# import java.nio.file.{Path => jPath, Paths => jPaths} 

# val writeToHDFS = (in:(Array[Byte], Configuration, String)) =>{
#   val bytes = in._1 
#   val fs = FileSystem.get(in._2) 
#   val output = in._3
  
# val outpath = new Path(output)
# val file = fs.create(outpath)

# file.write(bytes);
# file.close
# }

# def getHDFSConf():Configuration = {
# val coreFile = "/usr/local/hadoop/etc/hadoop/core-site.xml"
# val hdfsFile = "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"
# val yarnFile = "/usr/local/hadoop/etc/hadoop/yarn-site.xml"

# val corePath = new Path(coreFile)
# val hdfsPath = new Path(hdfsFile)
# val yarnPath = new Path(yarnFile)

# val conf = new Configuration()
# conf.addResource(corePath)
# conf.addResource(hdfsPath)
# conf.addResource(yarnPath)

# return conf
# }

# def getHDFSConfSingleton():Configuration = {
# if(hadoopConf == null){
#   hadoopConf = getHDFSConf()
# }

# return hadoopConf
# }


# val resultHDFS = result.map(i => (i._1, getHDFSConfSingleton, i._3))
# resultHDFS.map(writeToHDFS).filter(_ => false).count

#Django web app
ms0145.utah.cloudlab.us:8000

#spark-shell cluster mode
spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}
val pf = show_timing{sqlContext.read.parquet("/nidan/parquet/Slide1-uncompressed-256-ZINDEX.prqt")}
show_timing{pf.createOrReplaceTempView("data")}

//print schema of parquet file
// val pfs = sqlContext.read.parquet("/nidan/parquet/Slide1-uncompressed-256-ZINDEX.prqt").printSchema()


val result = show_timing{sqlContext.sql("SELECT imageId from data where partitionIndex >= 1 and partitionIndex <=6 and imageLevel = 0")}

val count = show_timing{result.count}
val dataTiles = show_timing{result.collect}

import java.io.File
import java.io.FileOutputStream

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
  }

 show_timing{dataTiles.map(writeToLocal).filter(_ => false).size}


# cache
val var2 = show_timing{sqlContext.read.parquet("/nidan/parquet/Slide1-uncompressed-256-ZINDEX.prqt").cache}

// To do

# append slide 2 tiles to slide 1 tiles
 time spark-submit  --name Nidan  --master  local[*]  --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL target/scala-2.11/nidan.core-assembly-0.1.jar  -11  ~/slide2  /nidan/parquet/Slide1-uncompressed-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP


//spark shell cluster mode 2
spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}
val pf = show_timing{sqlContext.read.parquet("/nidan/parquet/Slides-uncompressed-256-ZINDEX.prqt")}
show_timing{pf.createOrReplaceTempView("data")}

val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <=2 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}}

val count = show_timing{result.count}
val dataTiles = show_timing{result.collect}

import java.io.File
import java.io.FileOutputStream

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
  }

 show_timing{dataTiles.map(writeToLocal).filter(_ => false).size}

 //combining two different queries
 val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}}
 val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 2 and partitionIndex <3 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o7_${index}.JPEG")}}

//add image id to the query
 val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0 and imageId = 'Slide1'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}}

//select *
val result = show_timing{sqlContext.sql("SELECT * from data where partitionIndex >= 1 and partitionIndex <=6 and imageLevel = 0")}
From a parquet file with all the slides, retrieving 2/4/6 tiles from a single slide is faster than retrieving 2/4/6 tiles from different slides.

//zindex
 val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <2 and imageLevel = 0 and imageId = '1.svs'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}}

//zindex with sequential reads
 val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <6 and imageLevel = 0 and imageId = 'Slide1'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}}

//sequential indexing with sequential reads
 val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0 and imageId = '1.svs'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}}


//create single parquet file with all slides - first step for slide 1
time spark-submit  --name Nidan  --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL target/scala-2.11/nidan.core-assembly-0.1.jar  -11  ~/slide1  /nidan/parquet/Slides-uncompressed-256-ZINDEX.prqt JPEG 1 ZINDEX GROUP

//number of tiles 20
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0 and imageId = '1.svs'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <20 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <30 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <40 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <50 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <90 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}
val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <200 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o8_${index}.JPEG and imageId = 'Slide1'")}}

------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
//Running experiment for retrieving tiles from a parquet file with 1 slide on 4/19/2017 14:43
// val count = show_timing{result.count}
time spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=2g

def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}

import java.io.File

import java.io.FileOutputStream

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
  }

val sqlContext = show_timing{new org.apache.spark.sql.SQLContext(sc)}
import org.apache.spark.sql._
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

// show_timing{sqlContext.read.parquet("/nidan/parquet/Slides-uncompressed-256-ZINDEX.prqt").createOrReplaceTempView("data")}
show_timing{sqlContext.read.orc("/nidan/orc/testORC22/imageId=1.svs").createOrReplaceTempView("data")}

// show_timing{sqlContext.sql("SELECT imageBytes from data where partitionZIndex = 1 and imageLevel = 0 and imageId = '1.svs'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
show_timing{sqlContext.sql("SELECT  from data").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0 and imageId = '1.svs'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0 and imageId = '1.svs'").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

// Optimize the performance of spark SQL query
time spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=2g --conf spark.sql.tungsten.enabled=true --conf spark.sql.orc.filterPushdown=true

// ORC format
time spark-submit  --name Nidan  --master local[*] --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=1g  --conf  spark.sql.parquet.compression.codec=uncompressed --class nidan.main.MainSparkSQL ~/orcFormat/nidan.core/target/scala-2.11/nidan.core-assembly-0.1.jar  -11 /proj/nosql-json-PG0/nidan/KU_IMGS/JPEG256/slide1  /nidan/orc/testORC JPEG 1 ZINDEX GROUP

val result = show_timing{sqlContext.sql("SELECT imageBytes from data where partitionIndex >= 1 and partitionIndex <2 and imageLevel = 0").map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

// Shell commands for ORC format
time spark-shell --master spark://ctl:7077 --driver-memory 28G  --executor-memory 28G  --executor-cores 8  --num-executors 16  --conf spark.io.compression.codec=lzf  --conf spark.akka.frameSize=1024  --conf spark.driver.maxResultSize=2g --conf spark.sql.tungsten.enabled=true --conf spark.sql.orc.filterPushdown=true

def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000000000.0 + " seconds")
    res
}

import java.io.File

import java.io.FileOutputStream

import org.apache.spark.sql._

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

val writeToLocal = (in:(Array[Byte], Long, String)) =>{
    val bytes = in._1
    val output = in._3
    
    val writer = new FileOutputStream(output)
    writer.write(bytes)
    writer.close
  }

show_timing{sqlContext.read.orc("/nidan/orc/testORC22/imageId=1.svs").createOrReplaceTempView("data")}

// show_timing{sqlContext.sql("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <=1 and imageLevel = 0").map(x => x.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
show_timing{sqlContext.sql("SELECT imageBytes from data where partitionZIndex >= 1 and partitionZIndex <=1 and imageLevel = 0").map(x => x.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
