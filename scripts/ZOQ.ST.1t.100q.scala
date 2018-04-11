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


val db = "hdfs://ctl:9000/nidan/orc/KUDB.Z10.orc/"
val queries = Seq(
(s"$db/imageId=1.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 32 AND partitionZIndex <= 32)"),
(s"$db/imageId=2.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 59 AND partitionZIndex <= 59)"),
(s"$db/imageId=3.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 20 AND partitionZIndex <= 20)"),
(s"$db/imageId=4.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 57 AND partitionZIndex <= 57)"),
(s"$db/imageId=5.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 42 AND partitionZIndex <= 42)"),
(s"$db/imageId=6.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 23 AND partitionZIndex <= 23)"),
(s"$db/imageId=7.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 3 AND partitionZIndex <= 3)"),
(s"$db/imageId=8.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 35 AND partitionZIndex <= 35)"),
(s"$db/imageId=9.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 14 AND partitionZIndex <= 14)"),
(s"$db/imageId=10.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 17 AND partitionZIndex <= 17)"),
(s"$db/imageId=11.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 51 AND partitionZIndex <= 51)"),
(s"$db/imageId=12.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 26 AND partitionZIndex <= 26)"),
(s"$db/imageId=13.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 63 AND partitionZIndex <= 63)"),
(s"$db/imageId=14.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 43 AND partitionZIndex <= 43)"),
(s"$db/imageId=15.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 25 AND partitionZIndex <= 25)"),
(s"$db/imageId=16.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 62 AND partitionZIndex <= 62)"),
(s"$db/imageId=17.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 1 AND partitionZIndex <= 1)"),
(s"$db/imageId=18.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 50 AND partitionZIndex <= 50)"),
(s"$db/imageId=19.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 43 AND partitionZIndex <= 43)"),
(s"$db/imageId=20.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 55 AND partitionZIndex <= 55)"),
(s"$db/imageId=1.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 18 AND partitionZIndex <= 18)"),
(s"$db/imageId=2.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 32 AND partitionZIndex <= 32)"),
(s"$db/imageId=3.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 44 AND partitionZIndex <= 44)"),
(s"$db/imageId=4.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 36 AND partitionZIndex <= 36)"),
(s"$db/imageId=5.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 49 AND partitionZIndex <= 49)"),
(s"$db/imageId=6.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 15 AND partitionZIndex <= 15)"),
(s"$db/imageId=7.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 12 AND partitionZIndex <= 12)"),
(s"$db/imageId=8.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 45 AND partitionZIndex <= 45)"),
(s"$db/imageId=9.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 10 AND partitionZIndex <= 10)"),
(s"$db/imageId=10.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 26 AND partitionZIndex <= 26)"),
(s"$db/imageId=11.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 44 AND partitionZIndex <= 44)"),
(s"$db/imageId=12.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 12 AND partitionZIndex <= 12)"),
(s"$db/imageId=13.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 14 AND partitionZIndex <= 14)"),
(s"$db/imageId=14.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 15 AND partitionZIndex <= 15)"),
(s"$db/imageId=15.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 50 AND partitionZIndex <= 50)"),
(s"$db/imageId=16.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 40 AND partitionZIndex <= 40)"),
(s"$db/imageId=17.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 19 AND partitionZIndex <= 19)"),
(s"$db/imageId=18.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 40 AND partitionZIndex <= 40)"),
(s"$db/imageId=19.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 31 AND partitionZIndex <= 31)"),
(s"$db/imageId=20.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 47 AND partitionZIndex <= 47)"),
(s"$db/imageId=1.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 17 AND partitionZIndex <= 17)"),
(s"$db/imageId=2.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 3 AND partitionZIndex <= 3)"),
(s"$db/imageId=3.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 57 AND partitionZIndex <= 57)"),
(s"$db/imageId=4.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 12 AND partitionZIndex <= 12)"),
(s"$db/imageId=5.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 38 AND partitionZIndex <= 38)"),
(s"$db/imageId=6.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 14 AND partitionZIndex <= 14)"),
(s"$db/imageId=7.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 21 AND partitionZIndex <= 21)"),
(s"$db/imageId=8.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 49 AND partitionZIndex <= 49)"),
(s"$db/imageId=9.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 45 AND partitionZIndex <= 45)"),
(s"$db/imageId=10.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 13 AND partitionZIndex <= 13)"),
(s"$db/imageId=11.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 19 AND partitionZIndex <= 19)"),
(s"$db/imageId=12.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 5 AND partitionZIndex <= 5)"),
(s"$db/imageId=13.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 42 AND partitionZIndex <= 42)"),
(s"$db/imageId=14.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 36 AND partitionZIndex <= 36)"),
(s"$db/imageId=15.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 1 AND partitionZIndex <= 1)"),
(s"$db/imageId=16.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 58 AND partitionZIndex <= 58)"),
(s"$db/imageId=17.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 30 AND partitionZIndex <= 30)"),
(s"$db/imageId=18.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 19 AND partitionZIndex <= 19)"),
(s"$db/imageId=19.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 16 AND partitionZIndex <= 16)"),
(s"$db/imageId=20.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 14 AND partitionZIndex <= 14)"),
(s"$db/imageId=1.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 57 AND partitionZIndex <= 57)"),
(s"$db/imageId=2.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 9 AND partitionZIndex <= 9)"),
(s"$db/imageId=3.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 32 AND partitionZIndex <= 32)"),
(s"$db/imageId=4.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 38 AND partitionZIndex <= 38)"),
(s"$db/imageId=5.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 18 AND partitionZIndex <= 18)"),
(s"$db/imageId=6.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 18 AND partitionZIndex <= 18)"),
(s"$db/imageId=7.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 44 AND partitionZIndex <= 44)"),
(s"$db/imageId=8.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 19 AND partitionZIndex <= 19)"),
(s"$db/imageId=9.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 60 AND partitionZIndex <= 60)"),
(s"$db/imageId=10.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 13 AND partitionZIndex <= 13)"),
(s"$db/imageId=11.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 56 AND partitionZIndex <= 56)"),
(s"$db/imageId=12.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 53 AND partitionZIndex <= 53)"),
(s"$db/imageId=13.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 20 AND partitionZIndex <= 20)"),
(s"$db/imageId=14.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 51 AND partitionZIndex <= 51)"),
(s"$db/imageId=15.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 10 AND partitionZIndex <= 10)"),
(s"$db/imageId=16.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 35 AND partitionZIndex <= 35)"),
(s"$db/imageId=17.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 14 AND partitionZIndex <= 14)"),
(s"$db/imageId=18.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 32 AND partitionZIndex <= 32)"),
(s"$db/imageId=19.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 36 AND partitionZIndex <= 36)"),
(s"$db/imageId=20.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 7 AND partitionZIndex <= 7)"),
(s"$db/imageId=1.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 11 AND partitionZIndex <= 11)"),
(s"$db/imageId=2.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 47 AND partitionZIndex <= 47)"),
(s"$db/imageId=3.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 23 AND partitionZIndex <= 23)"),
(s"$db/imageId=4.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 31 AND partitionZIndex <= 31)"),
(s"$db/imageId=5.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 21 AND partitionZIndex <= 21)"),
(s"$db/imageId=6.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 34 AND partitionZIndex <= 34)"),
(s"$db/imageId=7.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 57 AND partitionZIndex <= 57)"),
(s"$db/imageId=8.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 20 AND partitionZIndex <= 20)"),
(s"$db/imageId=9.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 62 AND partitionZIndex <= 62)"),
(s"$db/imageId=10.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 38 AND partitionZIndex <= 38)"),
(s"$db/imageId=11.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 12 AND partitionZIndex <= 12)"),
(s"$db/imageId=12.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 40 AND partitionZIndex <= 40)"),
(s"$db/imageId=13.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 43 AND partitionZIndex <= 43)"),
(s"$db/imageId=14.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 27 AND partitionZIndex <= 27)"),
(s"$db/imageId=15.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 63 AND partitionZIndex <= 63)"),
(s"$db/imageId=16.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 51 AND partitionZIndex <= 51)"),
(s"$db/imageId=17.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 2 AND partitionZIndex <= 2)"),
(s"$db/imageId=18.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 51 AND partitionZIndex <= 51)"),
(s"$db/imageId=19.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 13 AND partitionZIndex <= 13)"),
(s"$db/imageId=20.svs/imageLevel=0","SELECT imageBytes FROM data WHERE imageLevel=0 AND (partitionZIndex >= 61 AND partitionZIndex <= 61)")
)


for (query <- queries){
show_timing{spark.read.orc(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}

