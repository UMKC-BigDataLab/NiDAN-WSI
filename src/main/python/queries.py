import sys

scriptFuntionImport = """ 
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

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  
"""

run = """

show_timing{sqlContext.read.orc(s"hdfs://ctl:9000$dataSource").createOrReplaceTempView("data")}
show_timing{sqlContext.sql(queries(0)._1).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}

"""


inputFile = sys.argv[1]
outputFile = sys.argv[2]

inFile = open(inputFile, 'r')
lines = inFile.readlines()
output = open(outputFile, 'w')
counter = 0

output.write(scriptFuntionImport)
for line in lines:
	counter = counter + 1

	if(counter % 3 == 0):
		output.write(run)
	else:
		output.write(line)
	
output.close()
