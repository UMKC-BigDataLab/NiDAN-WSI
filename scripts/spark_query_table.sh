#!/bin/bash
# $1 query file
# $2 query style SINGLE_TABLE or BIG_LOGICAL_TABLE
# $3 SparkSQL table
# $4 format PARQUET ORC
# $5 output script 

INPUT=$1
STYLE=$2
TABLE_DIR=$3
FORMAT=$4
SCRIPT=$5

# We are gonna use 'data' as alias
echo '
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

' > $SCRIPT


# Read data from SparkSQL
READ_FORMAT=""
if [ $FORMAT = "ORC" ]
then
	READ_FORMAT="orc"
else
	READ_FORMAT="parquet"
fi


if [ $STYLE = "BIG_LOGICAL_TABLE" ]
then
	echo -e 'val queries = Seq(' >> $SCRIPT

	exec 7< $INPUT
	read query <&7
	SLIDE=$(echo "$query" | cut -d',' -f1)
	SELECT=$(echo "$query" | cut -d',' -f2 | sed -e 's/WHERE /WHERE (/')
	echo -e "\"$SELECT) AND imageId='$SLIDE.svs' AND imageLevel=0\"" >> $SCRIPT

	while read query <&7
	do
		echo -e "," >> $SCRIPT

		SLIDE=$(echo "$query" | cut -d',' -f1)
		SELECT=$(echo "$query" | cut -d',' -f2 | sed -e 's/WHERE /WHERE (/')
		echo -e "\"$SELECT) AND imageId='$SLIDE.svs' AND imageLevel=0\"" >> $SCRIPT
	done
	echo -e ')\n\n' >> $SCRIPT
	
	# Close the file
	exec 7<&-

	# Put the tail to the script
	echo -e '
show_timing{spark.read.'$READ_FORMAT'("hdfs://ctl:9000/'$TABLE_DIR'").createOrReplaceTempView("data")}

show_timing{spark.sql(queries(0))
.map(_.getAs[Array[Byte]](0))
.rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}
.collect.map(writeToLocal)
.filter(_ => false).size}

for (query <- queries){
println(s">> Running query: ${query}")
show_timing{spark.sql(query).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
' >> $SCRIPT

# SINGLE_TABLE
else

	echo -e 'val queries = Seq(' >> $SCRIPT

	exec 7< $INPUT
	read query <&7
	SLIDE=$(echo "$query" | cut -d',' -f1)
	SELECT=$(echo "$query" | cut -d',' -f2)
	echo -e '("hdfs://ctl:9000/'$TABLE_DIR'/imageId='$SLIDE'.svs","'$SELECT'")' >> $SCRIPT

	while read query <&7
	do
		echo -e "," >> $SCRIPT

		SLIDE=$(echo "$query" | cut -d',' -f1)
		SELECT=$(echo "$query" | cut -d',' -f2)
		echo -e '("hdfs://ctl:9000/'$TABLE_DIR'/imageId='$SLIDE'.svs","'$SELECT'")' >> $SCRIPT
	done
	echo -e ')\n\n' >> $SCRIPT
	
	# Close the file
	exec 7<&-

	echo -e '
for (query <- queries){
show_timing{spark.read.'$READ_FORMAT'(query._1).createOrReplaceTempView("data")}
show_timing{spark.sql(query._2).map(_.getAs[Array[Byte]](0)).rdd.zipWithIndex.map{case (bytes, index) => (bytes, index, s"o6_${index}.JPEG")}.collect.map(writeToLocal).filter(_ => false).size}
}
	' >> $SCRIPT 	
fi

echo -e '
sc.stop
' >> $SCRIPT

echo ">> Script done"
