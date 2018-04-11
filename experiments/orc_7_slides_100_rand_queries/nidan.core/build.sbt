organization  := "umkc"

name := "nidan.core"

version       := "0.1"

scalaVersion  := "2.11.7"

fork in run := true

resolvers ++= Seq(
  "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
  "jai imageio" 	  at "https://mvnrepository.com/artifact/com.github.jai-imageio/jai-imageio-core",
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/"
)

libraryDependencies ++= {
  val sparkV =  "2.1.0"
  val hadoopV = "2.7.3"
  Seq(
  	"com.ning" 			  % 	 "compress-lzf" 	 % "1.0.3",
  	"joda-time"			  %	 "joda-time" 	 % "2.9.4",
  	"org.apache.hadoop"   %   "hadoop-common" % hadoopV,
    "org.apache.spark"    %%  "spark-core"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-sql"	  %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-hive"   %  sparkV % "provided",
    "org.apache.spark"    %%  "spark-streaming"   %  sparkV % "provided",
    "org.datanucleus" % "datanucleus-api-jdo" % "3.2.6" % "provided",
  	"org.datanucleus" % "datanucleus-rdbms" % "3.2.9" % "provided",
  	"org.datanucleus" % "datanucleus-core" % "3.2.10" % "provided",
  	"commons-io" % "commons-io" % "2.4",
  	"org.apache.commons" % "commons-imaging" % "1.0-SNAPSHOT"  	
  )
}

// Unmanage dependecies
unmanagedJars in Compile ++= Seq(file("custom_lib/openslide.jar"))

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}








