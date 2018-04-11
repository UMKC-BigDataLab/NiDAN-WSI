
JAVA_COMP=javac
SCALA_COMP=scalac
LINK=jar
JAR_NAME=nidand-project-1.0.jar

# TODO: Need to find a way to crack this 
CP_ImageSplits=ImageSplit.jar
CP_Scala=ScalaCode.jar
CP=../libs/compress-lzf-1.0.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.0.jar:/usr/local/spark/lib/spark-assembly-1.3.1-hadoop2.6.0.jar:/usr/local/scala/lib/


remove_class:
	mv *.class ./out

remove_unnecesary_jars:
	mv ${CP_ImageSplits} ./out
	mv ${CP_Scala} ./out


ImageSplits: ImageSplits.java
	@echo ">> Compiling ImageSplits"
	$(JAVA_COMP) -cp ${CP} ImageSplits.java 
	@echo " "
	@echo ">> Generating JAR file"
	jar cf ${CP_ImageSplits} *.class


ScalaComp: ImageSplits
	@echo ">> Compiling Scala code"
	$(SCALA_COMP) -classpath ${CP}:${CP_ImageSplits} *.scala 
	@echo " "
	@echo ">> Generating a JAR file"
	jar cf ${CP_Scala} *.class 

JavaComp: ScalaComp ImageSplits
	@echo ">> Compiling the rest of the Java classes"
	$(JAVA_COMP) -cp ${CP}:${CP_Scala} *.java 
	@echo " "
	@echo ">> Generating JAR file"	
	jar cf ${JAR_NAME} *.class

all: JavaComp ScalaComp ImageSplits remove_class remove_unnecesary_jars 
	@echo ">> Compiling done"

