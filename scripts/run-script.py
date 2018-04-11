#!/usr/bin/python
from sys import argv as args
from os import system as oscall
from timeit import default_timer as timer

def getScalaArgs(theJar, theClass):
	scalaArgs = []
# 	scalaArgs.append("env JAVA_OPTS=\"-Xmx2048M -Xms1024M -Xss1024M\"")
	scalaArgs.append("scala")
	scalaArgs.append("-classpath " + theJar)
	scalaArgs.append(theClass)

	return scalaArgs

def getSparkArgs(theJar, theClass):
	# Arguments to call Spark
	sparkArgs = []
	sparkArgs.append("spark-submit")
	sparkArgs.append("--name Nidan")
# 	sparkArgs.append("--master yarn-cluster")
	sparkArgs.append("--master local[*]")
	sparkArgs.append("--driver-memory 50g")
	sparkArgs.append("--executor-memory 20g")
	sparkArgs.append("--executor-cores 2")
	sparkArgs.append("--num-executors 2")
	sparkArgs.append("--conf spark.io.compression.codec=lz4")
	sparkArgs.append("--conf spark.akka.frameSize=2047")
	sparkArgs.append("--conf spark.driver.maxResultSize=4096")
	
	# Add the user arguments to the list
	sparkArgs.append("--class " + theClass)
	sparkArgs.append(theJar)

	return sparkArgs



option = args[1]

# Arguments for the program
classArg = args[2]
jarArg = args[3]
execArgs = args[4]

# Get the call arguments
if option == "-scala":
	sysArgs = getScalaArgs(jarArg, classArg)
elif option == "-spark":
	sysArgs = getSparkArgs(jarArg, classArg)

for ar in execArgs.replace("\"","").split(" "):
	sysArgs.append(ar)

systemCall = ""
for elemnent in sysArgs:
	systemCall = systemCall + " " + elemnent + " "


print(">>> Nidan")
print("# Command: " + systemCall)
start = timer()

oscall(systemCall)

end = timer()
print("# End")
print("# Elapsed time in seconds: " + str(end - start))