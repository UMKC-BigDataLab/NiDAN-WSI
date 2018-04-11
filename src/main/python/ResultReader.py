# ResultReader.py

import sys
import re
import glob, os


def getElapsedTime(timeStr):
	return timeStr.replace(" ", "").replace(">>Elapsedtime:", "").replace("ms", "")

def getPartitionType(partitionStr):
	return "Y" if partitionStr.find("Y") != -1 else "N"

def getExperiment(experimentStr):
	return experimentStr if experimentStr != "-" else "0"

def getExecution(executionStr):
	return "W" if executionStr.find("W") != -1 else "C"

def getFileSize(experimentStr):
	return re.search('\d+', experimentStr).group()

def getText(strDirectory):
	lines = []

	for file in glob.glob(strDirectory + "/*.txt"):
		descriptor = open(str(file), 'r')
		content = descriptor.read()
		lines = lines + content.splitlines()

	return lines

def printTable(tableResult, fileSize):
	row = fileSizes[fileSize] + " | " + "  ".join([str(a) for a in tableResults[fileSize]])
	print(row)


def printTimes(resultsDict):
	for partI in partitionType:
		part = resultsDict[partI]

		print("Table for " + partI + " partition type")
		for i in range(0, len(fileSizes)):
			printTable(part, i)
		print("\n")



myDir = sys.argv[1]
experiment = sys.argv[2]
print(">>>>>>>>>>>>>>>>>>>>>>")
print(">> Directory: " + myDir )
print(">>>>>>>>>>>>>>>>>>>>>>\n")

results = []
partitionType = ["Y", "N"]
fileSizes = ["256", "1024", "4096", "16384"]
splitSizes = ["2x2", "4x4", "8x8", "16x16", "32x32"]

# Get the data into one list
data = getText(myDir)

# Pre-process the data and extract only
# the variables that you need
startLine = 0
numberLines = len(data)
while (startLine < numberLines):
	
	dataExp = data[startLine + 1].split("/")

	partition = getPartitionType(data[startLine + 2])
	elapsedTime = getElapsedTime(data[startLine + 3])
	experimentNumber = getExperiment(dataExp[-1][0])
	execution = getExecution(dataExp[-1])
	splits = dataExp[-2]
	fileSize = getFileSize(dataExp[2])

	results.append((partition, elapsedTime, experimentNumber, execution, splits, fileSize))

	startLine = startLine + 4


# Get the work done
finalTimes = {}
for partition in "Y", "N":
	tuples = filter(lambda reg: reg[0] == partition and reg[3] == experiment, results)
	
	tableResults = []
	for fileSize in "256", "1024", "4096", "16384":

		tableRow = []
		for split in "2x2", "4x4", "8x8", "16x16", "32x32":
			experiments = filter(lambda reg: 
				reg[4] == split and 
				reg[5] == fileSize, 
				tuples)

			times = [float (e[1]) for e in experiments]
			size = len(times)

			if size > 0:
				avg = sum(times) / len(times)
			else:
				avg = float('nan')
				
			tableRow.append(avg)

		tableResults.append(tableRow)

	finalTimes[partition] = tableResults

printTimes(finalTimes)


			# print(times)
			# print("The average is: " + str(avg))
			# exit(1)

		# t = list(map(float, part2x2[0][1]))

		# print(float(part2x2[0][1]))
		

		# t = sum(list(float(part2x2[0][1])))

		# print(part2x2[0][0])

		# print(numpy.mean(part2x2[]))


		# part4x4 = filter(lambda reg: reg[4] == "4x4", tuples)
		# part8x8 = filter(lambda reg: reg[4] == "8x8", tuples)
		# part16x16 = filter(lambda reg: reg[4] == "16x16", tuples)
		# part32x32 = filter(lambda reg: reg[4] == "32x32", tuples)



# 		times = filter(lambda t: t[4] == splits, tuples)
# 		partitionResult = partitionResult + times

# 	finalTimes[partition] = partitionResult


# for rtime in finalTimes["Y"]:
# 	print(rtime)
# print("##")




















