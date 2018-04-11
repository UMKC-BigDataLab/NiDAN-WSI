#!/usr/bin/python
from os import system as oscall

hadoopDir = "/usr/local/hadoop/"
nameNodeRF = "rm -Rf {}/logs"
dataNodeRF = "ssh {}{} rm -Rf {}/logs"

# TODO Change it so it can read the slaves file
nodePrefix = "cp-"
startNode = 1
endNode = 4

# This will be executed in the master
removeLogs = []
removeLogs.append(nameNodeRF.format(hadoopDir))

# Remove logs from the cluster
for i in range(startNode, endNode):
    command = dataNodeRF.format(nodePrefix, i, hadoopDir)
    removeLogs.append(command)

# Run all the commands
for op in removeLogs:
    oscall(op)
    