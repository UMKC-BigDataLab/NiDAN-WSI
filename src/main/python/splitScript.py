import sys
import os
from os.path import join, getsize
from subprocess import call

def printHelp():
  print "USAGE: python splitScript.py <BASE_DIR> <IMAGE_NAME>"

# Looking into the outputDir items
def lookInOutputDirs(baseDir):
  for index in range(0,5):
    print ">> Directory {0}".format(outputDir[index])
    renameFiles(baseDir + outputDir[index])

# Renaming the files
def renameFiles(dir):
  splitID = 0
  for file in os.listdir(dir):
    if "x" in file:
      print "    Rename {0} to {1}".format(file, str(splitID) + "_.png")
      newName = dir + "/" + str(splitID) + "_.png"
      os.rename(dir + "/" + file, newName)

      splitID = splitID + 1



def splitImage(fileSize, baseDir, imageName):
  splitCommand = "split -a 5 -b {0} {1}"
  moveCommand = "mv ./x* {0}"

  # Split and move
  for index in range(0,5):
    blockSize = fileSize / splitsArray[index]

    print ">> Spliting the file"  
    command = splitCommand.format(blockSize, imageName)
    os.system(command)
    print " "

    print ">> Moving the splits "
    command = moveCommand.format(baseDir + outputDir[index])
    os.system(command)
    print " "



if __name__ == "__main__":
  splitsArray = [4, 16, 64, 256, 1024]
  outputDir = ["2x2", "4x4", "8x8", "16x16", "32x32"]

  if len(sys.argv) != 3:
    printHelp()
    sys.exit(1)

  baseDir = sys.argv[1]
  imageName = baseDir + sys.argv[2]

  print ">> Entering the directory {0}".format(baseDir)
  command = "cd {0}".format(baseDir)
  os.system(command)

  print ">> Looking for {0}".format(imageName)
  fileSize = os.path.getsize(imageName)

  splitImage(fileSize, baseDir, imageName)
  lookInOutputDirs(baseDir)





