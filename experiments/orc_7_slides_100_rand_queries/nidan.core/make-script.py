#!/usr/bin/python
from sys import argv as args
from os import system as oscall

gitCommand = "git {}"
sbtCommand = "sbt {}"

# Create a script to push changes on github
def githubPush(message):
    gitShell = []
    gitShell.append("add")
    gitShell.append("commit")
    gitShell.append("push")
    
    return gitShell

# Create a script to pull changes from github
def gitPull(branch):
    gitShell = []
    gitShell.append("pull from branch")
    
    return gitShell

# Create a script to compile the chages in sbt
def sbtCompileJar():
    sbtShell = []
    sbtShell.append("compile package")
    
    return sbtShell

# Create a script to clean the changes in sbt
def sbtClean():
    sbtShell = []
    sbtShell.append("clean")
    
    return sbtShell 