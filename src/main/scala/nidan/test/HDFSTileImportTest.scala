package nidan.test

import nidan.io.NidanContext

object HDFSTileImportTest {
  def main(args: Array[String]): Unit = {
    val inputDir = args(0)
    val outputFile = args(1)
    
    val sc = NidanContext.sparkContext
    val sql = NidanContext.sqlContext
    
    val metadata = sc.textFile(s"$inputDir/metadata.txt").filter(!_.contains("#"))
    
    
  }
}