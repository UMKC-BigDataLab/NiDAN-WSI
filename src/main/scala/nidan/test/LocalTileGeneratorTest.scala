package nidan.test

import nidan.preprocessing.LocalTileGenerator
import nidan.utils.NidanUtils

object LocalTileGeneratorTest {
  def main(args: Array[String]): Unit = {
    
    // Passed the test
    //testGenerateLastTile(localGenerator)
    
    testGenerateTilesFromArgs(args)
  }
  
  def testGeneratorTilesHelp() = {
    println(">> NIDAN LOCAL TILE GENERATOR ")
    println(">> ARGS: INPUT_FILE OUTPUT_DIR EXPONENT LEVEL")
    println(">> EXAMPLE: inputImage.svs /outputDir 2 1")
  }
  
  def testGenerateTilesFromArgs(args:Array[String]) {
    if(args(0) == "-h" ){
      testGeneratorTilesHelp()
      System.exit(1)
    }
      
      
    val file = args(0)
    val outputDir = args(1)
    val tiles = args(2).toInt
    val level = args(3).toInt
    
    val time = NidanUtils.timeIt{
    val localGenerator = new LocalTileGenerator(file, outputDir)
      
    println(">> NIDAN TEST")
    println(">> GENERATING TILES FROM ARGUMENTS")
    println("----------------------------------")
    println(s">> Data input is: $file")
    println(s">> Data output is: $outputDir")
    println(s">> Generating $tiles tiles for level $level")
    
    localGenerator.generateTiles(level, tiles)
    }._2
    
    println(">> Process ended")
    println(s">> TIME: ${time} secs")
  }
  
  def testGenerateLastTile(generator:LocalTileGenerator) ={
    println(">> TEST: GenerateLastTile")
    println(s"Total of levels => ${generator.imageLevels}")
    println(s"Extracting image from level => ${generator.imageLevels-1}")
    
    generator.generateTiles(generator.imageLevels-1, 0)
  }
  
}
