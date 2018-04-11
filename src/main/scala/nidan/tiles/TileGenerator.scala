package nidan.tiles

import scala.collection.mutable.ArrayBuffer

/***
 * @param inputFile The SVS input image
 * @param outputDir Where all tiles will be stored
 */
class TileGenerator (inputFile:String, outputDir:String){
  /***
   * 
   */
  def writeTiles(level:Integer, rows:Integer, cols:Integer):Boolean = {
    return false
  }
  
  def metaFromLevel(level:Integer, rows:Integer, cols:Integer):Array[TileMetadata] = {
    return null
  }
  
}