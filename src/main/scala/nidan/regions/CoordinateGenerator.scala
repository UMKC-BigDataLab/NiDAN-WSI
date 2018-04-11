package nidan.regions

import nidan.tiles.{TileDimension, TilePoint}
import scala.collection.mutable.ArrayBuffer


object CoordinateGenerator {
  /**
   * Generates an n x n list of coordinates from a given canvas dimension
   * @param dimension The canvas dimension
   * @param numberOfTiles A base 4 number for the total tiles to be extracted
   */
  def squareMatrixfromDimension(dimension:TileDimension, numberOfTiles:Int):
  List[(Int,Int,Int,TilePoint,TileDimension)]={
    val w = dimension.width.toLong
    val h = dimension.height.toLong
    
    val n = Math.sqrt(numberOfTiles).toInt
    val wGap = w / n
    val hGap = h / n
    
    /* This for loop generates all posible combinations
     * for the pair row and col, then it calculates the
     * boundaries where Point will be the upper left corner
     * and Dimension will have the tile's offsets
     * PS Scala is awesome!
     */
    val coords = for{
      r <- 0 to n -1
      c <- 0 to n -1
      
      index = (r * n) + c
      wOffset = (c * wGap).toInt
      hOffset = (r * hGap).toInt
      wTile = (if ( c == n-1) w - wOffset else wGap).toInt
      hTile = (if ( r == n-1) h - hOffset else wGap).toInt
      
    }yield (index, r, c, 
        new TilePoint(wOffset, hOffset), 
        new TileDimension(wTile, hTile)
    )
    
    return coords.toList 
  }
  
}