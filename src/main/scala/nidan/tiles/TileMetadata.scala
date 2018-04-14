package nidan.tiles

import java.io.File
import java.awt.{Dimension, Point}
import nidan.tiles.{TileDimension, TilePoint}
import org.apache.commons.imaging.ImageFormats




/**
 * @author debarron
 * 
 * This class only gets tile's metadata,
 * the actual image loading is performed 
 * elsewhere.
 * 
 * @param file Tile's file name
 * @param source The SVS image's name used as source
 * @param level Tile's level
 * @param position Tile's X and Y coordinate
 * @param dimension Tile's width and height
 * @param index Tile's sorting mechanism
 * @param rowsCols The total number of rows and cols used to split this level
 * 
 * TODO: Implement different image formats
 */
class TileMetadata (
    val file:String,
    val source:String,
    val level:Int, 
    val position:TilePoint, 
    val dimension:TileDimension,
    val index:Index,
    val rowsCols:(Int, Int)
)extends Serializable{  

  /**
   * Add all the tile information in a single string
   * It can also provide the tile filename, one must
   * provide/control the prefix
   * 
   * TODO Deal with different image formats
   */
  val imgFormat = ImageFormats.JPEG
  override def toString():String ={
    val builder = StringBuilder.newBuilder.append((
        source, level, 
        position.x, 
        position.y,
        dimension.width, 
        dimension.height, 
        index, rowsCols._1, rowsCols._2
      ).productIterator.toList.mkString("_") + "." + imgFormat.toString
    )  
    return builder.toString()
  }
  
}

object TileMetadata{
  
  /**
   * This code assumes that a tile exists and is passed as a parameter
   * 
   * @param file Tile's input file
   */
  def fromFile(file:String):TileMetadata = {
    val attrs = new File(file).getName.split("_")
    
    val _source = attrs(0)
    val _level = attrs(1).toInt
    val _x = attrs(2).toInt
    val _y = attrs(3).toInt
    val _width = attrs(4).toInt
    val _height = attrs(5).toInt
    val _row = attrs(6).toInt
    val _col = attrs(7).toInt
    val _seq = attrs(8).toInt
    val _z = attrs(9).toInt
    val _c = attrs(10).toInt
    val _rows = attrs(11).toInt
    val _cols = attrs(12).toInt
    
    return new TileMetadata(file, _source, _level, new TilePoint(_x, _y), new TileDimension(_width, _height), 
        new Index(_row, _col, _seq, _z, _c), (_rows, _cols)) 
  }
  
}


