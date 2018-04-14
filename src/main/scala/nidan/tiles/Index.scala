package nidan.tiles

/**
 * @author debarron
 * 
 * This class is used to establish an ordering among
 * the image tiles.
 * 
 * @param row Row number based 0
 * @param col Column number based 0
 * @param z Tile's Z-Order index
 * @param seq Tile's sequential index 
 * @param c Tile's content index
 */
class Index(row:Int, col:Int, seq:Int, z:Int, c:Int) extends Serializable{
  
  /**
   * Makes an array out of the tumple, then concats everything in a string
   * whos elements are separated with the '_' char
   */
  override def toString():String = Seq(row, col, seq, z, c).mkString("_")
}
