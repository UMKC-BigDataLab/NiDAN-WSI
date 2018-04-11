package nidan.tiles

@SerialVersionUID(100L)
class TileDimension (var width:Long, var height:Long) extends Serializable{
  
  def this(_w:Int, _h:Int) = this(_w.toLong, _h.toLong)
  def this() = this(0L, 0L)
   
}