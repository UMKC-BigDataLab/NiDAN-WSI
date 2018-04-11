package nidan.tiles

@SerialVersionUID(100L)
class TilePoint(var x:Long, var y:Long) extends Serializable{
  def this() = this(0L, 0L)
  def this(_x:Int, _y:Int) = this(_x.toLong, _y.toLong)
}