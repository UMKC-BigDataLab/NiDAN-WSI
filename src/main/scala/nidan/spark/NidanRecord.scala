package nidan.spark

class NidanRecord (
    val fileId:String,
    val level:Int, 
    val x:Long,
    val y:Long,
    val tileWidth:Long,
    val tileHeight:Long,
    val row:Int, 
    val col:Int, 
    val seqIndex:Int, 
    val zIndex:Int, 
    val cIndex:Int,
    val totalRows:Int,
    val totalCols:Int,
    val bytes:Array[Byte]
    ) extends Serializable