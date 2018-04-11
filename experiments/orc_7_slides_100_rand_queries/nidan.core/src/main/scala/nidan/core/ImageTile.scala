package nidan.core

case class ImageTile(var imageId:String,
  var imageLevel:Int,
  var imagePartitions:Int,
  var partitionIndex:Int,
  var partitionZIndex:Int,
  var partitionWidth:Int,
  var partitionHeight:Int,
  var imageWidht:Long,
  var imageHeight:Long,
  var imageBytes:Array[Byte]
){
  
  override def toString: String ={
    return s"${imageId}_${imageLevel}_${imagePartitions}_${partitionIndex}_${partitionZIndex}_${partitionWidth}_${partitionHeight}_${imageWidht}_${imageHeight}" 
  }
}


object ImageTile {
  /**
   * @param fileName The actual name of the file following the convention used in this project, without path
   * @param bytes The bytestream of the encoded file, not buffered image, just bytestream
   */
  def apply(fileName:String, bytes:Array[Byte]) = {
    val params = fileName.split("_")
    
    val _imageId = params(0) 
    val _imageLevel = params(1).toInt 
    val _imagePartitions = params(2).toInt 
    val _partitionIndex = params(3).toInt 
    val _partitionZIndex = params(4).toInt 
    val _partitionWidth = params(5).toInt 
    val _partitionHeight = params(6).toInt 
    val _imageWidth = params(7).toLong 
    val _imageHeight = params(8).toLong
    
    new ImageTile(_imageId, _imageLevel, _imagePartitions, _partitionIndex, _partitionZIndex,
        _partitionWidth, _partitionHeight, _imageWidth, _imageHeight, bytes)
  }
  
  def schema():Seq[String] ={
     val s = Seq(
        "imageId", "imageLevel", "imagePartitions", "partitionIndex", 
        "partitionZIndex", "partitionWidth", "partitionHeight", 
        "imageWidht", "imageHeight", "imageBytes"
      )
      return s
   }
  
}