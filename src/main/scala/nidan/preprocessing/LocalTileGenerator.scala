package nidan.preprocessing

import org.openslide.OpenSlide
import java.io.File
import scala.collection.mutable.ArrayBuffer
import java.awt.image.BufferedImage
import java.awt.image.DataBufferInt
import org.apache.commons.imaging.ImageFormats
import java.awt.image.WritableRaster
import javax.imageio.ImageIO
import javax.imageio.plugins.jpeg.JPEGImageWriteParam
import javax.imageio.IIOImage
import javax.imageio.stream.FileImageOutputStream
import javax.imageio.ImageWriteParam
import org.apache.commons.imaging.Imaging
import java.io.PrintWriter


// TODO Find a better way to group this classes, using the
// Scala style
case class TileOrder(zOrder:Int, rowOrder:Int, hOrder:Int, nOrder:Int)
case class TileCoordinate(x:Long, xPlusOffset:Long, y:Long, yPlusOffset:Long, level:Int, index:Int)
case class Tile(image:BufferedImage, coord:TileCoordinate, order:TileOrder) 


class LocalTileGenerator(imageFilepath:String, outputDir:String) {
  
  /**
   * The WSI image from where we will extact all the tiles
   */
  private val format = ImageFormats.JPEG
  private var wsiImage:OpenSlide = new OpenSlide(new File(imageFilepath)) 
  var imageLevels:Int = wsiImage.getLevelCount
  var totalTiles = 0L 
  
  /**
   * Divides the image stored at a given level into a sequence of totalTiles.
   * @return A sequence of coordinates for each tile
   */
  private def generateTilesCoordinates(level:Int):Seq[TileCoordinate] = {
    val widht = wsiImage.getLevelWidth(level)
    val height = wsiImage.getLevelHeight(level)
    
    // Since its a cuadratic matrix, rows and cols will have
    // the same value, for simplicity's sake keep it in rows
    val rows = Math.sqrt(totalTiles).toInt
    val tileWidht = widht / rows
    val tileHeight = height / rows
    
    var (xOffset, yOffset) = (0L, 0L)
    var (currentRow, currentCol) = (0, 0)
    
    val tileArray = new ArrayBuffer[TileCoordinate]()
    var tileIndex = 0
    
    /* Do the cycle ignoring the image's border tiles 
     * The idea is to avoid using ifs, and speeding up
     * the tile creation process
    */
    // Row cycle
    for (currentRow <- 0 to (rows -2)){
      xOffset = 0
      
      // Col cycle
      for (currentCol <- 0 to (rows -2)){
        tileIndex += 1
        tileArray.append(TileCoordinate(xOffset, xOffset + tileWidht, yOffset, yOffset + tileHeight, level, tileIndex))
        xOffset += tileWidht
      }
      
      yOffset += tileHeight
      tileIndex += 1
    }
    
    /* Add the image from the last row */
    xOffset = 0
    tileIndex = (rows * (rows - 1)) + 1
    for (currentCol <- 0 to (rows -2)){
      tileArray.append(TileCoordinate(xOffset, xOffset + tileWidht, yOffset, height, level, tileIndex))
      tileIndex += 1
      xOffset += tileWidht
    }
    
    /* Add the image from the last column */
    xOffset = (rows - 1) * tileWidht
    yOffset = 0
    tileIndex = rows
    for (currentRow <- 0 to (rows -2)){
      tileArray.append(TileCoordinate(xOffset, widht, yOffset, yOffset + tileHeight, level, tileIndex))
      tileIndex += rows
      yOffset += tileHeight
    }
    
    /* Add the last tile to the array */
    tileArray.append(TileCoordinate((rows -1) * tileWidht, widht, (rows -1) * tileHeight, height, level, totalTiles.toInt))
    
    tileArray.toSeq
  } 
  
  /**
   * Geneartes the output directory for a given level that
   * will get processed, the directory will be created under
   * outputDir. Returns directory's path
   */
  private def makeOutputDir(level:Int):String = {
    val newDir = s"$outputDir/${new File(imageFilepath).getName}L$level"
    val dir = new File(newDir)
    
    dir.mkdir()
    return newDir
  }
  
  val imageFromCoord = (coord:TileCoordinate) => {
    new BufferedImage((coord.xPlusOffset - coord.x).toInt, (coord.yPlusOffset - coord.y).toInt, BufferedImage.TYPE_INT_ARGB)
  }
  
  val zOrderFromIndex = (index:Int) =>{
    val rows = Math.sqrt(totalTiles).toInt
 
    val binRow = f"${Integer.toBinaryString((index-1) / rows).toInt}%032d".toCharArray
    val binCol = f"${Integer.toBinaryString((index-1) % rows).toInt}%032d".toCharArray
    
    var i = 0
    var zString = new StringBuilder()
    for (i <- 0 to binRow.length - 1){
      zString.append(binRow(i))
      zString.append(binCol(i))
    }
  
    Integer.parseInt(zString.toString, 2) + 1
  }
    
  
  val hOrderFromIndex = (index:Int) =>{
    index
  }
  
  
  // Code based on: https://en.wikipedia.org/wiki/Hilbert_curve
  // TODO Test the following code
  // ******************************************************
  // ******************************************************
  val h_rot = (n:Int, x:Int, y:Int, rx:Int, ry:Int) =>{
    var newX = x
    var newY = y
    
    if(ry == 0){
      if(rx == 1){
        newX = (n-1) - x
        newY = (n-1) - y
      }
      
      val t = newX
      newX = newY
      newY = t
    }
    
    (newX, newY)
  }
  
  val h_d2xy = (n:Int, d:Int) =>{
    var t = d
    var newX, newY = -1
    var rx, ry = 0
    
    if(d >= 0 && d < (n*n)){
      newX = 0
      newY = 0
      
      var i = 1
      for(i <- 1 to (n-1) by (i*2)){
        
        rx = if ((t & 2) == 2) 1 else 0
        ry = if ((t & 1) == 1) 1 else 0
        
        if (rx == 1){
          ry = if (ry == 1) 0 else 1
        }
        
        val newValues = h_rot(i, newX, newY, rx, ry)
        if (rx == 1)
          newX = newValues._1 + i
        if(ry == 1)
          newY = newValues._2 + i
         
        t /= 4
      }
    }
    
    (newX, newY)
  }
  // ******************************************************
  // ******************************************************
  
  val nOrderFromIndex = (index:Int) => {
    index
  }
  
  //zOrder:Int, rowOrder:Int, hOrder:Int, nOrder:Int)
  val ordersFromCoord = (coord:TileCoordinate) => {TileOrder(
      zOrderFromIndex(coord.index),
      coord.index,
      hOrderFromIndex(coord.index),
      nOrderFromIndex(coord.index)
    )
  }
  
  //case class Tile(image:BufferedImage, coord:TileCoordinate, zOrder:Int, rowOrder:Int, hOrder:Int, nOrder:Int) 
  
  /**
   * Writes a tile into a file
   */
  private def writeTileCoordinate(output:String, coord:TileCoordinate) = {
    val tile = Tile(null, coord, ordersFromCoord(coord))
    
    val image = imageFromCoord(coord)
    val g = image.getGraphics
    val dataArray = image.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
    
    val w = (coord.xPlusOffset - coord.x).toInt 
    val h = (coord.yPlusOffset - coord.y).toInt
    wsiImage.paintRegionARGB(dataArray, coord.x, coord.y, coord.level, w, h)
    
    g.drawImage(image, 0, 0, w, h, null)
    format.toString match{
      case "JPEG" => writeJPEG(image.getRaster, output)
      case _ => writeDefault(image, output)
    }
  }
  
  private def writeJPEG(raster:WritableRaster, output:String){
    val jpegParams = new JPEGImageWriteParam(null)
    jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT)
    jpegParams.setCompressionQuality(0.9f)
    
    val writer = ImageIO.getImageWritersByFormatName(format.toString).next()
    writer.setOutput(new FileImageOutputStream(new File(output)))
    writer.write(null, new IIOImage(raster, null, null), jpegParams);
  }  
  
  private def writeDefault(image:BufferedImage, output:String){
    Imaging.writeImage(image, new File(output), format, null)
  }
  
  //case class TileCoordinate(x:Long, xPlusOffset:Long, y:Long, yPlusOffset:Long, level:Int, index:Int)
  val coordToString = (c:TileCoordinate) =>{
    s"${c.xPlusOffset - c.x},${c.yPlusOffset - c.y},${c.x},${c.y}"
  }
  
  val orderToString = (tile:TileOrder) =>{
    s"${tile.rowOrder},${tile.zOrder},${tile.hOrder},${tile.nOrder}"
  }
  /********************************************************/
  // PUBLIC METHODS 
  /********************************************************/
  /**
   * Generate a sequence of tiles for a given level.
   * @param	level			The level in the WSI image which we want to divide
   * @param	numberOfTiles		A base 4 number indicating the total number of tiles, Ex 4 tiles or 16 tiles or 32 tiles 
   */
  def generateTiles(level:Int, numberOfTiles:Int) = {
    val slideId = new File(imageFilepath).getName
    val levelHeight = wsiImage.getLevelHeight(level)
    val levelWidth = wsiImage.getLevelWidth(level)
    
    totalTiles = numberOfTiles
    val tiles = generateTilesCoordinates(level)
    val baseDir = makeOutputDir(level)
    val metadata = new ArrayBuffer[String]()
    
    metadata.append("## filename,slideid,imageformat,level,partitions,levelW,levelH,tileW,tileH,x,y,rowOrder,zOrder,hOrder,nOrder")
    tiles.map{ t =>
      val fileName = s"tile.${level}.${t.index}.${format.toString}"
      val file = s"$baseDir/$fileName"
      
      val entry = new ArrayBuffer[String]()
      entry.append(fileName, slideId, format.toString, level.toString, totalTiles.toString)
      entry.append(levelWidth.toString, levelHeight.toString)
      entry.append(coordToString(t), orderToString(ordersFromCoord(t)))
      
      writeTileCoordinate(file, t)
      metadata.append(entry.mkString(","))
    }
    
    val metaFile = new PrintWriter(new File(s"$baseDir/metadata.txt"))
    metadata.foreach(line => metaFile.write(line + "\n"))
    metaFile.close
    
  }
}

