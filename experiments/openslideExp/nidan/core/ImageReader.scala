package nidan.core

import java.io.File
import java.nio.file.Paths
import java.nio.file.Files
import java.awt.image.BufferedImage
import org.apache.commons.imaging.ImageFormats
import org.apache.commons.imaging.Imaging
import org.openslide.OpenSlide
import java.awt.image.DataBufferInt
import nidan.io.NidanRectangle

object ImageReader {
  def bytesFromFile(input:String):Array[Byte] = {
    return Files.readAllBytes(Paths.get(input))
  }
  
  // TODO Test if this is correct with PNG, JPEG, TIFF
  def bytesFromBufferedImage(img:BufferedImage, format:ImageFormats):Array[Byte] = { 
    return Imaging.writeImageToBytes(img, format, null)
  }
  
  def bufferedFromOpenSlide(fig:NidanRectangle, level:Int, source:OpenSlide):BufferedImage = {
    val buffer = new BufferedImage(fig.width, fig.height, BufferedImage.TYPE_INT_ARGB)
    val graphics = buffer.getGraphics
    val dataArray = buffer.getRaster.getDataBuffer.asInstanceOf[DataBufferInt].getData
        
    source.paintRegionARGB(dataArray, fig.topX, fig.topY, level, fig.width, fig.height)
    graphics.drawImage(buffer, 0, 0, fig.width, fig.height, null)
    
    return buffer      
  }
}