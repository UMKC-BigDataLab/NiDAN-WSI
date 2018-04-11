import nidan.io.LinearSplit
import nidan.io.NidanImage
import nidan.io.NidanImageSplit
import javax.imageio.ImageIO
import java.io.File


object TestMain {
  def main(args: Array[String]): Unit = {    
    val imgName = args(0)
    
    val stream = ImageIO.createImageInputStream(new File(imgName))
    val reader = ImageIO.getImageReaders(stream)
    if(!reader.hasNext()) throw new ClassNotFoundException
	  val imgReader = reader.next()
    imgReader.setInput(stream)
    val width = imgReader.getWidth(imgReader.getMinIndex)
    val height = imgReader.getHeight(imgReader.getMinIndex)
    println(s"W: $width H: $height")
    
    println("Before splits")
    val linearSplits = LinearSplit.getSplits(width, height, 4)
    println(s"After splits size: ${linearSplits.size}")
    
    linearSplits.foreach{e =>
      println(s"Index: ${e._1} ")
    }
    
//    println(s"PatientID:${img.patientId} ImageID: ${img.imageId}")
//    img.splits.foreach(i => println(s"(${i.coordX}, ${i.coordY}) \n"))
  }
}