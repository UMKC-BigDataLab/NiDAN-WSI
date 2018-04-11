package nidan.test

import nidan.tiles.TileDimension
import nidan.regions.CoordinateGenerator


object TestCoordinateGenerator {
  def main(args: Array[String]): Unit = {
    val dim1 = new TileDimension(8, 8)
    val coords = CoordinateGenerator.squareMatrixfromDimension(dim1, 4)
    
    coords.foreach{x => 
      println(s"${x._1}, " +
          s"<${x._4.x.toInt},${x._4.y.toInt}; " +
          s"${x._4.x.toInt + x._5.width.toInt},${x._4.y.toInt}; " +
          s"${x._4.x.toInt},${x._4.y.toInt + x._5.height.toInt}; " +
          s"${x._4.x.toInt + x._5.width.toInt},${x._4.y.toInt + x._5.height.toInt}>, " +
          s"${x._2}, ${x._3}")
    }
    
    println("End of test")
  }
}