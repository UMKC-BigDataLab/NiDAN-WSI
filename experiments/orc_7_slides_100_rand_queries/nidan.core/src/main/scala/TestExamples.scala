

object TestExamples {
  def main(args: Array[String]): Unit = {
    println("Its working")
    
    
    val pattern = "_([0-9]+).([A-Za-z]+)".r
    val numPatt = "([0-9]+)".r
    
    val testStr = "_123.png"
    val testNum = "123 34 _234.png"
    val testReverse = "/this/is/a/long/url_34gd.png"
    
    
    // Testing regex
    val str = (pattern findAllIn testStr).mkString(",")
    println(str)
    
    // Test 2 Get the numbers
    val strNumbs = (numPatt findAllIn testNum).mkString("#")
    println(strNumbs)
    
    // Testing reverse
    val rev = testReverse.reverse
    val num = (numPatt findFirstIn rev) match {
      case Some(value) => value.toString.reverse
      case _ => -1
    }
    
    println(num)
		  
  }
}