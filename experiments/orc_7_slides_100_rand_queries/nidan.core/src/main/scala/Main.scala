//import java.awt.Rectangle
//import java.io.BufferedInputStream
//import java.io.File
//import java.io.FileInputStream
//import java.io.FileNotFoundException
//import java.io.IOException
//import java.io.InputStream
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
//
//import javax.imageio.ImageIO
//import nidan.models.entities.R_PatientWSImage
//import java.io.ByteArrayOutputStream
//import java.nio.ByteBuffer
//import java.io.BufferedOutputStream
//import java.io.FileOutputStream
//import scala.io.Source
// 
//// TODO generate image with representation
//// TODO generate space filling curve module
//// TODO generate an RDD with image formats and codes
//
//
//
//object Main{
//  
//  def main1(args: Array[String]): Unit = {
//    val imageDir = args(0)
//    val resultPath = "/users/dl544/nidan.core"
//    println(s"Trying with $imageDir")
// 
//    // Display the image width and height
//    val stream = ImageIO.createImageInputStream(new File(imageDir))
//    val reader = ImageIO.getImageReaders(stream)
//    
//    if(!reader.hasNext()) {
//      println("Error, no reader found")
//      return 
//    }
//      
//
//    println("It found a reader")
//    val imgReader = reader.next()
//    imgReader.setInput(stream)
//    val param = imgReader.getDefaultReadParam
//
//    val w = imgReader.getWidth(imgReader.getMinIndex)
//    val h = imgReader.getHeight(imgReader.getMinIndex)
//
//    // Test, a 4 division
//    println(s"Image info: W $w H $h")
//    val pieces = 4
//    val wGap = w / pieces
//    val hGap = h / pieces
//
//    var wOffset, hOffset = 0
//    var i, j = 0
//
//    val patientId = 1
//    val imageId = 1
//
//    for(i <- 0 to pieces-1){
//    	  val hRect = (hOffset, hOffset + (hGap-1))
//    		hOffset += hGap
//    		wOffset = 0
//
//    		for(j <- 0 to pieces-1){
//    		  val index = ((i * (pieces -1)) + i) + (j + 1)
//    		  // TODO change this to a proper function 
//    		  val indexSFC = 1
//    		  
//    			val wRect = (wOffset, wOffset + (wGap-1))
//    			wOffset += wGap
//
//    			println(s"LowCoorner: (${hRect._1} ${wRect._1}) MaxCorner: (${hRect._2} ${wRect._2})")
//    			val region = new Rectangle(wRect._1, hRect._1, wGap, hGap)
//    		  param.setSourceRegion(region)
//    		  val buffer = imgReader.read(0, param)
//    		  val arrayBuffer = new ByteArrayOutputStream()
//
//    			ImageIO.write(buffer, "jpg", new File(resultPath + "/img_" + index + ".jpg"))
//    			// Save as array
//    			ImageIO.write(buffer, "jpg", arrayBuffer)
//
//    			val headerInt = Array[Int](patientId, imageId, wRect._1, hRect._1, wGap, hGap, index, indexSFC)
//    			val header = getByteArray(headerInt)
//    			val byteImage = header ++ arrayBuffer.toByteArray()
//    			
//    			NidanImageTest.writeObject(byteImage, resultPath + "/img_" + index + ".obj")
//    		}
//    }
//  }
//  
//  
//  
//  def main(args: Array[String]): Unit = {
//    // Saving the data in a Scala style
//    val resultPath = "/users/dl544/nidan.core"
//    val testArray = Array[Int](1,0,256,1000,0,1)
//    val bytes = getByteArray(testArray)
//    
//    // Printing the array before save
//    println(">> Array before save")
//    testArray.foreach(println)
//    
//    // Write the file in Scala way
//    writeObject(bytes, resultPath + "/testBinObject.o")
//    println(">> Object saved in disk")
//    
//    // Read the object
////    val array1 = readObject(resultPath + "/testBinObject.o")
////    // Create the buffer
////    val buffer = ByteBuffer.allocate(4 * testArray.size)
////    buffer.put(array1)
////    buffer.rewind
//    
//    val array2 = readObject2(resultPath + "/testBinObject.o")
//    // Create the buffer
//    val buffer2 = ByteBuffer.allocate(4 * testArray.size)
//    buffer2.put(array2)
//    buffer2.rewind
//    
////    println(">> Output1:")
////    for(i <- 0 to testArray.size -1) println(buffer.getInt)
//    
//    println(">> Output2:")
//    for(i <- 0 to testArray.size -1) println(buffer2.getInt)
//    
//  }
//  
//  def writeObject(byteArray:Array[Byte], path:String) = {
//    val buffer = new BufferedOutputStream(new FileOutputStream(path))
//    Stream.continually(buffer.write(byteArray))
//    buffer.close()
//  }
//  
//
//  
//  def readObject2(path:String):Array[Byte] = {
//    val fis = new FileInputStream(new File(path))
//    
//    val array = Stream.continually(fis.read)
//      .takeWhile(_ != -1)
//      .map(_.toByte)
//      .toArray
//    
//    fis.close()
//    
//    return array
//  }
//  
//  def mainJava(args: Array[String]): Unit = {
//    val resultPath = "/users/dl544/nidan.core"
//    val testArray = Array[Int](1,0,256,1000,0,1)
//    val bytes = getByteArray(testArray)
//    
//    // Printing the array before save
//    println(">> Array before save")
//    testArray.foreach(println)
//    
//    
//    
//    println(s">> Info: Allocated(${4*testArray.size}) Required(${bytes.length})")
//    
//    // Write the array in a binary file
//    val fos = new FileOutputStream(new File(resultPath + "/test.obj"))
//    val fc = fos.getChannel
//    println(">> File created")
//    
//    // Creating the buffer
//    val buffer = ByteBuffer.allocate(4 * testArray.size)
//    buffer.put(bytes)
//    buffer.rewind
//    println(">> Buffer created")
//    
//    // Writing to disk
//    fc.write(buffer)
//    fc.close()
//    fos.close()
//    println(">> Buffer saved")
//    
//    // Reading
//    println(">> Reading the file")
//    val fis = new FileInputStream(new File(resultPath + "/test.obj"))
//    val fcR = fis.getChannel
//    val buffR = ByteBuffer.allocate(4 * testArray.size)
//    fcR.read(buffR)
//    buffR.rewind
//    
//    // Printing the numbers
//    println(">> The numbers are")
//    var i = 0
//    for(i <- 0 to testArray.size -1) println(buffR.getInt)
//    
//    fcR.close()
//    fis.close()
//  }
//  
//  def mainReadIMG(args: Array[String]): Unit = {
//    val path = args(0)
//    val files = testReading(path)
//    
//    files.foreach(println)
//    
//    val imgs = files.map(file => NidanImageTest.readImage(file))
//    
//    imgs.foreach(img =>
//      println(s"PID: ${img.patientId} IMGID: ${img.imageId}" +
//              s"INDEX: ${img.index} SPCINDEX: ${img.indexSPC} " + 
//              s"X: ${img.coordX} Y: ${img.coordY}")
//    )
//  }
//  def testReading(path:String):Array[String] = {
//    // Looks for Local File System
//		val nameFiles = new File(path).listFiles
//    		.filter(f => f.isFile)
//    		.filter(f => f.getName.contains(".obj"))
//    		.map(x => x.getAbsolutePath)
//    	
//    		return nameFiles
////    	nameFiles.foreach(println)
//  }
//  
//  def getByteArray(intArray:Array[Int]) : Array[Byte] = {
//    val byteBuffer = ByteBuffer.allocate(intArray.size * (Integer.SIZE/8))
//    intArray.map(byteBuffer.putInt(_))
//    
//    byteBuffer.flip
//    return byteBuffer.array
//  }
//
//  
//  def tupleFromFileName(fileName : String) : (Int, String) ={
//  	  val numbPattern = "([0-9]+)".r
//    
//    val revFN = fileName.reverse
//    val keyValue = (numbPattern findFirstIn revFN) match {
//      case Some(value) => value.toString.reverse.toInt
//      case _ => -1
//    }
//    
//    (keyValue, fileName)
//  }
//  
//  
//  
//  def readImageFromLocal(file: File) : Array[Byte] = {
//    var totalBytesRead, bytesRemaining, bytesRead = 0
//    
//    val result = new Array[Byte](file.length.toInt)
//    var input: InputStream = null
//    
//    try {  
//      input = new BufferedInputStream(new FileInputStream(file))  
//      while (totalBytesRead < result.length) {
//        bytesRemaining = result.length - totalBytesRead
//        bytesRead = input.read(result, totalBytesRead, bytesRemaining)
//        
//        if (bytesRead > 0) totalBytesRead = totalBytesRead + bytesRead
//      }
//      
//      input.close()
//    }
//    catch {
//      case ex: FileNotFoundException => println("FileNotFoundException exception")
//      case ex: IOException => println("IOException exception")
//    }
//    
//    return result
//  }
//  
//  
//  
//	def mainModel(args : Array[String]) = {
//		val path = args(0)
//		val sqlQuery = args(1)
//
//		val conf = new SparkConf()
//		val sc = new SparkContext(conf)
//		
//		
//
//		// Looks for HDFS File System
////		val fs = FileSystem.get(new Configuration())
////		val status = fs.listStatus(new Path(path))
////		val nameFiles = status.map(x => x.getPath.toString)
//		
//		println(s"Working on the path: $path")
//		println(s"Dealing with $sqlQuery")
//		
//		// Looks for Local File System
//		val nameFiles = new File(path).listFiles
//    		.filter(f => f.isFile)
//    		.map(x => x.getAbsolutePath)
//    		
//		val fileMap = sc.parallelize(nameFiles)
//    		.map(x => tupleFromFileName(x))
//      .sortByKey()
//      .collect()
//
//		// Print the files
////    println(s"The list of files in: $path \n-------------------")
////		fileMap.foreach{
////			case (k, v) => println(s"[$k] File name: $v")
////		}
//		println("-----------\n End of the list")
//		
//		// Read the image from the file
//    val imageRDD = fileMap.map{ 
//		  case (k, v) => (k, readImageFromLocal(new File(v)))
//      case _ => (-1, null)
//		}
//		
//		// Create the RDD as patient, index, bytes
//		val patientId = 1
//		val imagePatient = imageRDD.map{
//		  case (k, v) => (patientId, k, v)
//		  case _ => (-1, -1, null)
//		}
//		
//		// Create the class R_PatientWSImage
//		val relation = imagePatient.map{
//		  case (id, k, v) => R_PatientWSImage(id, k, v)
//		  case _ => null
//		}
//		
////		imagePatient.map{case (id, k, img) => 
////		  s"Patient: $id, Index: $k Img Size: ${img.size}"
////		}.foreach(println)
//		
//		val imagePatientRows = sc.parallelize(imagePatient.map{
//		  case (id, k, v) => Row(id, k, v)
//		})
//		      
//		val sqlContext = new SQLContext(sc)
//		val df = sqlContext.createDataFrame(imagePatientRows, R_PatientWSImage.getSchema)
//		df.registerTempTable(R_PatientWSImage.tableName)
//		
//		val res = sqlContext.sql(sqlQuery)
//		res.map(tuple => s"Patient: ${tuple(0)}, Index: ${tuple(1)}").collect().foreach(println)
//		
//		
//		sc.stop()
//	}
//	
//	
//}
//
//case class NidanImageTest(patientId:Int, imageId:Int, format:Int, coordX:Int, coordY:Int,
//    heigh:Int, width:Int, index:Int, indexSPC:Int, data:Array[Byte])
//    
//object NidanImageTest{
//  val headerSize = 9
//  val intSize = Integer.SIZE / 8
//  
//  def getByteArrayFromIntArray(intArray:Array[Int]) : Array[Byte] = {
//    val byteBuffer = ByteBuffer.allocate(intArray.size * intSize)
//    intArray.map(byteBuffer.putInt(_))
//    
//    byteBuffer.flip
//    byteBuffer.array
//  }
//  
//  def writeObject(byteArray:Array[Byte], path:String) = {
//    val buffer = new BufferedOutputStream(new FileOutputStream(path))
//    Stream.continually(buffer.write(byteArray))
//    buffer.close()
//  }
//  
//  
//  def readObject(path:String):Array[Byte] = {
//    val fis = new FileInputStream(new File(path))
//    
//    val array = Stream.continually(fis.read)
//      .takeWhile(_ != -1)
//      .map(_.toByte)
//      .toArray
//    
//    fis.close()
//    
//    return array
//  }
//  
//  def getBufferFromByteArray(bytes:Array[Byte]):ByteBuffer = {
//    val buffer = ByteBuffer.allocate(bytes.size)
//    buffer.put(bytes).rewind
//      
//    return buffer
//  }
//  
//  
//  def readImage(path:String):NidanImageTest = {
//    val byteArray = readObject(path)
//    
//    val header = byteArray.slice(0, 9 * intSize)
//    val buffer = ByteBuffer.allocate(9 * intSize)
//    
//    header.map(buffer.put(_))
////    val buffer = ByteBuffer.wrap(header)
//    buffer.flip
//    
//    val nidanImage = NidanImageTest(
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        buffer.getInt,
//        byteArray.slice(10 * intSize, byteArray.size)
//    		)
//    		
//    	return nidanImage  
//  }
//  
//}