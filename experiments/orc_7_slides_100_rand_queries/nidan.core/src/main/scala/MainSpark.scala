//import java.io.BufferedInputStream
//import java.io.File
//import java.io.FileInputStream
//import java.io.FileNotFoundException
//import java.io.IOException
//import java.io.InputStream
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.input.PortableDataStream
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
//
//import javax.imageio.ImageIO
//import nidan.io.LinearSplit
//import nidan.models.entities.R_PatientWSImage
//
//
//object MainSpark {
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
//  // Looks for HDFS File System
////		val status = fs.listStatus(new Path(path))
////		val nameFiles = status.map(x => x.getPath.toString)
////		val fs = FileSystem.get(new Configuration())
//		
//  
//  def mainStuff(args: Array[String]): Unit = {
//    
//		val conf = new SparkConf()
//		val sc = new SparkContext(conf)
//		
//		val path = args(0)
//		val sqlQuery = args(1)
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
//  }
//  
//  def testSparkNidanImageSplit(sc:SparkContext, args:Array[String]):Int ={
//    var result = 1
//    // Arguments please
//    val imgName = args(0)
//    val totalSplits = args(1).toInt
//    
//    println(">>> In the function")
//    println(imgName)
//    println(totalSplits)
//    
//    // Read the image
//    val stream = ImageIO.createImageInputStream(new File(imgName))
//    val reader = ImageIO.getImageReaders(stream)
//    if(!reader.hasNext()) throw new ClassNotFoundException
//	  val imgReader = reader.next()
//    imgReader.setInput(stream)
//    val width = imgReader.getWidth(imgReader.getMinIndex)
//    val height = imgReader.getHeight(imgReader.getMinIndex)
//    println(s"W: $width H: $height")
//    
//    // Get the splits
//    val linearSplits = LinearSplit.getSplits(width, height, totalSplits)
//    println(s">>> Working with: ${linearSplits.size} splits")
//    println(">>> Sizes:")
//    linearSplits.foreach{e =>
//      println(s"Index: ${e._1} ${e._2}")
//    }
//
//    // Broadcast the image
//    val broadcastImage = sc.broadcast(imgReader)
//    
//    // Parallelize the splits
//    val parallelizeSplits = sc.parallelize(linearSplits, linearSplits.size)
//    
//    println(">>> Some stuff")
//    println(broadcastImage.value.toString())
//    println(parallelizeSplits.id)
//    
//    
//    println(">>> It seams to be working")
//    return result
//  }
//  
//  
//  def readFile(sc:SparkContext, path:String)
//  :RDD[(String, PortableDataStream)] = {
//    val files = sc.binaryFiles(path)
//    
//    return files
//  }
//  
//  
//  
//  
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//		val sc = new SparkContext(conf)
//    
//    println(">>> The arguments are:")
//    args.foreach(println)
//    
//    val imgName = args(0)
//    
//    val files = readFile(sc, imgName)
//    files.foreach{img =>
//      println(s"About to process: ${img._1}")  
//      val stream = ImageIO.createImageInputStream(img._2.open())
//      val reader = ImageIO.getImageReaders(stream)
//      
//      if(!reader.hasNext()) throw new ClassNotFoundException
//	    
//      val imgReader = reader.next()  
//	    imgReader.setInput(stream)
//	    
//      val width = imgReader.getWidth(imgReader.getMinIndex)
//      val height = imgReader.getHeight(imgReader.getMinIndex)
//      println(s">>> W: $width H: $height")
//      
//      
////      println(s"W:${image.getWidth} H:${image.getHeight}")
//    }
//    
////     // Read the image
//
//    
//    println(">>> Starting test")
//    // Test
////    val result = testSparkNidanImageSplit(sc, args)
//    
//    println(">>> End of the test")
//    
//    sc.stop()
//  }
//}