import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import scala.collection.mutable.ListBuffer


object EmpiricalObservation2_1995 {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("spark://richmond:30166").getOrCreate().sparkContext
    //     val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    sc.setLogLevel("ERROR")
    println("Starting EmpiricalObservation2_1995. If you see this something is running.")

    val g20_value = 22234693

    // (9212268,List(9304281, 9308201, 9304310, 9306321, 9310354, 9311259))
    var valid_node_neighbor_1995 = sc.textFile("/loaders/valid_node_neighbor_1995").
      map( x => (x.split(",")(0).substring(1), x.split("List")(1).
        substring(1,x.split("List")(1).length-2 ).split(", ").toList    ) ).
      persist(MEMORY_AND_DISK_SER)
    // (9203203~9312293,9203203 9312293)
    var path_container = sc.textFile("/loaders/path_container_1995").map( x => (x.split(",")(0).
      substring(1),x.split(",")(1).substring(0,x.split(",")(1).length-1 ) ))

    //======================================    //======================================
    //======================================    //======================================   x._2.split("\\s+")(x._2.split("\\s+").length)

    val len3 = path_container.union(path_container.map( x => (
      x._1.split("~")(1) +"~"+ x._1.split("~")(0),
      x._2.split("\\s+").reverse.mkString("", " ", "")) ))

    //    println("PATHPATHPATH")
    //    path_container.foreach(println)
    //    println("PATHPATHPATH")
    var ixx = 2
    //    println("BEFORE ANYTHING len3 is:")
    //    len3.foreach(println)
    //    println("len3 count is: " + len3.count())
    def pathAdder (input_tilde_paths: RDD[(String,String)], node_neighbors: RDD[(String,List[String])]) : RDD[(String,String)] = {
      println("pathAdder:"+ixx)
      input_tilde_paths.persist(MEMORY_AND_DISK_SER)
      var longer_paths = input_tilde_paths.
        map( x => (x._1.split("~")(1),x._2)).
        //(a, ( b a, List( x,y,z )))
        join(node_neighbors).flatMap{
        case (node, (path, nodeList)) =>
          var path_extension_list = new ListBuffer[String]()
          //          println("     checking nodeList:"+nodeList)
          if (nodeList.size > 1) {
            for (i <- nodeList.indices) {
              if(!path.contains(nodeList(i))) {
                path_extension_list += path +" "+ nodeList(i)
                //                println("ADDING:"+path + " " + nodeList(i))
              }
            }
          }
          path_extension_list.toList
      }.map( x => (x.split("\\s+")(0) +"~"+ x.split("\\s+")(x.split("\\s+").length-1) , x)).
        union(input_tilde_paths)

      //      input_tilde_paths.unpersist()
      //println(">>>PRETRIM")
      var trimmed = longer_paths.groupByKey(300).map(x => (x._1, x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map(x => (x._1,x._2.head))

      trimmed.persist(MEMORY_AND_DISK_SER)
      input_tilde_paths.unpersist()
      //  trimmed.foreach(println)
      //  println(">>>PRETRIM")
      return trimmed
    }

    def totalPathCount(input_tilde_paths: RDD[(String, String)]): Int = {
      println("totalPathCount:"+ixx)
      val temprdd = input_tilde_paths.map(x => (x._2.split("\\s+").length - 1, 1)).reduceByKey(_ + _).collect()
      println("attempting to print contents of counts...")
      temprdd.foreach(println)
      val total_path_count = input_tilde_paths.map(x => (x._2.split("\\s+").length - 1, 1)).reduceByKey(_ + _).values.sum().toInt

      println(">>count="+total_path_count)
      return total_path_count
    }

    def checkNinety(path_count: Int, ninety_value: Int): Boolean = {
      val a = path_count.toDouble/ninety_value.toDouble
      println("percent at ixx="+ixx+" is "+a)
      return a>=.9
    }

    def areWeDoneHere(input_tilde_paths: RDD[(String, String)]): Boolean = {
      checkNinety(totalPathCount(input_tilde_paths), g20_value)
    }

    def yesWeAreDone(input_tilde_paths: RDD[(String, String)]): Unit = {
      println("DONE DONE DONE DONE DONE :" + ixx)
//      input_tilde_paths.collect()
      println(" >> FINAL RAW COUNT << ")
      println("   >> " + input_tilde_paths.count() + " << ")
      input_tilde_paths.map( x => (x._2.split("\\s+").length-1,1)).reduceByKey(_+_,300).saveAsTextFile(args(2))
    }

    def noWeNotDone(input_tilde_paths: RDD[(String, String)]): Unit = {
      println("")
      println("keep going at: " + ixx)
    }

    def checkIt(input_tilde_paths: RDD[(String, String)]): Unit = {
      println("checkIt with ixx="+ixx+" and raw count is =" + input_tilde_paths.count())
      //      println("//// START checkIt input_tilde_paths")
      //      input_tilde_paths.foreach(println)
      //      println("//// STOPS checkIt input_tilde_paths")

      if(areWeDoneHere(input_tilde_paths)){
        yesWeAreDone(input_tilde_paths)
      } else {
        noWeNotDone(input_tilde_paths)
        //        input_tilde_paths.unpersist()
      }
    }
//    var lenOld = len3
//    var lenNew = len3
//
//
//    for(i <- 2 until 21){
//      try {
//        ixx = i
//        lenNew = pathAdder(lenOld, valid_node_neighbor_1995)
//        checkIt(lenNew)
//        lenOld = lenNew
//      } catch {
//        case err: Any => {
//          println("caught something? ixx: " + ixx)
//          println(">> error: " + err)
//        }
//      } finally{
//          sc.stop()
//      }
//    }


    try {
      ixx = ixx + 1

      val len4 = pathAdder(len3, valid_node_neighbor_1995)
      checkIt(len4)


      ixx = ixx + 1
      val len5 = pathAdder(len4, valid_node_neighbor_1995)
      checkIt(len5)

      ///////////////////////////////////////////////////

      ixx = ixx + 1
      val len6 = pathAdder(len5, valid_node_neighbor_1995)
      checkIt(len6)

      ixx = ixx + 1
      val len7 = pathAdder(len6, valid_node_neighbor_1995)
      checkIt(len7)

      ixx = ixx + 1
      val len8 = pathAdder(len7, valid_node_neighbor_1995)
      checkIt(len8)

      ixx = ixx + 1
      val len9 = pathAdder(len8, valid_node_neighbor_1995)
      checkIt(len9)

      ixx = ixx + 1
      val len10 = pathAdder(len9, valid_node_neighbor_1995)
      checkIt(len10)

      ///////////////////////////////////////////////////
      ///////////////////////////////////////////////////

      ixx = ixx + 1
      val len11 = pathAdder(len10, valid_node_neighbor_1995)
      checkIt(len11)

      ixx = ixx + 1
      val len12 = pathAdder(len11, valid_node_neighbor_1995)
      checkIt(len12)

      ixx = ixx + 1
      val len13 = pathAdder(len12, valid_node_neighbor_1995)
      checkIt(len13)

      ixx = ixx + 1
      val len14 = pathAdder(len13, valid_node_neighbor_1995)
      checkIt(len14)

      ixx = ixx + 1
      val len15 = pathAdder(len14, valid_node_neighbor_1995)
      checkIt(len15)

      ///////////////////////////////////////////////////

      ixx = ixx + 1
      val len16 = pathAdder(len15, valid_node_neighbor_1995)
      checkIt(len16)

      ixx = ixx + 1
      val len17 = pathAdder(len16, valid_node_neighbor_1995)
      checkIt(len17)

      ixx = ixx + 1
      val len18 = pathAdder(len17, valid_node_neighbor_1995)
      checkIt(len18)

      ixx = ixx + 1
      val len19 = pathAdder(len18, valid_node_neighbor_1995)
      checkIt(len19)

      ixx = ixx + 1
      val len20 = pathAdder(len19, valid_node_neighbor_1995)
      checkIt(len20)
    } catch {
      case e: Any => println("caught something? ixx:"+ixx+" with e:"+e)
    } finally {
      sc.stop()
    println("in the finally block... did it write?")
    }


  }
}
