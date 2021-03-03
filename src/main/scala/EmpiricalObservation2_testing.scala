import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import scala.collection.mutable.ListBuffer


object EmpiricalObservation2_testing {

  def main(args: Array[String]): Unit = {
    // uncomment below line and change the placeholders accordingly
//        val sc = SparkSession.builder().master("spark://richmond:30166").getOrCreate().sparkContext

    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    sc.setLogLevel("ERROR")

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
    //    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    // (a,b)
    val pidfrom_pidto_rdd = sc.textFile(args(0)).
      filter(x => !x.startsWith("#")).
      map(line => (line.split("\\s+")(0),line.split("\\s+")(1)))

    // (b,a)
    val pidto_pidfrom_rdd = pidfrom_pidto_rdd.map( x => (x._2,x._1))

    // (a,b) , (b,a)
    val citations_all = pidfrom_pidto_rdd.union(pidto_pidfrom_rdd)
    //    val citations = pid_ft_tf_rdd.groupByKey()

    // (1,1992)  --toInt.toString handles 0001512 => 1512
    val pid_year_rdd = sc.textFile(args(1)).
      filter(x => !x.startsWith("#")).
      map(line => (line.split("\\s+")(0).toInt.toString,line.split("\\s+")(1).split("-")(0)))

    // (1,1992) EXCLUSIVE by year
    val pid_1992 = pid_year_rdd.filter( x => x._2.contains("1992") )
    val pid_1992_back = pid_1992
    val edges_1992 = citations_all.
      join(pid_1992_back).
      map( x => (x._2._1, x._1)).
      join(pid_1992_back).
      map( x => (x._1,x._2._1))

    ////////////////////////////////////////////////////////////////////////
    // (a~b, a b )
    val path_1992 = edges_1992.
      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )

    // add all new len1 edges from this year to path_container
    var path_container = path_1992

    // create (1, [2,3,4]) for each node, up to this year
    val valid_node_neighbor_1992 = edges_1992.
      map( x => x._1 + " " + x._2).
      map {
        p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
      }.reduceByKey(_:::_)

    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
    var all_2_pairs_1992 = valid_node_neighbor_1992.
      flatMap {case (node, nodeList) =>
        var edges = new ListBuffer[String]()
        if (nodeList.size > 1) {
          for (i <- 0 to (nodeList.size - 2)) {
            for (j <- i + 1 to (nodeList.size - 1)) {
              val start_node = nodeList(i)
              val end_node = nodeList(j)
              var start_end = ""
              if (start_node.toInt > end_node.toInt) {
                start_end = end_node + "~" + start_node
                edges += (start_end + ":" + end_node + " " + node + " " + start_node)
              }else{start_end = start_node + "~" + end_node
                edges += (start_end + ":" + start_node + " " + node + " " + end_node)
              }
            }
          }
        }
        edges.toList
      }.map(x => (x.split(":")(0), x.split(":")(1)))

    // add all new len2 edges
    path_container = path_container.union(all_2_pairs_1992).
      map( x =>
        //order "b~a" to "a~b" and reverse path to match
        if(x._1.split("~")(0).toInt < x._1.split("~")(1).toInt){
          x
        } else {
          (x._1.split("~")(1) + "~" + x._1.split("~")(0),
            x._2.split("\\s+").reverse.mkString("", " ", ""))
        }
      ).
      // collect similar start~end paths
      groupByKey().
      // filter down to shortest path by start~end
      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head))
    ////////////////////////////////////////////////////////////////////////

    path_container.foreach(println)

    //======================================    //======================================
    //======================================    //======================================   x._2.split("\\s+")(x._2.split("\\s+").length)

    val len3 = path_container.union(path_container.map( x => (
      x._1.split("~")(1) +"~"+ x._1.split("~")(0),
      x._2.split("\\s+").reverse.mkString("", " ", "")) )).persist(MEMORY_AND_DISK_SER)

    var ixx = 3

    def pathAdder (input_tilde_paths: RDD[(String,String)], node_neighbors: RDD[(String,List[String])]) : RDD[(String,String)] = {

      println("pathAdder:"+ixx)
      input_tilde_paths.persist(MEMORY_AND_DISK_SER)
      var longer_paths = input_tilde_paths.union(input_tilde_paths.
        map( x => (x._1.split("~")(1),x._2)).
        //(a, ( b a, List( x,y,z )))
        join(node_neighbors).flatMap{
        case (node, (path, nodeList)) =>
          var path_extension_list = new ListBuffer[String]()
          if (nodeList.size > 1) {
            for (i <- nodeList.indices) {
              if(!path.contains(nodeList(i)))
              path_extension_list += path +" "+ nodeList(i)
            }
          }
          path_extension_list.toList
      }.map( x => (x.split("\\s+")(0) +"~"+ x.split("\\s+")(x.split("\\s+").length-1) , x))
      )

      // select shortest and return
       longer_paths.
        // collect similar start~end paths
        groupByKey(300).
        // filter down to shortest path by start~end
        map(x => (x._1, x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head)).
        persist(MEMORY_AND_DISK_SER)

      return longer_paths
    }

    def totalPathCount(input_tilde_paths: RDD[(String, String)]): Int = {
      println("totalPathCount:"+ixx)
      val total_path_count = input_tilde_paths.map(x => (x._2.split("\\s+").length - 1, 2)).reduceByKey(_ + _, 300).values.sum().toInt
      return total_path_count
    }

    def checkNinety(path_count: Int, ninety_value: Int): Boolean = {
      path_count/ninety_value >= .9
    }

    def areWeDoneHere(input_tilde_paths: RDD[(String, String)]): Boolean = {
      checkNinety(totalPathCount(input_tilde_paths), 869493)
    }

    def yesWeAreDone(input_tilde_paths: RDD[(String, String)]): Unit = {
      println("DONE DONE DONE DONE DONE :" + ixx)
      input_tilde_paths.map( x => (x._2.split("\\s+").length-1,1)).reduceByKey(_+_,300).foreach(println)
      sc.stop()
    }

    def noWeNotDone(input_tilde_paths: RDD[(String, String)]): Unit = {
      println("keep going at: " + ixx)
    }

    def checkIt(input_tilde_paths: RDD[(String, String)]): Unit = {
      if(areWeDoneHere(input_tilde_paths)){
        yesWeAreDone(input_tilde_paths)
      } else {
        noWeNotDone(input_tilde_paths)
        input_tilde_paths.unpersist()
      }
    }

    ixx = ixx +1
    val len4 = pathAdder(len3,valid_node_neighbor_1992)
    checkIt(len4)

    ixx = ixx +1
    val len5 = pathAdder(len4,valid_node_neighbor_1992)
    checkIt(len5)

    ///////////////////////////////////////////////////

    ixx = ixx +1
    val len6 = pathAdder(len5,valid_node_neighbor_1992)
    checkIt(len6)

    ixx = ixx +1
    val len7 = pathAdder(len6,valid_node_neighbor_1992)
    checkIt(len7)

    ixx = ixx +1
    val len8 = pathAdder(len7,valid_node_neighbor_1992)
    checkIt(len8)

    ixx = ixx +1
    val len9 = pathAdder(len8,valid_node_neighbor_1992)
    checkIt(len9)

    ixx = ixx +1
    val len10 = pathAdder(len9,valid_node_neighbor_1992)
    checkIt(len10)

    ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////
  }
}
