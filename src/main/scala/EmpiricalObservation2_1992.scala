import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.sql.SparkSession
import spire.compat.{fractional, integral}
import scala.collection.parallel._

import scala.collection.mutable.ListBuffer


object EmpiricalObservation2_1992 {

  def main(args: Array[String]): Unit = {
    // uncomment below line and change the placeholders accordingly
        val sc = SparkSession.builder().master("spark://richmond:30166").getOrCreate().sparkContext

    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
//    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
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

    //======================================    //======================================
    //======================================    //======================================
    // path len adder
//    var path_container_old_count = 0.toLong
//    var path_container_new_count = 1.toLong

    // turn all tilde paths to (end-key,path) for both directions
    // // initialize with all 2 pairs from yearX
    var temp_endkey_holder = all_2_pairs_1992.
      // (a~b, a b ) to (b , a b)
      map(x => (x._1.split("~")(1), x._2)).
      union(
        all_2_pairs_1992.
          // (a~b, a b) to (a, b a)
          map(x => (x._1.split("~")(0), x._2.split("\\s+").reverse.mkString("", " ", "")))
      )
    // container for while loop, initialized to all 2 pair so it won't affect path_container
    var temp_tilde_paths = all_2_pairs_1992
//    while(path_container_old_count != path_container_new_count) {
    while( path_container.map( x => (x._2.split("\\s+").length - 1,2)).reduceByKey(_+_).values.sum()/ 727 < .9)
    {
      temp_endkey_holder = temp_tilde_paths.
        map(x => (x._1.split("~")(1), x._2)).
        union(
          all_2_pairs_1992.
            // (a~b, a b) to (a, b a)
            map(x => (x._1.split("~")(0), x._2.split("\\s+").reverse.mkString("", " ", "")))
        )
    temp_tilde_paths = temp_tilde_paths.subtract(temp_tilde_paths)
      temp_tilde_paths = temp_endkey_holder.
        //(a, b a) to (a, (b a, c))
        join(edges_1992).
        map(x =>
          if (!x._2._1.split("\\s+").contains(x._2._2)) {
            x._2._1 + " " + (x._2._2)
          } else {
            x._2._1
          }).
        map(x => (x.split("\\s+")(0) + "~" + x.split("\\s+")(x.split("\\s+").length - 1), x))
//      path_container_old_count = path_container.count()
      // add and trim path container
      path_container = path_container.union(temp_tilde_paths).
        map(x =>
          //order "b~a" to "a~b" and reverse path to match
          if (x._1.split("~")(0).toInt < x._1.split("~")(1).toInt) {
            x
          } else {
            (x._1.split("~")(1) + "~" + x._1.split("~")(0),
              x._2.split("\\s+").reverse.mkString("", " ", ""))
          }
        ).
        // collect similar start~end paths
        groupByKey().
        // filter down to shortest path by start~end
        map(x => (x._1, x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head))
//      path_container_new_count = path_container.count()
//      println("...while end... :: len:" + path_container.map( x => (x._2.split("\\s+").length,1) ).reduceByKey(_+_) +
//        ",count="+path_container.map( x => (x._2.split("\\s+").length,1) ).reduceByKey(_+_).max._2.toString)
//      println("total: " + path_container.map( x => (x._2.split("\\s+").length-1,2)).reduceByKey(_+_).values.sum())
    }
    //======================================    //======================================
    //======================================    //======================================

//    path_container.map( x => (x._2.split("\\s+").length-1,2)).reduceByKey(_+_).foreach(println)
//    println("final total: " + path_container.map( x => (x._2.split("\\s+").length - 1,2)).reduceByKey(_+_).values.sum())
    path_container.map( x => (x._2.split("\\s+").length-1,2)).reduceByKey(_+_).saveAsTextFile(args(2))
  }
}
