import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object EmpiricalObservation2_staging {

  def main(args: Array[String]): Unit = {
//        val sc = SparkSession.builder().master("spark://richmond:30166").getOrCreate().sparkContext
    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    sc.setLogLevel("ERROR")

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
    //    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // citations.txt:(a,b),(a,c) | this: (a,b),(b,a),(a,c),(c,a) | returns: (a,[b,c]),(b,[a]),(c,[a])
//    def citationsGrouped(): RDD[(java.lang.String,Iterable[String])] ={
//
//    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    val pid_1993 = pid_year_rdd.filter( x => x._2.contains("1993") )
    val pid_1994 = pid_year_rdd.filter( x => x._2.contains("1994") )
    val pid_1995 = pid_year_rdd.filter( x => x._2.contains("1995") )
    val pid_1996 = pid_year_rdd.filter( x => x._2.contains("1996") )
    val pid_1997 = pid_year_rdd.filter( x => x._2.contains("1997") )
    val pid_1998 = pid_year_rdd.filter( x => x._2.contains("1998") )
    val pid_high = pid_year_rdd.filter( x => x._2.contains("1999") || x._2.contains("2000") || x._2.contains("2001") || x._2.contains("2002"))

    val pid_1992_back = pid_1992
    val pid_1993_back = pid_1992_back.union(pid_1993)
    val pid_1994_back = pid_1993_back.union(pid_1994)
    val pid_1995_back = pid_1994_back.union(pid_1995)
    val pid_1996_back = pid_1995_back.union(pid_1996)
    val pid_1997_back = pid_1996_back.union(pid_1997)
    val pid_1998_back = pid_1997_back.union(pid_1998)

    val citations_92_to_98 = citations_all

    // 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92
    // 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92
    // 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92
    val edges_1992 = citations_all.
      join(pid_1992_back).
      map( x => (x._2._1, x._1)).
      join(pid_1992_back).
      map( x => (x._1,x._2._1))

    val path_1992 = edges_1992.
      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )

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
            for (j <- i + 1 until nodeList.size) {
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

    val path_container_1992 = path_1992.union(all_2_pairs_1992).
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
      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map( x=> (x._1,x._2.head))
//    path_container_1992.saveAsTextFile("/loaders/path_container_1992")

        valid_node_neighbor_1992.saveAsTextFile("./loaders/valid_node_neighbor_1992")
        path_container_1992.saveAsTextFile("./loaders/path_container_1992")

    // 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92
    // 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92
    // 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92 92


    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+


    // 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93
    // 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93
    // 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93
//
//
    val edges_1993 = citations_all.
      join(pid_1993_back).
      map( x => (x._2._1, x._1)).
      join(pid_1993_back).
      map( x => (x._1,x._2._1)).
      subtract(edges_1992)
////    edges_1993.saveAsTextFile("./loaders/edges_1993")
//
    val path_1993 = edges_1993.
      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )
////    path_1993.saveAsTextFile("./loaders/path_1993")
//
    // create (1, [2,3,4]) for each node, up to this year
    val valid_node_neighbor_1993 = edges_1992.union(edges_1993).
      map( x => x._1 + " " + x._2).
      map {
        p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
      }.reduceByKey(_:::_)
//    valid_node_neighbor_1993.saveAsTextFile("./loaders/valid_node_neighbor_1993_short")
//
//    valid_node_neighbor_1993.foreach(println)
//
//    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
    var all_2_pairs_1993 = valid_node_neighbor_1993.
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
////    all_2_pairs_1993.saveAsTextFile("./loaders/all_2_pairs_1993")
//
    val path_container_1993 = path_1992.union(path_1993).union(all_2_pairs_1993).
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
      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map( x => (x._1,x._2.head))
//    path_container_1993.saveAsTextFile("./loaders/path_container_1993_short")

    // 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93
    // 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93
    // 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93 93


    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+


    // 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94
    // 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94
    // 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94

        val edges_1994 = citations_all.
          join(pid_1994_back).
          map( x => (x._2._1, x._1)).
          join(pid_1994_back).
          map( x => (x._1,x._2._1)).
          subtract(edges_1992).subtract(edges_1993)

        val path_1994 = edges_1994.
          map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )

        // create (1, [2,3,4]) for each node, up to this year
        val valid_node_neighbor_1994 = edges_1992.union(edges_1993).union(edges_1994).
          map( x => x._1 + " " + x._2).
          map {
            p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
          }.reduceByKey(_:::_)

        // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
        var all_2_pairs_1994 = valid_node_neighbor_1994.
          flatMap {case (node, nodeList) =>
            var edges = new ListBuffer[String]()
            if (nodeList.size > 1) {
              for (i <- 0 to (nodeList.size - 2)) {
                for (j <- i + 1 until nodeList.size) {
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

        val path_container_1994 = path_1992.union(path_1993).union(path_1994).union(all_2_pairs_1994).
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
          map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map( x=> (x._1,x._2.head))


//    valid_node_neighbor_1994.saveAsTextFile("/loaders/valid_node_neighbor_1994")
//    path_container_1994.saveAsTextFile("/loaders/path_container_1994")

    // 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94
    // 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94
    // 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94 94


    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+


    // 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95
    // 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95
    // 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95

    val edges_1995 = citations_all.
      join(pid_1995_back).
      map( x => (x._2._1, x._1)).
      join(pid_1995_back).
      map( x => (x._1,x._2._1)).
      subtract(edges_1992).subtract(edges_1993).subtract(edges_1994)

    val path_1995 = edges_1995.
      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )

    // create (1, [2,3,4]) for each node, up to this year
    val valid_node_neighbor_1995 = edges_1992.union(edges_1993).union(edges_1994).union(edges_1995).
      map( x => x._1 + " " + x._2).
      map {
        p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
      }.reduceByKey(_:::_)

    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
    var all_2_pairs_1995 = valid_node_neighbor_1995.
      flatMap {case (node, nodeList) =>
        var edges = new ListBuffer[String]()
        if (nodeList.size > 1) {
          for (i <- 0 to (nodeList.size - 2)) {
            for (j <- i + 1 until nodeList.size) {
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

    val path_container_1995 = path_1992.union(path_1993).union(path_1994).union(path_1995).union(all_2_pairs_1995).
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
      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map( x=> (x._1,x._2.head))


//    valid_node_neighbor_1995.saveAsTextFile("/loaders/valid_node_neighbor_1995")
//    path_container_1995.saveAsTextFile("/loaders/path_container_1995")

    // 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95
    // 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95
    // 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95 95


    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+


    // 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96
    // 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96
    // 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96

    val edges_1996 = citations_all.
      join(pid_1996_back).
      map( x => (x._2._1, x._1)).
      join(pid_1996_back).
      map( x => (x._1,x._2._1)).
      subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)

    val path_1996 = edges_1996.
      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )

    // create (1, [2,3,4]) for each node, up to this year
    val valid_node_neighbor_1996 = edges_1992.union(edges_1993).union(edges_1994).union(edges_1995).union(edges_1996).
      map( x => x._1 + " " + x._2).
      map {
        p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
      }.reduceByKey(_:::_)

    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
    var all_2_pairs_1996 = valid_node_neighbor_1996.
      flatMap {case (node, nodeList) =>
        var edges = new ListBuffer[String]()
        if (nodeList.size > 1) {
          for (i <- 0 to (nodeList.size - 2)) {
            for (j <- i + 1 until nodeList.size) {
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

    val path_container_1996 = path_1992.union(path_1993).union(path_1994).union(path_1995).union(path_1996).union(all_2_pairs_1996).
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
      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map( x=> (x._1,x._2.head))


//    valid_node_neighbor_1996.saveAsTextFile("/loaders/valid_node_neighbor_1996")
//    path_container_1996.saveAsTextFile("/loaders/path_container_1996")


    // 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96
    // 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96
    // 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96 96


    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+
    // +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+ +=+


    // 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97
    // 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97
    // 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97

        val edges_1997 = citations_all.
          join(pid_1997_back).
          map( x => (x._2._1, x._1)).
          join(pid_1997_back).
          map( x => (x._1,x._2._1)).
          subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)

    val path_1997 = edges_1997.
      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )

    // create (1, [2,3,4]) for each node, up to this year
    val valid_node_neighbor_1997 = edges_1992.union(edges_1993).union(edges_1994).union(edges_1995).union(edges_1996).
      union(edges_1997).
      map( x => x._1 + " " + x._2).
      map {
        p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
      }.reduceByKey(_:::_)

    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
    var all_2_pairs_1997 = valid_node_neighbor_1997.
      flatMap {case (node, nodeList) =>
        var edges = new ListBuffer[String]()
        if (nodeList.size > 1) {
          for (i <- 0 to (nodeList.size - 2)) {
            for (j <- i + 1 until nodeList.size) {
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

    val path_container_1997 = path_1992.union(path_1993).union(path_1994).union(path_1995).union(path_1996).
      union(path_1997).union(all_2_pairs_1997).
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
      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length))).map( x=> (x._1,x._2.head))


//    valid_node_neighbor_1997.saveAsTextFile("/loaders/valid_node_neighbor_1997")
//    path_container_1997.saveAsTextFile("/loaders/path_container_1997")

    // 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97
    // 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97
    // 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97 97


    //    val edges_1995 = citations_all.
//      join(pid_1995_back).
//      map( x => (x._2._1, x._1)).
//      join(pid_1995_back).
//      map( x => (x._1,x._2._1)).
//      subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)
//
//    val edges_1996 = citations_all.
//      join(pid_1996_back).
//      map( x => (x._2._1, x._1)).
//      join(pid_1996_back).
//      map( x => (x._1,x._2._1)).
//      subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)
//
//    val edges_1997 = citations_all.
//      join(pid_1997_back).
//      map( x => (x._2._1, x._1)).
//      join(pid_1997_back).
//      map( x => (x._1,x._2._1)).
//      subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)
//
//    val edges_1998 = citations_all.
//      join(pid_1998_back).
//      map( x => (x._2._1, x._1)).
//      join(pid_1998_back).
//      map( x => (x._1,x._2._1)).
//      subtract(edges_1997).subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)



//    ////////////////////////////////////////////////////////////////////////
//    // (a~b, a b )
//    val path_1992 = edges_1992.
//      map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )
//
//    // add all new len1 edges from this year to path_container
//    var path_container = path_1992
//
//    // create (1, [2,3,4]) for each node, up to this year
//    val valid_node_neighbor_1992 = edges_1992.
//      map( x => x._1 + " " + x._2).
//      map {
//      p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
//    }.reduceByKey(_:::_)

//    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
//    var all_2_pairs_1992 = valid_node_neighbor_1992.
//      flatMap {case (node, nodeList) =>
//        var edges = new ListBuffer[String]()
//        if (nodeList.size > 1) {
//          for (i <- 0 to (nodeList.size - 2)) {
//            for (j <- i + 1 to (nodeList.size - 1)) {
//              val start_node = nodeList(i)
//              val end_node = nodeList(j)
//              var start_end = ""
//              if (start_node.toInt > end_node.toInt) {
//                start_end = end_node + "~" + start_node
//                edges += (start_end + ":" + end_node + " " + node + " " + start_node)
//              }else{start_end = start_node + "~" + end_node
//                edges += (start_end + ":" + start_node + " " + node + " " + end_node)
//              }
//            }
//          }
//        }
//        edges.toList
//      }.map(x => (x.split(":")(0), x.split(":")(1)))
//
//    // add all new len2 edges
//    path_container = path_container.union(all_2_pairs_1992).
//      map( x =>
//        //order "b~a" to "a~b" and reverse path to match
//        if(x._1.split("~")(0).toInt < x._1.split("~")(1).toInt){
//          x
//        } else {
//          (x._1.split("~")(1) + "~" + x._1.split("~")(0),
//            x._2.split("\\s+").reverse.mkString("", " ", ""))
//        }
//      ).
//      // collect similar start~end paths
//      groupByKey().
//      // filter down to shortest path by start~end
//      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head))

//    //======================================    //======================================
//    //======================================    //======================================
//    // path len adder
//
//    val year_to_analyze = args(3)
//
//    var path_container_old_count = 0.toLong
//    var path_container_new_count = 1.toLong
//
//    // turn all tilde paths to (end-key,path) for both directions
//    // // initialize with all 2 pairs from yearX
//    var temp_endkey_holder = all_2_pairs_1992.
//      // (a~b, a b ) to (b , a b)
//      map(x => (x._1.split("~")(1), x._2)).
//      union(
//        all_2_pairs_1992.
//          // (a~b, a b) to (a, b a)
//          map(x => (x._1.split("~")(0), x._2.split("\\s+").reverse.mkString("", " ", "")))
//      )
////    println("STARTING temp_path_holder")
////    temp_endkey_holder.foreach(println)
////    println("STOPPING temp_path_holder")
//
//    // container for while loop, initialized to all 2 pair so it won't affect path_container
//    var temp_tilde_paths = all_2_pairs_1992
//
//    while(path_container_old_count != path_container_new_count) {
//
//      temp_endkey_holder = temp_tilde_paths.
//        map(x => (x._1.split("~")(1), x._2)).
//        union(
//          all_2_pairs_1992.
//            // (a~b, a b) to (a, b a)
//            map(x => (x._1.split("~")(0), x._2.split("\\s+").reverse.mkString("", " ", "")))
//        )
//
//      temp_tilde_paths = temp_tilde_paths.subtract(temp_tilde_paths)
//
//      temp_tilde_paths = temp_endkey_holder.
//        //(a, b a) to (a, (b a, c))
//        join(edges_1992).
//        map(x =>
//          if (!x._2._1.split("\\s+").contains(x._2._2)) {
//            x._2._1 + " " + (x._2._2)
//          } else {
//            x._2._1
//          }).
//        map(x => (x.split("\\s+")(0) + "~" + x.split("\\s+")(x.split("\\s+").length - 1), x))
//
////      println("oaskdgnoasddgknosagknasodkgn")
////      temp_tilde_paths.foreach(println)
////      println("oaskdgnoasddgknosagknasodkgn")
//
//      path_container_old_count = path_container.count()
//      // add and trim path container
//      path_container = path_container.union(temp_tilde_paths).
//        map(x =>
//          //order "b~a" to "a~b" and reverse path to match
//          if (x._1.split("~")(0).toInt < x._1.split("~")(1).toInt) {
//            x
//          } else {
//            (x._1.split("~")(1) + "~" + x._1.split("~")(0),
//              x._2.split("\\s+").reverse.mkString("", " ", ""))
//          }
//        ).
//        // collect similar start~end paths
//        groupByKey().
//        // filter down to shortest path by start~end
//        map(x => (x._1, x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head))
//      path_container_new_count = path_container.count()
//
////      println("BEGIN path_container INSIDE WHILE LOOP")
////      path_container.foreach(println)
////      println("ENDED path_container INSIDE WHILE LOOP")
//
//
//    }
//    path_container.sortBy(_._2.split("\\s+").length).foreach(println)
//    //======================================    //======================================
//    //======================================    //======================================
//
//
//
//    ////////////////////////////////////////////////////////////////////////
//
//    // (b~c, b c )
//    val path_1993 = edges_1993//////////////////////////.union(edges_1992)
//     . map( x =>(x._1 + "~" + x._2, x._1 + " " + x._2) )
//
//    // add new 1993 len1 edges
//    path_container = path_container.union(path_1993)
//
//    // create (1, [2,3,4]) for each node, up to this year
//    val valid_node_neighbor_1993 = edges_1993.union(edges_1992).
//      map( x => x._1 + " " + x._2).
//      map {
//        p => (p.split("\\s+")(0), List(p.split("\\s+")(1)))
//      }.reduceByKey(_:::_)
//
////    edges_1992.union(edges_1993).foreach(println)
//
////    valid_node_neighbor_1993.foreach(println)
//
//    // (1, [2,3,4]) => (2 1 3, 3 1 2, 4 1 2, 3 1 4) or so
//    var all_2_pairs_1993 = valid_node_neighbor_1993.
//      flatMap {case (node, nodeList) =>
//        var edges = new ListBuffer[String]()
//        if (nodeList.size > 1) {
//          for (i <- 0 to (nodeList.size - 2)) {
//            for (j <- i + 1 to (nodeList.size - 1)) {
//              val start_node = nodeList(i)
//              val end_node = nodeList(j)
//              var start_end = ""
//              if (start_node.toInt > end_node.toInt) {
//                start_end = end_node + "~" + start_node
//                edges += (start_end + ":" + end_node + " " + node + " " + start_node)
//              }else{start_end = start_node + "~" + end_node
//                edges += (start_end + ":" + start_node + " " + node + " " + end_node)
//              }
//            }
//          }
//        }
//        edges.toList
//      }.map(x => (x.split(":")(0), x.split(":")(1)))
//
//    // add all new
//    path_container = path_container.union(all_2_pairs_1993).
//      map( x =>
//        //order "b~a" to "a~b" and reverse path to match
//      if(x._1.split("~")(0).toInt < x._1.split("~")(1).toInt){
//      x
//      } else {
//        (x._1.split("~")(1) + "~" + x._1.split("~")(0),
//          x._2.split("\\s+").reverse.mkString("", " ", ""))
//      }
//
//      ).
//    // collect similar start~end paths
//      groupByKey().
//      // filter down to shortest path by start~end
//      map( x => (x._1,x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head))
//
//    //======================================    //======================================
//    // path len adder
//    path_container_old_count = 0.toLong
//    path_container_new_count = 1.toLong
//
//    while(path_container_old_count != (path_container_new_count)) {
//      var temp_path_holder = all_2_pairs_1992.
//        // (a~b, a b ) to (b , a b)
//        map(x => (x._1.split("~")(1), x._2)).
//        union(
//          all_2_pairs_1992.
//            // (a~b, a b) to (a, b a)
//            map(x => (x._1.split("~")(0), x._2.split("\\s+").reverse.mkString("", " ", "")))
//          //(a, b a) to (a, (b a, c))
//        ).join(edges_1992).
//        map(x =>
//          if (!x._2._1.split("\\s+").contains(x._2._2)) {
//            x._2._1 + " " + (x._2._2)
//          } else {
//            x._2._1
//          }).
//        map(x => (x.split("\\s+")(0) + "~" + x.split("\\s+")(x.split("\\s+").length - 1), x))
//
//      path_container_old_count = path_container.count()
//      // add and trim path container
//      path_container = path_container.union(temp_path_holder).
//        map(x =>
//          //order "b~a" to "a~b" and reverse path to match
//          if (x._1.split("~")(0).toInt < x._1.split("~")(1).toInt) {
//            x
//          } else {
//            (x._1.split("~")(1) + "~" + x._1.split("~")(0),
//              x._2.split("\\s+").reverse.mkString("", " ", ""))
//          }
//        ).
//        // collect similar start~end paths
//        groupByKey().
//        // filter down to shortest path by start~end
//        map(x => (x._1, x._2.toList.sortWith(_.split("\\s+").length < _.split("\\s+").length).head))
//      path_container_new_count = path_container.count()
//
//    }
//    path_container.foreach(println)
    //======================================    //======================================
  }

}

//(111~112,111 112)
//(111~113,111 113)
//(112~113,112 111 113)
//(222~333,222 333)
//(111~114,111 114)
//(111~222,111 222)
//(112~114,112 114)
//(112~222,112 222)
//(113~222,113 111 222)
//(113~114,113 111 114)
//(111~333,111 222 333)
//(114~222,114 112 222)
//(112~333,112 222 333)