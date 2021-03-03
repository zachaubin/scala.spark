import org.apache.spark.sql.SparkSession


object EmpiricalObservation1 {

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

//    val text = sc.textFile(args(0))
//    val counts = text.flatMap(line => line.split(" ")
//    ).map(word => (word,1)).reduceByKey(_+_)
    val year_rdd = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\\s+")(1).split("-")(0), 1));
//    year_rdd.reduceByKey(_ + _).saveAsTextFile(args(1));


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
    val pid_1999 = pid_year_rdd.filter( x => x._2.contains("1999") )
    val pid_2000 = pid_year_rdd.filter( x => x._2.contains("2000") )
    val pid_2001 = pid_year_rdd.filter( x => x._2.contains("2001") )
    val pid_2002 = pid_year_rdd.filter( x => x._2.contains("2002") )
    val pid_2003 = pid_year_rdd.filter( x => x._2.contains("2003") )


    // pid for all years up to yearX
    val pid_1992_back = pid_1992
    val pid_1993_back = pid_1992_back.union(pid_1993)
    val pid_1994_back = pid_1993_back.union(pid_1994)
    val pid_1995_back = pid_1994_back.union(pid_1995)
    val pid_1996_back = pid_1995_back.union(pid_1996)
    val pid_1997_back = pid_1996_back.union(pid_1997)
    val pid_1998_back = pid_1997_back.union(pid_1998)
    val pid_1999_back = pid_1998_back.union(pid_1999)
    val pid_2000_back = pid_1999_back.union(pid_2000)
    val pid_2001_back = pid_2000_back.union(pid_2001)
    val pid_2002_back = pid_2001_back.union(pid_2002)
    val pid_2003_back = pid_2002_back.union(pid_2003)

    // (a, b) , (b, a)
    val edges_1992 = citations_all.
        join(pid_1992_back).
        map( x => (x._2._1, x._1)).
        join(pid_1992_back).
        map( x => (x._1,x._2._1))

    val edges_1993 = citations_all.
        join(pid_1993_back).
        map( x => (x._2._1, x._1)).
        join(pid_1993_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1992)

    val edges_1994 = citations_all.
        join(pid_1994_back).
        map( x => (x._2._1, x._1)).
        join(pid_1994_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1993).subtract(edges_1992)

    val edges_1995 = citations_all.
        join(pid_1995_back).
        map( x => (x._2._1, x._1)).
        join(pid_1995_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)

    val edges_1996 = citations_all.
        join(pid_1996_back).
        map( x => (x._2._1, x._1)).
        join(pid_1996_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)

    val edges_1997 = citations_all.
        join(pid_1997_back).
        map( x => (x._2._1, x._1)).
        join(pid_1997_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)

    val edges_1998 = citations_all.
        join(pid_1998_back).
        map( x => (x._2._1, x._1)).
        join(pid_1998_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1997).subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).
        subtract(edges_1992)

    val edges_1999 = citations_all.
        join(pid_1999_back).
        map( x => (x._2._1, x._1)).
        join(pid_1999_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1998).subtract(edges_1997).subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).
        subtract(edges_1993).subtract(edges_1992)

    val edges_2000 = citations_all.
        join(pid_2000_back).
        map( x => (x._2._1, x._1)).
        join(pid_2000_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_1999).subtract(edges_1998).subtract(edges_1997).subtract(edges_1996).subtract(edges_1995).
        subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)

    val edges_2001 = citations_all.
        join(pid_2001_back).
        map( x => (x._2._1, x._1)).
        join(pid_2001_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_2000).subtract(edges_1999).subtract(edges_1998).subtract(edges_1997).subtract(edges_1996).
        subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)


    val edges_2002 = citations_all.
        join(pid_2002_back).
        map( x => (x._2._1, x._1)).
        join(pid_2002_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_2001).subtract(edges_2000).subtract(edges_1999).subtract(edges_1998).subtract(edges_1997).
        subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).subtract(edges_1992)


    val edges_2003 = citations_all.
        join(pid_2003_back).
        map( x => (x._2._1, x._1)).
        join(pid_2003_back).
        map( x => (x._1,x._2._1)).
        subtract(edges_2002).subtract(edges_2001).subtract(edges_2000).subtract(edges_1999).subtract(edges_1998).
        subtract(edges_1997).subtract(edges_1996).subtract(edges_1995).subtract(edges_1994).subtract(edges_1993).
        subtract(edges_1992)


    val e92_count =  edges_1992.count() / 2
    val e93_count = (edges_1993.count() / 2 + e92_count)
    val e94_count = (edges_1994.count() / 2 + e93_count)
    val e95_count = (edges_1995.count() / 2 + e94_count)
    val e96_count = (edges_1996.count() / 2 + e95_count)
    val e97_count = (edges_1997.count() / 2 + e96_count)
    val e98_count = (edges_1998.count() / 2 + e97_count)
    val e99_count = (edges_1999.count() / 2 + e98_count)
    val e00_count = (edges_2000.count() / 2 + e99_count)
    val e01_count = (edges_2001.count() / 2 + e00_count)
    val e02_count = (edges_2002.count() / 2 + e01_count)
    val e03_count = (edges_2003.count() / 2 + e02_count)


//    println("---------------------------------------")
//    println("t(1992) Nodes: " + pid_1992_back.count())
//    println("t(1992) Edges: " + e92_count)
//    println("---------------------------------------")
//    println("t(1993) Nodes: " + pid_1993_back.count())
//    println("t(1993) Edges: " + e93_count)
//    println("---------------------------------------")
//    println("t(1994) Nodes: " + pid_1994_back.count())
//    println("t(1994) Edges: " + e94_count)
//    println("---------------------------------------")
//    println("t(1995) Nodes: " + pid_1995_back.count())
//    println("t(1995) Edges: " + e95_count)
//    println("---------------------------------------")
//    println("t(1996) Nodes: " + pid_1996_back.count())
//    println("t(1996) Edges: " + e96_count)
//    println("---------------------------------------")
//    println("t(1997) Nodes: " + pid_1997_back.count())
//    println("t(1997) Edges: " + e97_count)
//    println("---------------------------------------")
//    println("t(1998) Nodes: " + pid_1998_back.count())
//    println("t(1998) Edges: " + e98_count)
//    println("---------------------------------------")
//    println("t(1999) Nodes: " + pid_1999_back.count())
//    println("t(1999) Edges: " + e99_count)
//    println("---------------------------------------")
//    println("t(2000) Nodes: " + pid_2000_back.count())
//    println("t(2000) Edges: " + e00_count)
//    println("---------------------------------------")
//    println("t(2001) Nodes: " + pid_2001_back.count())
//    println("t(2001) Edges: " + e01_count)
//    println("---------------------------------------")
//    println("t(2002) Nodes: " + pid_2002_back.count())
//    println("t(2002) Edges: " + e02_count)
//    println("---------------------------------------")
//    println("t(2003) Nodes: " + pid_2003_back.count())
//    println("t(2003) Edges: " + e03_count)
//    println("---------------------------------------")

      var output = ""
      output = output + "---------------------------------------" + "\n"
      output = output + "t(1992) Nodes: " + pid_1992_back.count() + "\n"
      output = output + "t(1992) Edges: " + e92_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1993) Nodes: " + pid_1993_back.count() + "\n"
      output = output + "t(1993) Edges: " + e93_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1994) Nodes: " + pid_1994_back.count() + "\n"
      output = output + "t(1994) Edges: " + e94_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1995) Nodes: " + pid_1995_back.count() + "\n"
      output = output + "t(1995) Edges: " + e95_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1996) Nodes: " + pid_1996_back.count() + "\n"
      output = output + "t(1996) Edges: " + e96_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1997) Nodes: " + pid_1997_back.count() + "\n"
      output = output + "t(1997) Edges: " + e97_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1998) Nodes: " + pid_1998_back.count() + "\n"
      output = output + "t(1998) Edges: " + e98_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(1999) Nodes: " + pid_1999_back.count() + "\n"
      output = output + "t(1999) Edges: " + e99_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(2000) Nodes: " + pid_2000_back.count() + "\n"
      output = output + "t(2000) Edges: " + e00_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(2001) Nodes: " + pid_2001_back.count() + "\n"
      output = output + "t(2001) Edges: " + e01_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(2002) Nodes: " + pid_2002_back.count() + "\n"
      output = output + "t(2002) Edges: " + e02_count + "\n"
      output = output + "--------------------------------------- " + "\n"
      output = output + "t(2003) Nodes: " + pid_2003_back.count() + "\n"
      output = output + "t(2003) Edges: " + e03_count + "\n"
      output = output + "--------------------------------------- " + "\n"

      sc.parallelize(output.split("\n")).saveAsTextFile(args(2))
      sc.stop()
  }
}
//---------------------------------------
//t(1992) Nodes: 855
//t(1992) Edges: 152
//---------------------------------------
//t(1993) Nodes: 2852
//t(1993) Edges: 2862
//---------------------------------------
//t(1994) Nodes: 5746
//t(1994) Edges: 11379
//---------------------------------------
//t(1995) Nodes: 9190
//t(1995) Edges: 29808
//---------------------------------------
//t(1996) Nodes: 13129
//t(1996) Edges: 58927
//---------------------------------------
//t(1997) Nodes: 17381
//t(1997) Edges: 98307
//---------------------------------------
//t(1998) Nodes: 22102
//t(1998) Edges: 142934
//---------------------------------------
//t(1999) Nodes: 27143
//t(1999) Edges: 201300
//---------------------------------------
//t(2000) Nodes: 32362
//t(2000) Edges: 265408
//---------------------------------------
//t(2001) Nodes: 37675
//t(2001) Edges: 335131
//---------------------------------------
//t(2002) Nodes: 38557
//t(2002) Edges: 348478
//---------------------------------------
//t(2003) Nodes: 38557
//t(2003) Edges: 348478
//---------------------------------------


//1993 by laksheen
//1 2919
//2 16386
//3 46857
//4 76790