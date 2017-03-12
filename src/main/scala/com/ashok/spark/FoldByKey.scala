package com.ashok.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._

object FoldByKey {
  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("FoldByKey").
      setMaster(appConf.getConfig(args(0)).getString("deploymentMode"))
    val sc = new SparkContext(conf)
    //This is a good read on how partitions effect the querying=> https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rdd-partitions.html
    //Example for fold transformation
    val rdd1 = sc.parallelize(
      List(
        ("maths", 80),
        ("science", 90)
      ), 2
    )
    /*If you run the below print statement in the spark-shell
    we will get partitions as 8 as the system has 8 cores and by default it will take 8 partitions
    but in standalone mode, we will get 1 partitions.
     So to avoid this, we mention number of partitions we need in parallelize so that it will be standard every where.
     So for this example I have given partitions as 2
     */
    print("The partitions size is " + rdd1.partitions.length)

    val additionalMarks = ("extra", 4)
    //this is the type of rdd used for fold
    val sum = rdd1.fold(additionalMarks) { (acc, marks) => //acc is accumulator of type additional marks, marks is type of rdd1
      val sum = acc._2 + marks._2 //this is the function that operates on each element in rdd with previous accumulator
      ("total", sum)
    }
    /*
    Initial value is 4, so 4 + (4*2) + 80 + 90 = 182 [Here 2 is number of partitions, so for each partition you will have an extra score
     for example, if you have partitions as 3, then the below will be the answer
     4+(4*3)+80+90=186
     */
    print(sum)

    //This is FoldBykey example, difference between fold and foldbykey is nothing but, foldbykey is used for paired rdd's
    val deptEmployees = List(
      ("devlopers", ("ashok", 1500.0)),
      ("devlopers", ("ragu", 1200.0)),
      ("analysts", ("phani", 2200.0)),
      ("analysts", ("sai", 500.0))
    )
    val employeeRdd = sc.parallelize(deptEmployees, 2)

    val maxByDept = employeeRdd.foldByKey(("dummy", 0.0))(
      (acc, element) => if (acc._2 > element._2) acc else element
    )
    println("maximum salaries in each dept is")
    maxByDept.collect().foreach(println)
    //The only disadvantage of reducebyKey and foldbykey is that input and output type should be same and it can be avoided by using aggregateBykey
  }
}
