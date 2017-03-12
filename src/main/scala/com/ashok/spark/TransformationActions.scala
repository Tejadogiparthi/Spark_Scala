package com.ashok.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._

object TransformationActions {
  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Transformations_Actions").
      setMaster(appConf.getConfig(args(0)).getString("deploymentMode"))
    val sc = new SparkContext(conf)

    val baseRdd1 = sc.parallelize(Array("hello", "ashok", "hi", "big", "data", "hub", "hub", "hi"), 1)
    val baseRdd2 = sc.parallelize(Array("hey", "gates", "jobs", "ashok"), 1)
    val baseRdd3 = sc.parallelize(Array(1, 2, 3, 4), 2)
    //gets the sample values
    val sampleRdd = baseRdd1.sample(false, 0.5)
    //union combines and merges baseRdd2 with baseRdd1
    val unionRdd = baseRdd1.union(baseRdd2).repartition(1)
    //intersection gets the common values between baseRdd1 and baseRdd2
    val intersectionRdd = baseRdd1.intersection(baseRdd2, 1)
    //distinct gets the unique values in baseRdd1
    val distinctRdd = baseRdd1.distinct.repartition(1)
    //subtract will remove baseRdd2 values from baseRdd1
    val subtractRdd = baseRdd1.subtract(baseRdd2)
    //Cartersion, (a,b,c).cartesian(1,2,3,4) => (a,1),(a,2),(a,3),(a,4),(b,1),(b,2),(b,3),(b,4),(c,1),(c,2),(c,3),(c,4)
    val cartesianRDD = sampleRdd.cartesian(baseRdd2)
    //reduce function adds on the values
    val reducedValue = baseRdd3.reduce((a, b) => a + b)

    //collect, gets all the values
    val collectedRdd = distinctRdd.collect
    collectedRdd.foreach(println)
    //gets the count of the RDD
    val count = distinctRdd.count
    //gets the first value of teh RDD
    val first = distinctRdd.first
    println("count is " + count);
    println("First Element is " + first)
    //takes the first three values of RDD
    val takeValues = distinctRdd.take(3)
    //takes any 2 values as samples
    val takeSample = distinctRdd.takeSample(false, 2)
    //gets the values in ascending order
    val takeOrdered = distinctRdd.takeOrdered(2)
    takeValues.foreach(println)
    //example for fold, very good explanation => http://apachesparkbook.blogspot.com/2015/11/reduce-examples.html
    val foldResult = distinctRdd.fold("<>")((a, b) => a + b)
    println(foldResult)

  }

}
