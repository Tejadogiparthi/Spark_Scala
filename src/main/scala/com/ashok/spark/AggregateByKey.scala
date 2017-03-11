package com.ashok.spark

import org.apache.spark._

object AggregateByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CombineByKey").setMaster("local")
    //Create a spark context
    val sc = new SparkContext(conf)
    //sample scores scored by a cricket player is given below. The goal is to find the average runs scored by each player
    val sampledata = Array(("Sachin",100.0),("Sachin",40.0),("Sachin",30.0),("Kohli",110.0),("Kohli",80.0),("Dravid",10.0),("Dravid",70.0))
    val scoresData = sc.parallelize(sampledata)
      //.map(x => (x._1,(x._2, 0)))

    val averageCalculation= scoresData.aggregateByKey((0.0,0))(
     (acc, score) => (acc._1 + score , acc._2+1),
    (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1+acc2._1, acc1._2+acc2._2)
    )
    //While using aggregatebykey, even though some times combine logic and reduce logic are same, input and outputs types will be different

    val averageScores = averageCalculation.map({case (player, scores) => (player, scores._1/scores._2)})
    averageScores.collect().foreach(println)








  }

}
