package com.ashok.spark

import org.apache.spark._

object CombinedByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CombineByKey").setMaster("local")
    //Create a spark context
    val sc = new SparkContext(conf)
    //sample scores scored by a cricket player is given below. The goal is to find the average runs scored by each player
    val sampledata = Array(("Sachin",100.0),("Sachin",40.0),("Sachin",30.0),("Kohli",110.0),("Kohli",80.0),("Dravid",10.0),("Dravid",70.0))
    val scoresData = sc.parallelize(sampledata)
    //Build the tuples in such a way that the output in the form of (player, (totalscore, totalmatches))
    val averageScoresCalculation = scoresData.combineByKey(
     (x: Double) => (x,1), //create combiner
      (acc:(Double, Int), x) => (acc._1 + x, acc._2+1), //merge value
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1+acc2._1, acc1._2+acc2._2)) //merge combiners

    //While using combinebykey, the combine logic and reducer logic is different, but the input and output format of both are same

    //scores._1 = total score, scores._2 = total matches
    val averageScores = averageScoresCalculation.map( { case (player, scores) => (player, scores._1/scores._2) })
    averageScores.collect().foreach(println)






  }


}
