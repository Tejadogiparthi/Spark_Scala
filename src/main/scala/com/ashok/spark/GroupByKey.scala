package com.ashok.spark

import org.apache.spark._

object GroupByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local")
    //Create a spark context
    val sc = new SparkContext(conf)
    //Load our data file
    val input = sc.textFile("./data/cards.txt")
    //Split the lines into words
    val words = input.flatMap(line => line.split(","))
    //Calculate the counts
    val counts = words.map(word => (word,1)).groupByKey().map(card => (card._1, card._2.sum))
    //In groupbykey there is no concept of combiner
    counts.foreach(println)


  }

}
