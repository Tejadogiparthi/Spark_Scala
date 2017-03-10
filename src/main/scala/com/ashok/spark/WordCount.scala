package main.scala.com.ashok.spark

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    //Create a spark context
    val sc = new SparkContext(conf)
    //Load our data file
    val input = sc.textFile("./data/cards.txt")
    //Split the lines into words
    val words = input.flatMap(line => line.split(","))
    //Calculate the word count
    val counts = words.map(word => (word,1)).reduceByKey((value1, value2) => value1 + value2)
    counts.foreach(println)
  }

}
