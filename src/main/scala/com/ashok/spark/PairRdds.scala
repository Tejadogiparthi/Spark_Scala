package com.ashok.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._

object PairRdds {
  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("SparkRdds").
      setMaster(appConf.getConfig("dev").
        getString("deploymentMode"))
    val sc = new SparkContext(conf)

    val baseRdd = sc.parallelize(Array("this,is,a,ball","it,is,a,cat","john,is,in,town,hall"),1)
    val inputRdd = sc.makeRDD(List(("is",2),("it",2),("cat",8),("this",6),("john",5),("a",1)),1)
    val wordsRdd = baseRdd.flatMap(record => record.split(","))
    val wordPairs = wordsRdd.map(word => (word, word.length))
    val filteredWordPairs = wordPairs.filter({case (word,length) => length>=2})
    //filteredWordPairs.collect().foreach(println)

    //You can provide the hdfs path here
    val textFile = sc.textFile("data/stocks.txt")
    val stocksPairRdd = textFile.map { record =>
      val colData = record.split("\t")
      (colData(0), colData(6))
    }

    val stocksGroupdRdd = stocksPairRdd.groupByKey()
    val stocksReducedRdd = stocksPairRdd.reduceByKey((x,y)=> x+y)
    val substractedRdd = wordPairs.subtractByKey(inputRdd)
    val cogroupedRdd = wordPairs.cogroup(inputRdd)

    //cogroupedRdd.collect().foreach(println)

    val updatedCogroup = cogroupedRdd.map{
      x=> {
        val key = x._1
        val item1 = x._2._1.map(x=> x)
        val item2 = x._2._2.map(x=> x)
        (key, (item1, item2))
      }
    }
    //updatedCogroup.collect().foreach(println)

    val joinRdd = filteredWordPairs.join(inputRdd)
    val sortedRdd = wordPairs.sortByKey()
    val leftOuterJoinRdd = inputRdd.leftOuterJoin(filteredWordPairs)  //this will give result like (a,(b,some(c))
    val leftOuterFinalRdd = leftOuterJoinRdd.map{case (a, (b,c)) => (a, (b,c.getOrElse()))} //this will give result like (a,(b,c))
    val rightOtherJoinRdd = wordPairs.rightOuterJoin(inputRdd)
    val rightOuterFinalRdd = rightOtherJoinRdd.map{case (a,(b,c)) => (a, (b.getOrElse(),c))}
    val flatMapsValuesRdd = filteredWordPairs.flatMapValues(length => 1 to 5)
    val mapValuesRdd = wordPairs.mapValues(length => length*2)
    val keysRdd = wordPairs.keys
    val valuesRdd = filteredWordPairs.values
    //valuesRdd.collect().foreach(println)
  }

}
