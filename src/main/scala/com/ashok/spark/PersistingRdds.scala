package com.ashok.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.ConfigFactory
import org.apache.spark.storage.StorageLevel

object PersistingRdds {
  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Persisting Rdds").
      setMaster(appConf.getConfig("dev").
        getString("deploymentMode"))
    val sc = new SparkContext(conf)

    val inputRdd = sc.parallelize(Array("this,is,a,ball","it,is,a,cat","julie,is,in,the,church"),1)
    val wordsRdd = inputRdd.flatMap(record => record.split(","))
    val wordLengthpairs = wordsRdd.map(word => (word,word.length))
    val wordPairs = wordsRdd.map(word => (word,1))
    val reduceWordCountRdd = wordPairs.reduceByKey((X,Y) => X+Y)
    val filteredWordLengthPairs = wordLengthpairs.filter{case (word, length) => length >=3}
    reduceWordCountRdd.cache()
    val joinedRdd = reduceWordCountRdd.join(filteredWordLengthPairs)
    joinedRdd.persist(StorageLevel.MEMORY_AND_DISK)
    
    //The difference between cache and persist is that cache can be treated as subset of persist.
    //cache = persist(StorageLevel.Memory)
    
    val wordPairsCount = reduceWordCountRdd.count()
    val wordPairsCollection = reduceWordCountRdd.take(10)
    val joinedRddCount = joinedRdd.count()
    val joinedPairs = joinedRdd.collect()
    reduceWordCountRdd.unpersist()
    joinedRdd.unpersist()
    
  }
}
