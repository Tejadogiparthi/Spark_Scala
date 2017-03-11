package main.scala.com.ashok.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._

object SparkContext {
  def main(args: Array[String]): Unit = {
    //Instantiate ConfigFactory to pick the properties from application.properties file
    val appConf = ConfigFactory.load()
    //set the configurations for spark context
    val conf = new SparkConf().
      setAppName("SparkContext Sample").
      setMaster(appConf.getConfig(args(1)).getString("deploymentMode"))
    //create spark context
    val sc = new SparkContext(conf)
    //Files path, the file is available in https://raw.githubusercontent.com/asingamaneni/Spark_Scala/master/data/stocks.txt
    val filePath = args(0)
    val data = sc.textFile(filePath, 2)
    val totalLines = data.count()
    println("Total number of lines: %s".format(totalLines))
  }
}
