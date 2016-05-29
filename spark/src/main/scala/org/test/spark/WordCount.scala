package org.test.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args:Array[String]) = {
    var conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    
    val t = sc.textFile("food.txt")
    val t1 = t.flatMap { line => line.split(" ")}.map{ word => (word,1)}
    .reduceByKey(_+_)
    .saveAsTextFile("food.count.txt")
  }
}