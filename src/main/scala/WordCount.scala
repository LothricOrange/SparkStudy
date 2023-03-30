import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("HelloWorld")
    val sc = new SparkContext(conf)
    val start_time = new Date().getTime
    val linesRDD: RDD[String] = sc.textFile("file:///home/node01/Downloads/ZHETIAN_25.txt")
    val wordsRDD:RDD[String] = linesRDD.flatMap(_.split("[^a-zA-Z]+"))
    val pairsRDD:RDD[(String,Int)]=wordsRDD.map((_, 1))
    val wordCountsRDD:RDD[(String,Int)]=pairsRDD.reduceByKey(_+_)
    val wordCountsSortRDD:RDD[(String,Int)]=wordCountsRDD.sortBy(_._2,false)
    wordCountsSortRDD.repartition(1).foreach(x => println(x))
    val wordNumRDD:RDD[Int]=wordCountsRDD.map(x => x._2)
    println(wordNumRDD.reduce(_+_))
    val end_time =new Date().getTime
    println(end_time-start_time+"毫秒")
  }

}
