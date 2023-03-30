package com.yj.scala

import java.util
import java.util.Collections

import org.apache.spark.{SparkConf, SparkContext}

object ElectricSparkMonthDeal {
  def main(args: Array[String]): Unit = {
    val filePath = "hdfs://master:9000/electricanalysis/output3/dayelectricdeal";
    val outPath = "hdfs://master:9000/electricanalysis/output4/monthelectricdeal";
    val sparkConf = new SparkConf().setMaster("local").setAppName("Electric_Spark_Month_Deal")
    val sc = new SparkContext(sparkConf)

    val dayDealData = sc.textFile(filePath).map(str => (str.split(",", -1)(1), str))
    val result = dayDealData.groupByKey.map(tuple => {
      val userList = new util.ArrayList[String]
      tuple._2.foreach(userList.add)
      Collections.sort(userList, (o1: String, o2: String) => o1.split(",", -1)(0).compareTo(o2.split(",", -1)(0)))
      val sb = new StringBuffer
      sb.append(tuple._1).append(",")
      userList.forEach(str => sb.append(str.split(",", -1)(2)).append(","))
      sb.deleteCharAt(sb.length - 1).toString
    })

    //result.foreach(println)
    result.saveAsTextFile(outPath)

    while(true){

    }
  }
}
