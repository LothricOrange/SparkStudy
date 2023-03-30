package com.yj.scala

import java.util
import java.util.Collections

import org.apache.spark.{SparkConf, SparkContext}

object ElectricSparkDayDeal {
  def main(args: Array[String]): Unit = {
    val filePath = "hdfs://master:9000/electricanalysis/output3/electricclear";
    val outPath = "hdfs://master:9000/electricanalysis/output3/dayelectricdeal";
    val sparkConf = new SparkConf().setMaster("local").setAppName("Electric_Spark_Day_Deal")
    val sc = new SparkContext(sparkConf)

    val cleanData = sc.textFile(filePath).map(str => {
      val split = str.split(",", -1)
      val sb = new StringBuilder
      val arr = new Array[String](6)
      System.arraycopy(split, 0, arr, 0, 6)
      if (arr(0) != null) sb.append(arr(0))
      for (i <- 1 until arr.length) {
        sb.append(",")
        if (arr(i) != null) sb.append(arr(i))
      }
      val day = split(6).substring(8, 10)
      sb.append(",").append(day)
      (split(0), sb.toString)
    }).groupByKey

    val result = cleanData.map(tuple => {
      val userList = new util.ArrayList[String]
      tuple._2.foreach(userList.add)
      //按照日期进行集合排序
      Collections.sort(userList, (o1: String, o2: String) => o1.split(",", -1)(6).compareTo(o2.split(",", -1)(6)))
      val sb: StringBuffer = new StringBuffer

      for (i <- 0 until userList.size - 1) {
        val todayStr = userList.get(i).toString.split(",", -1)
        val tomorrowStr = userList.get(i + 1).toString.split(",", -1)
        val today = todayStr(6)
        val tomorrow = tomorrowStr(6)
        if (tomorrow.toInt - today.toInt == 1) { //数据日期，表号，日总电量，尖电能数据，峰电能数据，平电能数据，谷电能数据。
          sb.append(today).append(",")
          sb.append(todayStr(0)).append(",")

          sb.append(String.format("%.4f",Math.abs(tomorrowStr(1).toDouble - todayStr(1).toDouble))).append(",")
          sb.append(Math.abs(tomorrowStr(2).toDouble - todayStr(2).toDouble)).append(",")
          sb.append(Math.abs(tomorrowStr(3).toDouble - todayStr(3).toDouble)).append(",")
          sb.append(Math.abs(tomorrowStr(4).toDouble - todayStr(4).toDouble)).append(",")
          sb.append(String.format("%.4f",Math.abs(tomorrowStr(5).toDouble - todayStr(5).toDouble)))
          sb.append("\r\n")
        }
      }
      if (sb.length > 1) sb.deleteCharAt(sb.length - 1)
      sb.toString
    })

    //result.foreach(println)
    result.saveAsTextFile(outPath)
  }



}
