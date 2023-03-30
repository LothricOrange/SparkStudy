package com.yj.scala

import org.apache.spark.{SparkConf, SparkContext}

object ElectricSparkClear {
  def main(args: Array[String]): Unit = {
    val filePath = "hdfs://master:9000/electricanalysis/basic/userelectric";
    val outPath = "hdfs://master:9000/electricanalysis/output3/electricclear";
    val sparkConf = new SparkConf().setMaster("local").setAppName("Electric_Spark_Clear")
    val sc = new SparkContext(sparkConf)
    val originData = sc.textFile(filePath).filter(str => {
      val split = str.split(",", -1)
      split.length == 8 && "1".equals(split(7))
    })


    val pairData = originData.map(str => (str.split(",", -1)(0), str)).groupByKey

    //如果用户存在31天数据，则保留，不满31天电表数据删除。
    val filterData = pairData.filter(values => values._2.iterator.size == 31)

    //去除无关字段
    val returnResult = filterData.map(values => {
      val sb = new StringBuilder
      values._2.foreach(str => {
        val arr = new Array[String](7)
        System.arraycopy(str.split(",", -1), 0, arr, 0, arr.length)
        if (arr(0) != null) sb.append(arr(0))
        for (i <- 1 until arr.length) {
          sb.append(",")
          if (arr(i) != null) sb.append(arr(i))
        }
        sb.append("\r\n")
      })
      sb.deleteCharAt(sb.length() - 1).toString
    })


    /*println("-----------------")
    returnResult.foreach(println)
    var count = 0;
    println(returnResult.count())
    println("-----------------")*/

    returnResult.saveAsTextFile(outPath);
  }
}
