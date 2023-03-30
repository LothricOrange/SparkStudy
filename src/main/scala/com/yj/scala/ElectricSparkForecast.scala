package com.yj.scala

import org.apache.spark.{SparkConf, SparkContext}

object ElectricSparkForecast {
  def main(args: Array[String]): Unit = {
    val filePath = "hdfs://master:9000/electricanalysis/output3/monthelectricdeal"
    val outPath = "hdfs://master:9000/electricanalysis/output3/electricforecast"
    val sparkConf = new SparkConf().setMaster("local").setAppName("Electric_Spark_Forecast")
    val sc = new SparkContext(sparkConf)

    //加载处理月份数据
    val monthData = sc.textFile(filePath).map(str => {
        val split = str.split(",", -1)
        val sb = new StringBuffer
        for (i <- 1 until split.length) {
          sb.append(split(i)).append(",")
        }
        //表号； 1号总电量，2号总电量…30号总电量。
        (split(0), sb.toString.substring(0, sb.length - 1))
      })

    //预测数据
    val resData = monthData.map(tuple => {
        val split = tuple._2.split(",", -1)
        //移动平均跨越期
        val n = split.length
        //一次平均分子
        var oneSum:Double = 0
        //二次平均分子
        var secSum:Double = 0
        for (d <- 0 until split.length) {
          oneSum += split(d).toDouble
          secSum += oneSum / (d + 1)
        }
        //一次移动平均
        val oneEvg = oneSum / n
        //二次移动平均
        val secEvg = secSum / n
        val at = 2 * oneEvg - secEvg
        val bt = 2.toDouble / (n - 1) * (oneEvg - secEvg)
        val sb = new StringBuffer
        //预测7个周期值
        for (t <- 1 to 7) {
          sb.append(",").append(String.format("%.4f",bt * t + at))
        }
      tuple._1 + sb.toString
    })

    //resData.foreach(println)
    resData.saveAsTextFile(outPath)
  }


}
