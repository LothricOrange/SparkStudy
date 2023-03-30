import org.apache.spark.sql.{DataFrame, SparkSession}

object GetDB {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("createdataframefrommysql")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    val map: Map[String, String] = Map[String, String](
      elems = "url" -> "jdbc:mysql://10.14.137.242:3306/casepro",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root",
      "dbtable" -> "t_plane_order"
    )

    val map2: Map[String, String] = Map[String, String](
      elems = "url" -> "jdbc:mysql://10.14.137.242:3306/casepro",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root",
      "dbtable" -> "t_plane_weather"
    )

    val score: DataFrame = session.read.format("jdbc").options(map).load
    val df = score.toDF("序号","航班号","子订单","飞行日期","头等舱","公务舱","经济舱","其他","头等舱总数","公务舱总数","经济舱总数","其他总数")
    df.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding","utf-8").csv("D:\\rea\\planeorder.csv")

    val score2 = session.read.format("jdbc").options(map2).load
    val df2 = score2.toDF("日期","高温","低温","天气状况","风","空气")
    df2.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding","utf-8").csv("D:\\rea\\weather.csv")


    //读
    /*val readFile = session
      .read
      .option("header","true")
      .option("multiLine","true")
      .option("encoding","utf-8")  //utf-8
      .csv("D:\\rea\\planeorder.csv")

    readFile.show()*/
  }
}
