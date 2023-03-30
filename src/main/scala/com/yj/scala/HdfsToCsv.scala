package com.yj.scala

import java.io.{BufferedReader, ByteArrayInputStream, FileOutputStream, OutputStream,InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import java.net.URI

import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader

object HdfsToCsv {
  def main(args: Array[String]): Unit = {
    var hdfsMsg = "hdfs://master:9000/electricanalysis/output3/electricforecast"
    var localFName = "D:/electricforecast.csv"
    var headStr = "电表号,预测值,预测值顺序"
    readHdfsFile(hdfsMsg, localFName, headStr)

    hdfsMsg = "hdfs://master:9000/electricanalysis/output3/electricclear"
    localFName = "D:/electricclear.csv"
    headStr = "电表号,总电能,尖电能数据,峰电能数据,平电能数据,谷电能数据,数据日期"
    hdfsPath(hdfsMsg, localFName, headStr)

    hdfsMsg = "hdfs://master:9000/electricanalysis/output3/dayelectricdeal"
    localFName = "D:/dayelectricdeal.csv"
    headStr = "数据日期,电表号,日总用电量,尖电能数据,峰电能数据,平电能数据,谷电能数据"
    hdfsPath(hdfsMsg, localFName, headStr)
  }

  def hdfsPath(hdfsPath: String, localFName: String, headStr: String): Unit = {
    val conf = new Configuration // 获取conf配置
    val fileSystem = FileSystem.get(URI.create(hdfsPath), conf)
    val status = fileSystem.listStatus(new Path(hdfsPath))
    val inLocal = new FileOutputStream(localFName) // 写入本地文件

    inLocal.write((headStr + "\n").getBytes) //设置csv文件头

    for (file <- status) {
      val outHDFS = fileSystem.open(new Path(file.getPath.toUri)) // 从HDFS读出文件流
      IOUtils.copyBytes(outHDFS, inLocal, 1024, false)
      outHDFS.close()
    }
    fileSystem.close()
    inLocal.close()
  }


  def readHdfsFile(hdfsFName: String, localFName: String, headStr: String): Unit = {
    val sb = new StringBuffer
    val conf = new Configuration // 获取conf配置
    val fs = FileSystem.get(URI.create(hdfsFName), conf) // 创建文件系统对象
    val statuses = fs.listStatus(new Path(hdfsFName))
    for (file <- statuses) {
      if (file.getPath.getName.startsWith("part")) {
        val outHDFS = fs.open(new Path(hdfsFName + "/" + file.getPath.getName)) // 从HDFS读出文件流
        val reader = new InputStreamReader(outHDFS, "UTF-8")
        val bufReader = new BufferedReader(reader)
        var line:String = bufReader.readLine()
        while (line != null) {
          val valueArr = line.split(",", -1)
          for (i <- 0 until 7) {
            sb.append(valueArr(0)).append(",")
            sb.append(valueArr(i + 1)).append(",")
            sb.append(i + 1).append("\r\n")
          }
          line = bufReader.readLine()
        }
        outHDFS.close()
      }

    }
    sb.deleteCharAt(sb.length - 1)


    val inLocal = new FileOutputStream(localFName) // 写入本地文件
    inLocal.write((headStr + "\n").getBytes) //设置csv文件头

    val a = new ByteArrayInputStream(sb.toString.getBytes)
    IOUtils.copyBytes(a, inLocal, 1024, true)

    inLocal.close()
    fs.close()
  }
}
