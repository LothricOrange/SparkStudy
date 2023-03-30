import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
/**
 * 连接hadoop集群，读取文件
 */
object ConHdfs {
  // 配置,文件系统
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://master:9000")
  val fs = FileSystem.get(conf)
  //读取文件
  def readFile(): Unit = {
    val in = fs.open(new Path("/a.txt"))
    var buf = new Array[Byte](1024)
    var len = in.read(buf)
    while (len != -1) {
      print(new String(buf, 0, len))
      len = in.read(buf)
    }
    //关闭资源
    in.close()
  }
  //写文件
  def writeFile(): Unit = {
    val out = fs.create(new Path("/a.txt"), true)
    for (x <- 1 to 5) {
      out.writeBytes("hello," + x)
    }
    out.close()
  }
  //上传文件//下载文件
  def cpFile(): Unit = {
    fs.copyFromLocalFile(new Path("/root/a.txt"), new Path("/a2.txt"))
    fs.copyToLocalFile(new Path("/a.txt"), new Path("/root/aaa.txt"))
    fs.close()
  }
  /**
   * 启动程序
   */
  def main(args: Array[String]): Unit = {
    readFile()
    //writeFile()
    //cpFile()
  }
}
