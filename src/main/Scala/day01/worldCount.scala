package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/1 21:21
 * @version 1.0
 */
object worldCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("worldCount")
    val rdd: RDD[(String, Int)] = new SparkContext(conf)
      .textFile("H:\\input\\mr1")
      .flatMap(_.split("\t"))
      .map((_, 1))
      .reduceByKey(_ + _)

    //主动删除目录：
    /*    val hadoopConf = new Configuration()
        val fs: FileSystem = FileSystem.get(hadoopConf)
        val outPut: String = "H:\\output\\sparkout"
        val outPutPath: Path = new Path(outPut)

        if (fs.exists(outPutPath)) {
          fs.delete(outPutPath, true)
          println(s"delete outputPath: ${outPut}")
        }*/
    //    rdd.saveAsTextFile(outPut)
    val outPut: String = "H:\\output\\sparkout"
    //引入隐式转换
    import Util.MyPredef.String2HdfsUtil
    // 给字符串赋予删除hdfs的功能
    // 删除已存在的输出目录
    outPut.deleteHdfs()
    rdd.saveAsTextFile(outPut)

  }

}
