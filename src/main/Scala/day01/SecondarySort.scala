package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 的二次排序
 *
 * @author HuGuangtao
 * @date 2021/8/3 10:55
 * @version 1.0
 */

class SecondarySortKey(val word: String, val num: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    // 定义二次排序比较key，在内部实现二次排序比较逻辑 同时要实现序列化接口
    // spark 序列化支持java的序列化框架，默认使用java序列化框架
    // 实现先按照单词降序排序，单词相同再按照数值升序排序
    if (this.word.compareTo(that.word) == 0)
      this.num - that.num
    else
      that.word.compareTo(this.word)

  }
}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SecondarySort").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //输入目录
    val inputRdd: RDD[String] = sc.textFile("H:\\input\\sparkInput\\secondary_sort")

    //输出目录
    //引入隐式转换
    val outPutDir = "H:\\output\\sparkout\\secondary_sort"
    import Util.MyPredef.String2HdfsUtil
    outPutDir.deleteHdfs()

    //过滤和二次排序
    val rdd2: RDD[(SecondarySortKey, String)] = inputRdd.filter(f => {
      if (f.startsWith("(") && f.endsWith(")")) true else false
    }).map(f => {
      val str: String = f.substring(1, f.length - 1)
      val strings: Array[String] = str.split(",")
      (new SecondarySortKey(strings(0), strings(1).toInt), f)

    })
    val arr: Array[(SecondarySortKey, String)] = rdd2.sortBy(_._1).collect()

    for (elem <- arr) {
      println(elem._2)
    }
  }


}
