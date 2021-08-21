package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实现word count的二次排序
 *
 * @author HuGuangtao
 * @date 2021/7/4 20:01
 * @version 1.0
 */

class SecondarySortKey(val name: String, val num: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.name.compareTo(that.name) == 0) {
      this.num - that.num
    } else {
      that.name.compareTo(this.name)
    }
  }
}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SecondarySort")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("H:\\tmp\\spark\\input\\word.txt")
    val rdd2 = rdd1.filter(f => {
      if (f.contains("(") && f.contains(")")) true else false
    }).map(f => {

      // "(hainiu,100)" --> "hainiu,100"
      val str = f.substring(1, f.length - 1)
      val strArr: Array[String] = str.split(",")
      (new SecondarySortKey(strArr(0), strArr(1).toInt), str)
    })
//    val rdd3: RDD[(SecondarySortKey, String)] = rdd2.sortBy(_._1)

    val tuples: Array[(SecondarySortKey, String)] = rdd2.sortByKey().collect()

    for (elem <- tuples) {
      println(elem._2)
    }
  }

}
