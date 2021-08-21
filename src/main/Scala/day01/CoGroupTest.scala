package day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/2 20:26
 * @version 1.0
 */
object CoGroupTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoGroupTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("id01", "aa"), ("id02", "bb"), ("id03", "cc")))
    val rdd2 = sc.parallelize(List(("id01", 10), ("id03", 13), ("id04", 14)))
    val tuples: Array[(String, (Iterable[String], Iterable[Int]))] = rdd1.cogroup(rdd2).collect()
    println(tuples.toBuffer)

  }
}
